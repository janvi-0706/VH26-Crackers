"""dedup.py: the Bloom-filter-as-candidate, exact-set-as-confirmation
mechanism, in isolation. THE claim this file exists to prove, repeatedly
and from different angles: a Bloom filter's own answer is NEVER, by
itself, grounds to suppress an event — only an exact-set-confirmed hit is,
for every tier, P0 included.
"""

from __future__ import annotations

from triage.dedup import BloomFilter, Deduplicator


# --------------------------------------------------------------------------
# BloomFilter: no false negatives, sane sizing
# --------------------------------------------------------------------------


def test_bloom_filter_never_produces_a_false_negative():
    """The one guarantee a Bloom filter must never break: everything added
    is always reported as present. 2000 distinct keys, all checked."""
    bloom = BloomFilter(expected_items=2000, false_positive_rate=0.01)
    keys = [f"dedup:{i}" for i in range(2000)]
    for k in keys:
        bloom.add(k)
    assert all(bloom.might_contain(k) for k in keys)


def test_bloom_filter_rejects_most_keys_that_were_never_added():
    """Not a proof of the exact false-positive rate (that would need a
    much larger statistical sample to be a meaningful assertion) — just
    confirming the filter is doing real, useful work, not degenerately
    reporting "maybe" for everything."""
    bloom = BloomFilter(expected_items=2000, false_positive_rate=0.01)
    for i in range(2000):
        bloom.add(f"dedup:{i}")
    never_added = [f"never-added:{i}" for i in range(5000)]
    false_positives = sum(1 for k in never_added if bloom.might_contain(k))
    # At a 1% target rate over 5000 probes, ~50 false positives is
    # expected; well under half of the probe set is a generous, non-flaky
    # bound that still catches "this filter does nothing" outright.
    assert false_positives < len(never_added) * 0.5


def test_bloom_filter_rejects_bad_construction_arguments():
    import pytest

    with pytest.raises(ValueError):
        BloomFilter(expected_items=0, false_positive_rate=0.01)
    with pytest.raises(ValueError):
        BloomFilter(expected_items=100, false_positive_rate=0.0)
    with pytest.raises(ValueError):
        BloomFilter(expected_items=100, false_positive_rate=1.0)


# --------------------------------------------------------------------------
# Deduplicator.check(): the actual identity-model decision
# --------------------------------------------------------------------------


def test_a_genuinely_new_dedup_key_is_admitted():
    dedup = Deduplicator()
    assert dedup.check("payment:customer:1:1") is False


def test_the_same_dedup_key_seen_again_is_confirmed_as_a_duplicate():
    dedup = Deduplicator()
    assert dedup.check("payment:customer:1:1") is False  # first delivery: admitted
    assert dedup.check("payment:customer:1:1") is True   # second: confirmed duplicate
    assert dedup.check("payment:customer:1:1") is True   # third: still a duplicate


def test_different_dedup_keys_never_collide_in_practice():
    dedup = Deduplicator()
    for i in range(500):
        assert dedup.check(f"order:customer:{i}:1") is False
    # Every one of those 500 keys, checked again, is now a confirmed repeat.
    for i in range(500):
        assert dedup.check(f"order:customer:{i}:1") is True


def test_an_unconfirmed_bloom_hit_is_never_suppressed_for_any_tier_p0_included():
    """The prompt's own explicit callout, proved directly: force a real
    Bloom-filter collision (two different dedup_keys landing on the exact
    same bit positions) between a key that WAS added and one that never
    was, then confirm the never-added one — even though the Bloom filter
    itself reports "maybe seen" for it — is still admitted, not suppressed.
    This is checked against a P0-shaped dedup_key specifically, because
    that is the one tier CLAUDE.md's hard rule 3 makes it unacceptable to
    get wrong."""
    dedup = Deduplicator(expected_items=10, false_positive_rate=0.01)
    seen_key = "payment:customer:1:1"
    dedup.check(seen_key)  # admits it, sets its bloom bits

    # Search for a distinct key whose bloom slots are a SUBSET of (or equal
    # to) seen_key's own — guaranteed to exist quickly for a small filter,
    # and gives a real, reproducible collision rather than a hoped-for one.
    bloom = dedup._bloom  # white-box: this test exists to prove the exact
                           # mechanism, not just its external behaviour.
    seen_slots = set(bloom._slots(seen_key))
    colliding_key = None
    for i in range(100_000):
        candidate = f"payment:customer:2:{i}"
        if candidate == seen_key:
            continue
        if set(bloom._slots(candidate)) <= seen_slots:
            colliding_key = candidate
            break
    assert colliding_key is not None, "no colliding key found — widen the search"

    assert bloom.might_contain(colliding_key) is True  # the Bloom filter says "maybe"
    # ... but it was never actually added, so the exact set cannot confirm
    # it — THE rule: admitted, not suppressed.
    assert dedup.check(colliding_key) is False


def test_a_confirmed_p0_duplicate_is_still_suppressed_this_is_not_hard_rule_3():
    """The other half of the same claim, stated the other direction: a
    GENUINE, exact-confirmed repeat of a P0 dedup_key (e.g. a real
    duplicate payment webhook retry) IS suppressed — dedup is an identity
    check ("was this business fact already admitted"), a different
    question from CLAUDE.md hard rule 3's own ("once admitted, is a P0
    event ever batched/deferred/sampled/shed"). Confirming this distinction
    holds is as important as the false-positive-safety test above; getting
    it backwards (never suppressing P0 duplicates at all) would let a
    replayed payment webhook double-book a customer forever."""
    dedup = Deduplicator()
    p0_key = "payment:customer:7:3"
    assert dedup.check(p0_key) is False  # first delivery
    assert dedup.check(p0_key) is True   # genuine retry: confirmed duplicate


def test_the_exact_set_is_bounded_and_evicts_the_oldest_entry():
    dedup = Deduplicator(expected_items=1000, false_positive_rate=0.01, exact_capacity=5)
    for i in range(5):
        assert dedup.check(f"k{i}") is False
    assert dedup.exact_set_size() == 5
    # One more distinct key evicts the oldest (k0).
    assert dedup.check("k5") is False
    assert dedup.exact_set_size() == 5


def test_an_aged_out_key_is_re_admitted_not_incorrectly_suppressed_forever():
    """The bounded-window consequence, stated as its own claim: once a
    genuinely-seen key ages out of the exact set, a real repeat of it is
    treated exactly like a Bloom false positive — admitted, not
    suppressed — by the same rule, not a special case. `exact_capacity=1`
    makes eviction happen on the very next distinct key, deterministically."""
    dedup = Deduplicator(expected_items=1000, false_positive_rate=0.01, exact_capacity=1)
    assert dedup.check("k0") is False  # admitted, now the only exact-set entry
    assert dedup.check("k1") is False  # a different key evicts k0
    # k0 is still "maybe" in the Bloom filter (Bloom never forgets) but is
    # no longer in the exact set — re-admitted, not suppressed.
    assert dedup.check("k0") is False
