import { useEffect, useRef, useState } from "react";
import type { MetricsFrame } from "../types/metrics";

export type ConnectionStatus =
  | "connecting"
  | "open"
  | "reconnecting"
  | "closed";

// Hardcoded per the Stage B prompt: the backend always listens on :8000
// regardless of which port actually served this page (FastAPI static mount
// on 8000 itself, or the Vite dev server on 5173 during development).
const WS_URL = "ws://localhost:8000/ws";

// How many frames of history the charts get to work with. At 4 Hz this is a
// 60-second rolling window — long enough to show a spike ramping and
// recovering, short enough that a chart never has to decimate.
export const HISTORY_LENGTH = 240;

const RECONNECT_DELAYS_MS = [500, 1000, 2000, 4000, 8000] as const;
const MAX_RECONNECT_DELAY_MS = RECONNECT_DELAYS_MS[RECONNECT_DELAYS_MS.length - 1];

interface MetricsSocketState {
  status: ConnectionStatus;
  latest: MetricsFrame | null;
  history: MetricsFrame[];
  /** Wipe the local rolling window. The backend's /control/reset clears
   * its own counters instantly, but without this the charts would keep
   * showing up to 60s of now-stale pre-reset samples — confusing on stage,
   * where RESET is supposed to mean "clean slate now". Call this from the
   * same place that calls the reset API, not automatically: only a
   * deliberate reset should throw away history, never a socket hiccup. */
  clearHistory: () => void;
}

/**
 * Owns exactly one WebSocket to the backend for the component tree's
 * lifetime, and reconnects with capped exponential backoff whenever the
 * connection drops. A panel just reads `latest` / `history`; the socket
 * itself is invisible unless `status` says otherwise.
 */
export function useMetricsSocket(): MetricsSocketState {
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [latest, setLatest] = useState<MetricsFrame | null>(null);
  const historyRef = useRef<MetricsFrame[]>([]);
  const [historyTick, setHistoryTick] = useState(0);

  useEffect(() => {
    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let attempt = 0;
    let cancelled = false;

    const connect = () => {
      if (cancelled) return;
      setStatus(attempt === 0 ? "connecting" : "reconnecting");

      socket = new WebSocket(WS_URL);

      socket.onopen = () => {
        attempt = 0;
        setStatus("open");
      };

      socket.onmessage = (event: MessageEvent<string>) => {
        let frame: MetricsFrame;
        try {
          frame = JSON.parse(event.data) as MetricsFrame;
        } catch {
          return; // a malformed frame must not take the whole dashboard down
        }
        setLatest(frame);
        const next = [...historyRef.current, frame];
        if (next.length > HISTORY_LENGTH) next.shift();
        historyRef.current = next;
        setHistoryTick((t) => t + 1);
      };

      const scheduleReconnect = () => {
        if (cancelled) return;
        setStatus("reconnecting");
        const delay =
          RECONNECT_DELAYS_MS[Math.min(attempt, RECONNECT_DELAYS_MS.length - 1)];
        attempt += 1;
        reconnectTimer = setTimeout(connect, Math.min(delay, MAX_RECONNECT_DELAY_MS));
      };

      socket.onclose = scheduleReconnect;
      socket.onerror = () => socket?.close();
    };

    connect();

    return () => {
      cancelled = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (socket) {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onclose = null;
        socket.onerror = null;
        socket.close();
      }
    };
  }, []);

  // historyTick exists only to force a re-render when the ref-backed array
  // grows; consumers read the array itself, never the tick.
  void historyTick;

  const clearHistory = () => {
    historyRef.current = [];
    setHistoryTick((t) => t + 1);
  };

  return { status, latest, history: historyRef.current, clearHistory };
}
