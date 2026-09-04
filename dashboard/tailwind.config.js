/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // A small, named palette rather than raw Tailwind grays sprinkled
        // through every component — one place to retune the whole theme.
        surface: {
          DEFAULT: "#0b0e14", // page background
          panel: "#12161f", // panel background
          raised: "#1a2030", // hovered/active panel
          border: "#232a3b",
        },
        ink: {
          DEFAULT: "#e6e9f2", // primary text
          muted: "#8b93a7", // labels, secondary text
          faint: "#5b6478",
        },
        good: "#34d399", // emerald-400 — SLA met, calm
        bad: "#f87171", // red-400 — SLA missed, shed
        warn: "#fbbf24", // amber-400 — degrading, reconnecting
        tier: {
          p0: "#60a5fa", // blue-400
          p1: "#c084fc", // purple-400
          p2: "#fb923c", // orange-400
        },
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "Consolas", "monospace"],
      },
    },
  },
  plugins: [],
};
