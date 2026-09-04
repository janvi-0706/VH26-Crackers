import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Builds straight into dashboard/dist, which src/triage/app.py mounts as
// static (with SPA fallback) when it exists. Relative base so the built
// assets work when served from FastAPI's "/" mount, not just from a
// dev-server root.
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
  },
});
