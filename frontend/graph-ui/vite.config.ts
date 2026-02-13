// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  define: {
    "process.env": {},
    process: { env: {} },
  },
  build: {
    lib: {
      entry: resolve(__dirname, "src/entry.tsx"),
      name: "CeleryDag",
      formats: ["umd"],
      fileName: () => "graph.js",
    },
    outDir: resolve(__dirname, "..", "..", "celery_root", "web", "static", "graph"),
    emptyOutDir: true,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        assetFileNames: "graph.css",
      },
    },
  },
});
