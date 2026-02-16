// SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
// SPDX-FileCopyrightText: 2026 Maximilian Dolling
// SPDX-FileContributor: AUTHORS.md
//
// SPDX-License-Identifier: BSD-3-Clause

import { promises as fs } from "node:fs";
import { resolve } from "node:path";

import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const GRAPH_OUT_DIR = resolve(
  __dirname,
  "..",
  "..",
  "celery_root",
  "components",
  "web",
  "static",
  "graph",
);

const SPDX_BANNER = `/* SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen */
/* SPDX-FileCopyrightText: 2026 Maximilian Dolling */
/* SPDX-FileContributor: AUTHORS.md */
/* SPDX-License-Identifier: BSD-3-Clause */
`;

function spdxBanner() {
  return {
    name: "spdx-banner",
    generateBundle(
      _: unknown,
      bundle: Record<string, { type: string; code?: string; source?: string | Uint8Array }>,
    ) {
      Object.values(bundle).forEach((entry) => {
        if (entry.type === "chunk" && entry.code) {
          if (!entry.code.startsWith("/* SPDX-FileCopyrightText:")) {
            entry.code = `${SPDX_BANNER}\n${entry.code}`;
          }
          return;
        }
        if (entry.type === "asset" && entry.source) {
          const sourceText =
            typeof entry.source === "string"
              ? entry.source
              : new TextDecoder().decode(entry.source);
          if (!sourceText.startsWith("/* SPDX-FileCopyrightText:")) {
            entry.source = `${SPDX_BANNER}\n${sourceText}`;
          }
        }
      });
    },
    async writeBundle() {
      const cssPath = resolve(GRAPH_OUT_DIR, "graph.css");
      try {
        const css = await fs.readFile(cssPath, "utf8");
        if (!css.startsWith("/* SPDX-FileCopyrightText:")) {
          await fs.writeFile(cssPath, `${SPDX_BANNER}\n${css}`);
        }
      } catch {
        // Ignore missing assets during build.
      }
    },
  };
}

export default defineConfig({
  plugins: [react(), spdxBanner()],
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
    outDir: GRAPH_OUT_DIR,
    emptyOutDir: true,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        assetFileNames: "graph.css",
      },
    },
  },
});
