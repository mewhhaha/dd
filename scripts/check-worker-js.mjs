import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ESLint } from "eslint";
import globals from "globals";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const sourceDir = join(root, "crates/runtime/js/execute_worker");
const units = (await readFile(join(sourceDir, "units.txt"), "utf8"))
  .split(/\r?\n/).map((unit) => unit.trim()).filter(Boolean);
const sources = await Promise.all(units.map((unit) => readFile(join(sourceDir, unit), "utf8")));
const eslint = new ESLint({
  overrideConfigFile: true,
  overrideConfig: {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "script",
      globals: { ...globals.browser, Deno: "readonly", RuntimeHttpClient: "readonly" },
    },
    rules: {
      "no-undef": "error",
      "no-unused-vars": ["error", { args: "none", caughtErrors: "none" }],
    },
  },
});
const [result] = await eslint.lintText(sources.join("\n"), { filePath: "execute_worker.js" });
for (const message of result.messages) {
  let line = message.line;
  let index = 0;
  while (index < sources.length - 1 && line > sources[index].split("\n").length) {
    line -= sources[index].split("\n").length;
    index += 1;
  }
  console.error(`${units[index]}:${line}:${message.column}: ${message.message} (${message.ruleId ?? "syntax"})`);
}
if (result.errorCount > 0) process.exitCode = 1;
else console.log(`worker JavaScript: ${units.length} source units checked for undefined and unused bindings`);
