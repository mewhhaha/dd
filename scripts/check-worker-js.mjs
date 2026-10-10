import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { ESLint } from "eslint";
import globals from "globals";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const jsDir = join(root, "crates/runtime/js");
const sourceDir = join(jsDir, "execute_worker");
const units = (await readFile(join(sourceDir, "units.txt"), "utf8"))
  .split(/\r?\n/).map((unit) => unit.trim()).filter(Boolean);
const sources = await Promise.all(units.map((unit) => readFile(join(sourceDir, unit), "utf8")));

// Worker code shares the runtime's context and can replace any global, so
// the runtime's own scripts name none: built-ins come from primordials and
// web classes from the bootstrap object. Every ES and browser global is off;
// only the values no script can change remain.
const unchangeable = new Set(["undefined", "NaN", "Infinity"]);
const noGlobals = Object.fromEntries(
  [...Object.keys(globals.builtin), ...Object.keys(globals.browser)]
    .filter((name) => !unchangeable.has(name))
    .map((name) => [name, "off"]),
);
const eslint = new ESLint({
  overrideConfigFile: true,
  overrideConfig: {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "script",
      globals: noGlobals,
    },
    rules: {
      "no-undef": "error",
      "no-unused-vars": ["error", { args: "none", caughtErrors: "none" }],
    },
  },
});

let errors = 0;
// Each runtime script runs as the body of a function whose one parameter is
// the runtime's bootstrap object.
async function lint(label, source, locate) {
  const [result] = await eslint.lintText(
    `(function (__bootstrap) {\n${source}\n})();\n`,
    { filePath: `${label}.js` },
  );
  for (const message of result.messages) {
    const [file, line] = locate(message.line - 1);
    console.error(`${file}:${line}:${message.column}: ${message.message} (${message.ruleId ?? "syntax"})`);
  }
  errors += result.errorCount;
}

await lint("execute_worker", sources.join("\n"), (line) => {
  let index = 0;
  while (index < sources.length - 1 && line > sources[index].split("\n").length) {
    line -= sources[index].split("\n").length;
    index += 1;
  }
  return [units[index], line];
});
for (const script of ["bootstrap.js", "web/init.js"]) {
  await lint(script, await readFile(join(jsDir, script), "utf8"), (line) => [script, line]);
}
if (errors > 0) process.exitCode = 1;
else console.log(`runtime JavaScript: ${units.length} worker units, bootstrap.js and web/init.js name no globals and leave no unused bindings`);
