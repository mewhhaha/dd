import globals from "globals";

export default [{
  files: ["packages/dd-vite/src/**/*.js", "scripts/**/*.mjs"],
  languageOptions: {
    ecmaVersion: "latest",
    sourceType: "module",
    globals: globals.node,
  },
  rules: { "no-undef": "error" },
}];
