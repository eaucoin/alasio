// import-in-the-middle ships the declarations of register-hooks.mjs as register-hooks.d.ts,
// where TypeScript looks for register-hooks.d.mts; this points the module at them.
declare module "import-in-the-middle/register-hooks.mjs" {
  export * from "import-in-the-middle/register-hooks.js";
}
