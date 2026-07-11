# @mewhhaha/create-dd

Create a dd project from one of the published templates.

```sh
pnpm create @mewhhaha/dd
```

Pass the directory and template to skip the interactive prompts:

```sh
pnpm create @mewhhaha/dd my-app --template react-router
```

Templates: `react-router`, `react-router-rsc`, and `hono`. Dependencies install by default; pass `--no-install` to skip it. Use `--package-manager pnpm|npm|yarn|bun` to select an installer.
