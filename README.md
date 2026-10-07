# alasio

## Hosting

On a Linux x86-64 machine with Node 24 and sudo, make a Telegram bot with
BotFather, then:

```sh
npx alasio init
npx alasio up
```

`npx alasio status` shows how it runs, and `npx alasio@latest upgrade`
upgrades it.

## Development

alasio is TypeScript that Node 24 runs as it is:

```sh
npm ci
npm run typecheck && npm test
```

`npm run test:e2e` runs it end to end, in Docker.
