# Telegram Private Chatbot (Cloudflare Workers Edition)

This project is a Telegram private-chat relay bot running on Cloudflare Workers.

Current behavior:
- Users must pass a human verification quiz (button choices).
- After verification, user messages are forwarded to one admin account (private chat mode, not group topic mode).
- Admin can reply directly to forwarded messages and the bot will relay replies back to users.
- Admin commands support ban/unban, close/open conversation, reset verification, and info query.

## What Changed

The current `worker.js` uses **ADMIN UID mode** and no longer depends on Telegram group topics.

Required variable change:
- Old setup (common in older forks): `SUPERGROUP_ID`
- Current setup: `ADMIN_UID`

## Features

- Human verification:
  - Local question bank (`LOCAL_QUESTIONS`)
  - Multiple-choice inline buttons (no text input needed)
  - Challenge TTL: 5 minutes (`VERIFY_TTL_SECONDS`)
  - Verified session TTL: 30 days (`VERIFIED_TTL_SECONDS`)
- Message relay:
  - User -> admin private chat (forwarded message)
  - Admin reply-to-forwarded-message -> user
- Control:
  - Ignore user slash commands except `/start`
  - Ban/unban, close/open session, verification reset, status check

## Admin Commands

- `/ban <uid>`: ban user (all messages ignored silently)
- `/unban <uid>`: unban user
- `/close <uid>`: close conversation (user sees close notice)
- `/open <uid>`: reopen conversation
- `/reset <uid>`: clear verification state; next message requires verification again
- `/info <uid>`: show user status (Verified/Banned/Closed)

Notes:
- You can omit `<uid>` by replying to a forwarded user message first, then sending the command.

## Prerequisites

1. Create a Telegram bot
   - Use [@BotFather](https://t.me/BotFather) to get `BOT_TOKEN`.

2. Get your admin UID
   - Use `@userinfobot` (or similar) to get your numeric Telegram user ID.
   - This value will be used as `ADMIN_UID`.

3. Prepare Cloudflare Workers
   - Enable Workers in your Cloudflare account.
   - Create one KV namespace (recommended name: `TOPIC_MAP`).

## Deploy via Cloudflare Dashboard (Manual)

1. Cloudflare -> Workers & Pages -> Create Worker.
2. Open code editor and replace code with this repo's `worker.js`.
3. In Worker settings -> Variables and Secrets, add:
   - Text variable: `BOT_TOKEN` = your Telegram bot token
   - Text variable: `ADMIN_UID` = your numeric admin UID
4. Add KV binding:
   - Binding name: `TOPIC_MAP`
   - Namespace: choose your KV namespace
5. Save and deploy again.

## Deploy via Wrangler (CLI)

Install tools:

```bash
npm install -g wrangler
wrangler login
```

Create KV namespace (first time only):

```bash
wrangler kv namespace create TOPIC_MAP
```

Put returned namespace `id` into `wrangler.toml`.

Set secrets:

```bash
wrangler secret put BOT_TOKEN
wrangler secret put ADMIN_UID
```

Deploy:

```bash
wrangler deploy
```

## Set Telegram Webhook (Required)

After deployment, set webhook to your Worker URL:

```text
https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook?url=<YOUR_WORKER_URL>
```

Example:

```text
https://api.telegram.org/bot123456:ABCDEF/setWebhook?url=https://telegram.example.workers.dev
```

Success response:

```json
{"ok":true,"result":true,"description":"Webhook was set"}
```

## Quick Test

1. A normal user sends a message to the bot.
2. Bot sends verification options.
3. User answers correctly and sends another message.
4. Admin receives forwarded message.
5. Admin replies to that forwarded message.
6. User receives admin reply.

## Troubleshooting

1. Webhook set, but no response
   - Confirm latest Worker version is deployed.
   - Confirm variable names are exact: `BOT_TOKEN`, `ADMIN_UID`, `TOPIC_MAP`.
   - Re-run webhook setup (delete + set if needed).

2. Admin reply does not reach user
   - Admin must reply to the forwarded message.
   - Only the account matching `ADMIN_UID` is treated as admin.

3. Verification keeps repeating
   - Check KV binding `TOPIC_MAP` is correctly attached to a real namespace.

## Project Files

- `worker.js`: active runtime logic
- `wrangler.toml`: Cloudflare deploy config
- `ref_worker.js` / `worker.raw.js`: historical/reference files (not necessarily production)

## Security

Keep `BOT_TOKEN` and Cloudflare credentials private.  
Never commit secrets into a public repository.
