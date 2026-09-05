# bender

## ⚠️ maintenance status ⚠️

This project is actively maintained only for the goal of running [Embassy](https://github.com/embassy-rs/embassy)'s CI. I don't have bandwidth to maintain it for other use cases. If you need help or want to contribute big features feel free to ask, but a positive response (or a response at all) is not guaranteed.

## Features

- Doesn't suck

## Requirements

- nftables
- containerd
- Linux kernel v5.13+ (for nftables cgroupv2 matching)
- `data_dir` must be in a BTRFS filesystem.
  - If you're running as non-root, it must be mounted with the `user_subvol_rm_allowed` option.

## Getting Started

- Depending on where your repos are:
  - If they're in your personal account: go to your personal settings -> Developer settings -> GitHub apps -> New GitHub App
  - If they're go to your organization's Settings -> Developer settings -> GitHub apps -> New GitHub App
- Fill the form like this:
  - GitHub App name: enter some cool name.
  - Homepage URL: the URL where you're going to deploy Bender. e.g. `https://bender.example.com`
  - Webhook URL: The url, with `/webhook` added. e.g. `https://bender.example.com/webhook`
  - Webhook secret: Generate a long and secure random string. For example with `pwgen -s 32`.
  - Callback URL: The url, with `/auth/callback` added. e.g. `https://bender.example.com/auth/callback`. This is only needed for "Log in with GitHub" in the web UI, see below.
  - Repository permissions
    - Commit statuses: Read and write
    - Contents: Read-only
    - Pull requests: Read-only
  - Subscribe to events
    - Pull request
    - Push
  - Where can this GitHub App be installed?: Only on this account.
    - IMPORTANT: If you set it to "Any account" instead, then ANYONE on GitHub will be able to use your CI service on THEIR repos.
- Create
- In "Private keys", click "Generate a private key". Keep the downloaded `.pem` file.
- If you want web UI login, note the "Client ID" and use "Generate a new client secret". Both are on the same page.
- In the left menu click "Install App"
- Select the repositories you want to use Bender with.

- Write the following into `config.toml`.

```yaml
external_url: https://bender.example.com # replace
data_dir: data
listen_port: 8000
image: embassy.dev/ci:latest
net_sandbox:
  allowed_domains:
    - "*.github.com"
    - "*.githubusercontent.com"
github:
  webhook_secret: REPLACE_ME_WITH_YOUR_SECRET # replace
  app_id: 321321 # replace
  private_key: | # replace
    -----BEGIN RSA PRIVATE KEY-----
    MIIEpQxxxxxxxxx
    xxxxxxxxxxxxxREPLACE_MExxxxxxxxx
    xxxxxx9N7c=
    -----END RSA PRIVATE KEY-----
```

- Run `bender -c config.toml`

## Web UI login

The dashboard is public and read-only. To let people cancel jobs from it, add the
GitHub App's OAuth credentials to the `github:` section of the config:

```yaml
github:
  # ... webhook_secret, app_id, private_key as above ...
  client_id: Iv1.xxxxxxxxxxxx # replace
  client_secret: REPLACE_ME # replace
  session_secret: REPLACE_ME # replace, e.g. `pwgen -s 64`
```

Make sure the app's Callback URL is `<external_url>/auth/callback`.

Anyone can then log in with GitHub, but they can only cancel a job if they have
**push access to that job's repository** — checked against GitHub with the user's
own token every time, so access follows whatever the repo already says.

Sessions are signed cookies and are never stored server-side, so there's no
database. They don't expire; changing `session_secret` logs everyone out.

Leave these three settings out and the UI stays read-only for everyone, with no
login link.

## Merge queue support

Bender understands GitHub merge queue branches (`gh-readonly-queue/...`) out of
the box.

- Long jobs keep refreshing their GitHub status (every `status_refresh_interval`,
  default 30 minutes). This matters because GitHub's merge queue fails a group
  when a required check goes silent for too long (default 60 minutes).

Add this to `config.toml`:

```yaml
# Re-post a pending status to GitHub this often for jobs that are still
# running or queued. Prevents GitHub's merge queue check timeout (default
# 60 min) from failing long-running groups. Set to 0s to disable.
status_refresh_interval: 30m
```
