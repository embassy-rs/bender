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
external_url: https://bender.example.com  # replace
data_dir: data
listen_port: 8000 
image: embassy.dev/ci:latest
net_sandbox:
  allowed_domains:
  - '*.github.com'
  - '*.githubusercontent.com'
github:
  webhook_secret: REPLACE_ME_WITH_YOUR_SECRET  # replace
  app_id: 321321  # replace
  private_key: |  # replace
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
  client_id: Iv1.xxxxxxxxxxxx  # replace
  client_secret: REPLACE_ME  # replace
  session_secret: REPLACE_ME  # replace, e.g. `pwgen -s 64`
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
the box. Two background tasks keep the merge queue from wedging:

- Long jobs keep refreshing their GitHub status (every `status_refresh_interval`,
  default 30 minutes). This matters because GitHub's merge queue fails a group
  when a required check goes silent for too long (default 60 minutes).
- A poller reconciles each configured repo's queue branches against bender's
  in-memory job queue. A queue branch with no live job is a push notification
  bender missed — e.g. because it was restarting, which loses the in-memory
  queue. After `event_grace` (which accounts for GitHub's event delivery
  delay), the job is reconstructed and put back into the queue.

Add this to `config.toml`:

```yaml
# Re-post a pending status to GitHub this often for jobs that are still
# running or queued. Prevents GitHub's merge queue check timeout (default
# 60 min) from failing long-running groups. Set to 0s to disable.
status_refresh_interval: 30m

merge_queue:
  enabled: true
  repos: [embassy-rs/embassy]  # owner/repo list to poll
  poll_interval: 5m      # how often to list the queue branches
  event_grace: 15m       # wait this long for the webhook before treating
                         # a queue branch's job as missed
  job_ttl: 24h           # remember finished merge-queue jobs this long, so
                         # we don't retrigger a group that's still waiting
                         # on another required check
  # What to do with a missed notification. "retrigger" (default) puts the CI
  # jobs back into the in-memory queue. "fail" posts a failure status on
  # fail_contexts so GitHub removes the PR from the queue.
  action: retrigger
  fail_contexts: []      # e.g. ["ci/build"], only used with action: fail
```

The poller keeps everything in memory — branch listings are compared against
the in-memory queue each round, with timestamps purged as branches leave the
queue, so nothing accumulates on disk.
