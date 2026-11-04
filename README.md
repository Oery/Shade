# Shade

A personal agent that runs on your own machine. You talk to it in the terminal, on Discord, or through the [client API](docs/api.md); it can also run scheduled jobs and react to webhooks. What it may do is decided by a config file you write, not by the model: each conversation, job and webhook gets only the capabilities you grant it.

- One daemon (`shaded`) and a thin client (`shade`), in Rust, on [Infinity](https://infinity.hydro.run/docs/infinity-runtime/overview/).
- State in one SQLite database: conversations, jobs, runs, and every tool call's outcome. Work accepted before a crash is finished or reported after it, never silently dropped or repeated.
- Tools: the time, notifications, a sandboxed shell (bubblewrap), scheduled jobs, and any MCP server you configure (each server is a capability).
- Memory: a small index of facts the model keeps in your Obsidian vault, shown only where the vault is granted.

[PLAN.md](PLAN.md) holds the design and every milestone's decisions; [AGENTS.md](AGENTS.md) the coding rules.

## Install

Linux only. Needs `bwrap` (bubblewrap) if anything is granted `shell`.

On Arch Linux, build and install the package from this repository (it packages the committed HEAD):

```sh
cd packaging/arch && makepkg -si
```

It installs `shaded` and `shade` in `/usr/bin`, the systemd user units, and `config.example.toml` and this README in `/usr/share/doc/shade`. If the sandbox tests cannot run where you build (no user namespaces), add `--nocheck`.

On NixOS, use the flake's module (it builds with the toolchain pinned in `rust-toolchain.toml`):

```nix
# flake.nix
inputs.shade.url = "github:Oery/Shade";

# in your NixOS configuration
imports = [ inputs.shade.nixosModules.default ];
services.shade.enable = true;          # shaded as a user service, bubblewrap on its PATH
users.users.<you>.linger = true;       # start it at boot
```

The daily backup timer is on by default (`services.shade.backups`). Each user keeps their own `~/.config/shade`. `nix build github:Oery/Shade` builds just the package.

With Home Manager (NixOS or any other distribution), the user module also writes the config from Nix:

```nix
imports = [ inputs.shade.homeModules.default ];
services.shade = {
  enable = true;                       # shaded as a user service, daily backups
  settings = {                         # becomes ~/.config/shade/config.toml
    timezone = "Europe/Paris";
    model = { provider = "anthropic"; id = "claude-opus-5-5"; api_key_env = "ANTHROPIC_API_KEY";
              context_window = 1000000; max_output_tokens = 128000; };
    chat.caps = [ "now" "notify" ];
  };
};
```

`settings` ends up in the Nix store, so it only ever names secrets; their values go in `~/.config/shade/.env` (`chmod 600`). Leave `settings` empty to keep writing `config.toml` yourself. `shaded` restarts when the generated config changes. Home Manager cannot enable lingering: `loginctl enable-linger "$USER"` once, or `users.users.<you>.linger = true` on NixOS.

Elsewhere, build with the toolchain from `rust-toolchain.toml`:

```sh
cargo install --locked --path crates/daemon   # shaded
cargo install --locked --path crates/cli      # shade
mkdir -p ~/.config/systemd/user && cp deploy/*.service deploy/*.timer ~/.config/systemd/user/
```

The units expect `/usr/bin`; point them at `~/.cargo/bin` with `systemctl --user edit shaded` (and `shade-backup`):

```ini
[Service]
ExecStart=
ExecStart=%h/.cargo/bin/shaded
```

## Configure

```sh
mkdir -p ~/.config/shade
cp config.example.toml ~/.config/shade/config.toml
touch ~/.config/shade/.env && chmod 600 ~/.config/shade/.env
```

`config.example.toml` documents every section. Secrets never go in the config: it names environment variables (`api_key_env`, `token_env`, an MCP server's `env`), read from the environment or the `.env` next to the config, which must be `chmod 600`. A minimal setup is `[model]` plus `[chat] caps = ["now"]`; add capabilities one at a time:

| Section | Gives |
| --- | --- |
| `[chat]` | What owner chat may use: `now`, `notify`, `shell`, `shell_network`, `schedules`, and MCP server names. |
| `[discord]` | Owner chat (DMs and mentions), guest mentions, notifications, approval buttons. |
| `[discord.guests]` | Tools non-owners may use when mentioning the bot in server channels; none by default. |
| `[sandbox]` | Where `shell` runs. Required when anything holds `shell`. |
| `[mcp.<name>]` | An MCP server over stdio; its name becomes a capability. |
| `[memory]` | Memories in the vault of one MCP server. |
| `jobs/*.toml` | Long-lived scheduled jobs, next to the config. Owner chat can draft them; you approve each change. |
| `[api]`, `[http]` | Personal HTTP JSON/SSE API for native clients and scripts; see [the API guide](docs/api.md). |
| `[hooks.<name>]`, `[http]` | Webhooks, for this machine and your tailnet. |
| `[limits]` | Bounds on every turn and run, and on background work. |

The config is read once: restart `shaded` to apply a change.

The shell uses system tools plus entries from the daemon's `PATH` that resolve
into `/nix/store`. This includes NixOS user profiles such as
`/etc/profiles/per-user/<user>/bin`, when present on the daemon's `PATH`.
The resolved store paths are used inside the sandbox; your home and profile
directories stay hidden. Programs still need `shell_network` for network access
and explicitly configured environment variables for credentials.

On Discord, `owners` use `[chat]` permissions. Other users can mention the bot
in server channels with `[discord.guests] caps` (for example, `["now"]`). Guests
cannot DM the bot, manage schedules or watches, use `/new`, or approve changes.
Every channel has separate owner and guest histories; guests share one
conversation in that channel and do not receive the owner's custom chat prompt.
Guest capabilities are configured independently of owner capabilities. Giving
guests an MCP server also shares that server's tools and any memory tied to it;
`notify`, if granted, may message only the owner.

## Run

```sh
shaded &                  # or the systemd service below
shade chat                # terminal chat; `shade chat --thread work` for another conversation
shade ask "what time is it?"
shade schedule list       # jobs; also pause, resume, rm, approve, reject
shade usage               # tokens per conversation
shade audit --since 1d    # grants, tool outcomes, run status and usage
```

`shade usage` shows the 20 most recently active roots, their origin and status,
and all-history totals by origin/source (up to 100 groups). Compaction requests
count toward their root. To follow one root, copy its ID into
`shade audit --since 7d --root <id>`; audit shows up to 20 roots with their last
50 entries in that window. Times are UTC. The grant shown is the persisted
delegation; `effective` also applies the current config.

Tool dispatches and captured outcomes come from the durable operation ledger.
Lifecycle observations and provider-reported usage share a queue of 128 records;
pressure or a crash can lose them, and the reports show this process's loss
count. Arguments, result bodies, prompts and webhook payloads are omitted from
audit summaries. Whole known secrets of at least 8 bytes are removed from shell
and MCP results before recording and delivery, and from reported run failures.
This does not scrub existing history, transformed secrets, or fragments cut by
tool output limits. MCP diagnostics log only byte counts, since a server may
print credentials to stderr.

Optional `[model.prices]` rates add estimated USD, saved per request; see the
example config. A dash means some usage had no price. These are estimates based
on reported tokens, not bills; rig 0.31 currently omits cached streaming counts.

Owner-chat messages queue in order within each conversation across all clients;
separate conversations run concurrently. API clients can browse saved owner
histories, stream replies and tool activity, and approve proposed changes.
Enable `[api] token_env = "SHADE_API_TOKEN"` and follow [the client API guide](docs/api.md).

### As a systemd user service

```sh
systemctl --user enable --now shaded
loginctl enable-linger "$USER"     # start at boot, without logging in
journalctl --user -u shaded -f     # logs
```

The service restarts on failure. On stop it gets 30 seconds to finish gracefully, then anything left (MCP servers, sandboxed commands) is killed; work it was doing is recovered at the next start.

### Webhooks over Tailscale

Webhooks are served on loopback or a Tailscale address only. Keep the default `127.0.0.1:8787` and put Tailscale in front:

```sh
tailscale serve --bg --https=443 http://127.0.0.1:8787
```

Then `POST https://<machine>.<tailnet>.ts.net/hook/<name>` with `Authorization: Bearer <token>`. Generate tokens with `openssl rand -hex 32`.

## Back up and restore

A backup is a consistent copy of the database, taken while the daemon keeps serving:

```sh
shade backup ~/shade-backup.db      # refuses an existing file; creates it 0600
```

Daily backups, kept 14 days in `~/.local/state/shade/backups`:

```sh
systemctl --user enable --now shade-backup.timer
```

A backup holds the database only: conversations, jobs (including job files' state), runs, tool outcomes, and usage. Back up the rest yourself: `~/.config/shade` (the config, its `.env`, and `jobs/`), the sandbox workspace, and your vault.

To restore:

```sh
systemctl --user stop shaded
cd ~/.local/share/shade
mkdir -m 700 replaced && mv shade.db shade.db-wal shade.db-shm replaced/ 2>/dev/null
cp ~/.local/state/shade/backups/shade-<date>.db shade.db && chmod 600 shade.db
systemctl --user start shaded
```

At start, Shade finishes or reports whatever the backup had in flight: unanswered inputs are sent again, tool calls whose result was not recorded are reported as unknown, and runs that cannot resume are marked interrupted.

To rehearse a restore without touching anything real, open the backup in an isolated data directory with a config that cannot reach any service: `[model] base_url` pointing at a closed local port, no `[discord]`, `[mcp]` or `[hooks]`, and `[limits] max_concurrent_background_runs = 0`.

```sh
mkdir -p /tmp/restore/shade && cp <backup> /tmp/restore/shade/shade.db
XDG_DATA_HOME=/tmp/restore shaded --config /tmp/rehearsal/config.toml &
XDG_DATA_HOME=/tmp/restore shade schedule list
XDG_DATA_HOME=/tmp/restore shade usage
```

## Operating limits

- **Run deadlines and model capacity.** `max_run_seconds` bounds provider requests, response streams and shell/MCP/notification waits, including time waiting for a model slot. Expired runs report a limit failure; interrupted writes may already have taken effect. `max_concurrent_completions` (8 by default) limits active provider requests and streams across all chats and background runs. Background runs also have their own concurrency limit.
- **Owner control.** The control socket trusts the local user who runs `shaded`. Discord owners use `[chat]`; other users' server mentions use `[discord.guests]`, with separate history and no tools by default.
- **No exactly-once side effects.** A tool call interrupted before its result was recorded is reported as possibly done, never retried blindly. Notifications and MCP writes can therefore be missed or, if the model retries after checking, done once more.
- **Grants limit, they do not judge.** A conversation, job or webhook can only use what it was granted, but whatever writes to it (a webhook sender, a web page the shell fetches, a note in the vault) can steer what it does within those caps. Grant narrowly.
- **The sandbox isolates, it does not meter.** Shell commands see only the workspace and get no network without `shell_network`; time and output are bounded, CPU, memory and disk are not.
- **Config changes need a restart.** Nothing is reloaded live; a capability removed from the config disappears from existing conversations at the next start.
- **Memory is small on purpose.** The index the model sees is at most 4 KiB.
- **Backups exclude secrets and files.** See above.
