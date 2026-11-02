shade is a personal agent harness.

The runtime should stay very simple.

It needs to support tools calls and MCP servers.

It needs to support cron jobs. It needs to support chatting using other services (for example, Discord).

shade should use Rust for the runtime language. However the code must stay dead simple. Very low complexity.

Avoid cloning. Dont unwrap/expect. prefer returning a new error instead of using map_err. Avoid chaining many functions. Avoid long functions.

Write functions as short paragraphs, not one dense block. Put a blank line between unrelated steps: between groups of declarations, before and after control-flow blocks (`if`, `match`, `loop`, `for`, `while`) that are separate steps, and before the final return value, which goes on its own line. Lines that belong to one step stay together.

EXAMPLE USE CASES:
- Asking the agent to plan a task that will get triggered at some date
- Agent can write into a shared Obsidian database (probably should not be part of the runtime)
- Agent can be triggered by other services. Maybe through webhooks or something.
- Agent should have shell access

PERMISSIONS:
- Each task should run with limited permissions. For example, only chatting should be able to edit schedules tasks.

Shade should use the infinity framework
https://infinity.hydro.run/docs/infinity-runtime/overview/
