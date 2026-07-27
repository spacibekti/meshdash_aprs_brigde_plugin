# Building MeshDash Plugins — an Agent's Guide

Distilled from building `aprs_bridge`, an in-process MeshDash plugin bridging
Meshtastic mesh traffic to APRS RF. Nothing here is specific to APRS — it's
what an agent should already know before touching *any* MeshDash plugin, so
the next build doesn't have to rediscover it the hard way.

## 1. What a MeshDash plugin is

A plugin is a folder under `plugins/<plugin_id>/` containing:

- **`manifest.json`** — `id` (must match the directory name), `watchdog`
  (`true`/`false`, mandatory), `entry_point` (usually `main.py`), and
  optionally `router_prefix` (mounts a FastAPI router), `static_prefix`
  (serves a `static/` folder), `nav_menu` (adds a sidebar link).
- **`main.py`** — must expose `init_plugin(context)`. Optionally exposes
  `plugin_router` (a FastAPI `APIRouter`, module-scope object MeshDash mounts
  automatically if `router_prefix` is set).
- **`setup.py`** (only if you need third-party deps beyond MeshDash's own) —
  installs them as a one-time subprocess step, gated by a `.setup_complete`
  sentinel file so it doesn't re-run every load.
- **`static/`** (only if you have a UI) — served at `static_prefix`.

MeshDash loads `main.py` via `importlib.util.spec_from_file_location`, **not**
as a normal package import — relative imports (`from . import foo`) will
break unless your plugin's directory is itself an importable package on
`sys.path`. Test this early: write a test that loads `main.py` the same way
MeshDash does, and catch relative-import mistakes in CI, not in production
logs after a broken deploy. (`test_api_routes.py`'s `_load_main_module`
pattern in this repo is a working example.)

## 2. Hard rules — will break the whole dashboard if violated

These aren't style preferences. Getting any of them wrong doesn't just break
your plugin — MeshDash plugins run **in-process with no sandbox**, so a
mistake here can take down every other plugin too.

- **Never open your own Meshtastic interface** (`meshtastic.SerialInterface`,
  `TCPInterface`, etc.). MeshDash owns the one connection to the radio. A
  second interface either fights over the serial port or double-processes
  every packet.
- **`pub.subscribe(callback, "meshtastic.receive")` is the correct way to get
  real-time RX** — confirmed by reading MeshDash's own production plugins
  (`mesh_ping`, `tcp_proxy`), which use exactly this pattern. It is *not* a
  second connection — it's an in-process listener on the one connection
  MeshDash already owns. Always `pub.unsubscribe(callback,
  "meshtastic.receive")` first, wrapped in `try/except`, before subscribing —
  otherwise a plugin reload double-registers the callback and every packet
  gets processed twice.
- **Every DM-consuming plugin sees every DM — pub/sub fans a packet out to
  every subscriber, and MeshDash has no concept of one plugin "owning" or
  claiming a message.** If your plugin has any kind of "unrecognized input
  falls back to some default action" behavior for DMs, that default action
  will eventually fire on a message that was actually meant for a *different*
  plugin's own command syntax — confirmed live, this silently misdirected
  content (in this project's case, transmitting it on RF) that the sender
  never intended to send there. Only act on a DM your plugin can recognize
  unambiguously (an explicit prefix/command); leave everything else
  completely untouched — no action *and* no reply, since even an unsolicited
  reply can interfere with whatever else was meant to handle it. Don't
  rate-limit, log-as-consumed, or otherwise treat an unrecognized DM as
  "yours" in any way before you've confirmed it actually is.
- **Transmit only via `await connection_manager.sendText(...)`**, and only
  after checking `connection_manager.is_ready.is_set()`. Don't queue sends
  when the connection isn't ready — log and drop (or retry later at a higher
  layer), don't block.
- **Background threads**: start them inside `init_plugin`, `daemon=True`.
  Heartbeat `context["plugin_watchdog"][plugin_id] = time.time()` at least
  every 120 seconds, or the watchdog will consider the plugin hung and
  restart it.
- **Cross-thread async calls**: a background thread can't `await` anything —
  schedule coroutines onto MeshDash's event loop with
  `asyncio.run_coroutine_threadsafe(coro, context["event_loop"])`.
- **DB access from async handlers** goes through `await
  asyncio.to_thread(...)` — don't block the event loop with synchronous
  SQLite calls in a FastAPI route handler.
- **Third-party dependencies** are installed by your own `setup.py` +
  `.setup_complete` sentinel, and **imported inside `init_plugin`, never at
  module scope**. Nothing at module import time may raise — MeshDash imports
  every plugin's `main.py` up front, and an unhandled exception there kills
  the dashboard for *all* plugins, not just yours. There's also a real
  timeout on this: init_plugin has ~15s, module import ~10s — don't do
  blocking network/dependency-install work at either stage.

## 3. Reliable mesh delivery — the empirical lessons

Everything in this section was discovered by testing on real hardware, not
by reading MeshDash's docs — the failure modes here are silent (no exception,
no error log) unless you specifically build in the visibility to catch them.

- **Concurrent `sendText` calls to different destinations can race and
  silently drop one of them.** If you ever fan a single event out to
  multiple mesh nodes, don't fire one `asyncio.run_coroutine_threadsafe` per
  destination independently and let them run unawaited/concurrently.
  Sequence them: one scheduled coroutine that `await`s each delivery in turn.
- **Sequencing alone isn't enough — the gap between sequential deliveries
  matters, and it's larger than you'd guess.** Even strictly sequenced, a
  too-short gap (0.5s, then 2.0s — both failed in testing) let a later
  delivery silently never reach its device, apparently because the earlier
  send's own ack-wait/retry cycle was still occupying the radio. 4.0s was
  the smallest value that held up reliably on the reference hardware. Make
  this a configuration value, not a hardcoded constant — the right number is
  a property of the specific radio/mesh, not something to guess once and
  ship.
- **`sendText(..., wantAck=True)` gets you real delivery confirmation and
  retry from Meshtastic's own routing layer** — distinct from any
  message-ID-based ack/retry your own protocol might implement on top. A
  plain fire-and-forget send can silently never reach a node (out of range,
  briefly asleep, etc.) with absolutely nothing in your own logs to show it.
  `wantAck=True` at least gives the mesh itself a chance to retry, and gives
  you a way to *observe* the difference (see next point).
- **How to actually verify mesh-level delivery while debugging**: MeshDash
  logs every outbound send as `[signal] Sending → dest=... ack=<bool>
  msg=...`. When `wantAck=True`, a genuine delivery shows up as a
  `ROUTING_APP` packet in the log, `fromId` matching the destination node —
  that's Meshtastic's own ack coming back over RF, independent of anything
  your plugin does. If you see the `Sending` line but never a matching
  `ROUTING_APP` reply, the packet didn't actually land on that device,
  regardless of what your own code returned. This was the only way to
  distinguish "our software did everything right but the radio/mesh didn't
  deliver it" from "our software has a bug" during live debugging.
- **Don't trust a single test to rule out a race.** A bug that only
  reproduces "sometimes" under fan-out, but never when addressing a single
  node directly, is a strong signal the bug is in the fan-out/sequencing
  logic, not the device — cross-check by sending to the same destination
  both ways (alone vs. as part of a batch) before concluding a device itself
  is unreliable.

## 4. Testing patterns

- **Fake the MeshDash context objects, don't spin up MeshDash.** A
  lightweight `FakeConnectionManager` (a real `threading.Event` for
  `is_ready`, an async `sendText` that just records its calls into a list)
  and a `SimpleNamespace(nodes=..., local_node_id=...)` standing in for
  `meshtastic_data` covers almost everything a bridge/relay-style plugin
  needs to test, with zero hardware and zero real MeshDash dependency.
- **Test the plugin module load itself**, not just its logic — load
  `main.py` the same way MeshDash's `importlib.util.spec_from_file_location`
  does, in a test, to catch relative-import breakage and module-scope
  exceptions before they reach production.
- **Full suite should run in single-digit seconds.** If a reliability fix
  introduces a real delay (e.g. the fan-out gap above), give tests a
  config override with a tiny value (milliseconds) instead of the
  production default — don't let correctness fixes slow down the whole
  suite by seconds per test.
- Every dataclass-based config object used across multiple test files needs
  every new field added to *every* test file's config-builder helper if it's
  a plain dataclass with no field defaults of its own (only the loader
  function should apply defaults) — a new required field breaks every other
  test file's fixture until updated.

## 5. Deployment gotchas

- **`rsync` with multiple source files at different directory depths and a
  single destination *directory* flattens every source to its basename.**
  `rsync a/b/c.html a/d.py dest/` does **not** preserve `b/c.html` — it
  copies both files directly into `dest/`, silently creating
  `dest/c.html` (wrong location) while leaving the real target
  (`dest/b/c.html`) untouched. This is an easy, quiet mistake when deploying
  a plugin's `static/` subfolder alongside top-level `.py` files in one
  command. Deploy nested paths with an explicit destination path per file,
  or rsync whole directories, not a flat list mixing depths.
- **Never blindly overwrite a live `config.json`** when deploying code —
  the running instance may have settings tuned via the plugin's own config
  UI that don't exist in your repo's template file. Deploy code files only;
  let new config fields default via `.get(key, default)` in your loader so
  old configs keep working without needing a matching edit.
- **Static file changes don't need a service restart** (served fresh from
  disk on each request) — but browsers cache aggressively, so a hard
  refresh is often needed to see the change. **Python code changes do need
  a restart** (`systemctl restart` or equivalent) since the module is only
  imported once at startup.

## 6. Config design pattern

A pattern that worked well end-to-end for a plugin with a live-editable
config UI:

1. A frozen dataclass (e.g. `BridgeConfig`) with every field, no defaults on
   the dataclass itself.
2. A `load_config(path) -> Config` function that reads JSON, validates
   required fields and value ranges, and supplies defaults for everything
   optional via `raw.get(key, default)`. Raise a clear `ConfigError` and
   refuse to run rather than starting half-configured — "fail loud."
3. A FastAPI `ConfigUpdateRequest` Pydantic model mirroring the dataclass,
   with matching defaults, used by a `POST /config` endpoint that writes
   the file (atomically — write to a temp path, validate it with the same
   `load_config`, then `os.replace`) rather than trusting the request body
   directly.
4. A web UI form field per config value, both reading (`c.field ??
   default`) and writing (`parseFloat($('id').value || 'default')`) with
   the same default as steps 2–3.
5. Keep a template `config.json` in the repo with sensible values — but
   remember step 5 above about not overwriting a live one.

Every new field touches all five places. Missing one doesn't break anything
immediately, but it does mean the default silently drifts out of sync
between the API, the UI, and fresh installs.

## 7. General practices worth carrying forward

- **Keep a living spec of hard invariants** (this project's `CLAUDE.md`) —
  especially for anything with legal/compliance stakes (this plugin
  transmits on licensed amateur radio spectrum). Write down not just *what*
  the rule is but *why*, so a future change can tell the difference between
  "this was a deliberate, confirmed decision" and "this was just how it
  happened to be built." Update it as design decisions evolve instead of
  letting it go stale — a stale spec is worse than none, because it reads
  as authoritative.
- **Build in phases, one fully tested before the next starts.** Protocol
  core → one-way proof of concept → full bidirectional flow → reliability →
  UI, in that order, with a real test suite gating each transition, kept
  this project's complexity manageable throughout.
- **Real hardware is the actual gate for reliability claims, not unit
  tests.** Unit tests can prove your code builds the right bytes; they
  cannot prove a real radio actually receives them. Every delivery-
  reliability fix in this project (concurrency, fan-out timing, `wantAck`,
  third-party ack framing) was discovered, and only confirmed fixed, by
  testing against live hardware — treat "tests pass" and "verified live" as
  two separate, both-required bars before calling something done.
