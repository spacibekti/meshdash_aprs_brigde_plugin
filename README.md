# APRS ⇄ Meshtastic Bridge (MeshDash plugin)

A [MeshDash](https://meshdash.co.uk/) plugin that bidirectionally bridges APRS RF (via a KISS TNC) and a Meshtastic mesh network. It lets Meshtastic users exchange direct messages with APRS stations on RF, and vice versa, under the callsign of a single licensed control operator.

> **This is amateur radio software.** Every RF transmission it makes is legally attributed to and controlled by one person's FCC (or equivalent) amateur radio license. Read [Compliance model](#compliance-model--fcc-part-97) before running this against a real transmitter.

## What it does

- **RF → mesh**: An APRS message addressed to your registered callsign, a live Meshtastic node's short name, or a node's 4-hex-char id code is delivered to the corresponding mesh device(s) as a direct message, prefixed with the sending station's callsign so the mesh recipient knows who's messaging them.
- **Mesh → RF**: A Meshtastic user DMs the gateway node with `CALLSIGN: message text`, or `#r message text` to reply to their last correspondent, and it goes out as a real APRS message on RF, attributed by the AX.25 source callsign (see below) and identified in the text by the sender's mesh name. A DM matching neither form is left completely alone — other MeshDash plugins also see every DM sent to the gateway node, so an unrecognized bare message is never guessed at or auto-forwarded; it might be meant for a different plugin entirely.
- **Registration**: `!register CALLSIGN-SSID` / `!unregister`, sent as a mesh DM, links a Meshtastic node to a callsign so RF stations can reach it by that callsign directly. Registration is optional — see below. This convention follows [aprstastic](https://github.com/afourney/aprstastic), the prior-art reference this project builds on.
- **Third-party relay**: Unregistered/unlicensed mesh users can still send and receive messages over RF. They're identified by their mesh device's name instead of a callsign, and the AX.25 source is always the gateway operator's own licensed callsign — this is the FCC §97.115 third-party-traffic model (a licensed control operator relaying communications on behalf of others), not a claim that every mesh user is individually licensed.
- **Reliable delivery**: APRS message-ID ACK/retry with a decaying retransmit schedule in both directions, content-based dedupe (so a digipeated repeat of the same packet isn't delivered twice), and per-direction / per-callsign rate limiting. An RF→mesh ack for a message addressed to anything other than the gateway's own callsign (a mesh short name, node-id code, or another registered callsign) is wrapped in standard APRS third-party-traffic format so the sender's own ack-matching recognizes it — a plain same-source ack there left real APRS clients retransmitting indefinitely, since ack-matching generally expects the ack to come from whoever the message was addressed to. The AX.25 source stays the gateway's own callsign either way; only the payload changes.
- **Multi-device callsigns**: One callsign can have several registered Meshtastic devices (e.g. an operator running more than one node). An RF reply is routed to whichever device most recently sent traffic under that callsign, or to every device if none has yet.
- **Conversation-aware reply routing**: Because every outbound frame's AX.25 source is always the gateway's own callsign, a reply from an RF station always comes back addressed to the gateway, not to the original mesh sender's (possibly nonexistent) callsign. The bridge tracks per-correspondent conversation history so that reply still reaches the right mesh device — including for unregistered/unlicensed senders, who have no callsign of their own to be addressed at.
- **Web UI**: A MeshDash nav-menu page for viewing bridge status, managing registrations (with a live mesh-node picker), and editing configuration (applied on next restart).

## Compliance model — FCC Part 97

This is deliberately not a "gate everything behind a verified license" design. It follows the same model used by real-world callsign↔mesh/SMS gateways:

- **Every RF transmission carries the gateway operator's own licensed callsign as its AX.25 source**, regardless of who originated the content on the mesh side. That's the accountability mechanism — the FCC's §97.115 third-party-traffic model, where a licensed control operator may relay communications on behalf of others.
- **`!register` performs no license-ownership verification.** It's self-service, matching the [aprstastic](https://github.com/afourney/aprstastic) model this project draws on: anyone can claim any syntactically valid callsign. This is an accepted, known tradeoff, not an oversight.
- **A registered callsign is required to receive RF messages addressed specifically to that callsign.** It is *not* required to send mesh→RF or to receive replies — unregistered senders are still relayed (identified by mesh device name instead of a callsign), and replies find their way back via conversation tracking, not registration.
- **Broadcast/channel traffic is never bridged to RF, structurally.** The mesh→RF path only ever reads direct messages addressed to the gateway's own node. There is deliberately no channel-index allowlist on top of that — on real hardware, Meshtastic DMs don't carry usable channel-encryption metadata, so such a check cannot discriminate anything and would be a false sense of security.

**Whether running this fully automated satisfies the "control operator present and able to supervise/terminate" requirement of §97.115 is a legal judgment for the license holder operating the gateway, not something this software resolves.** If you deploy this, that determination is yours to make.

## Architecture

```
        KISS-over-TCP                                  Meshtastic
   ┌───────────────┐   AX.25 UI frames   ┌─────────────────────────┐
   │  TNC / radio  │◄───────────────────►│  aprs_bridge (this repo)│◄──── pub/sub RX,
   │ (e.g. Direwolf│                     │   running inside        │      connection_manager
   │  / SoundModem)│                     │      MeshDash            │      .sendText() TX
   └───────────────┘                     └─────────────────────────┘
```

- **TNC layer** (`transport.py`): owns a KISS-over-TCP socket to a TNC such as Direwolf or a hardware KISS modem. Reconnects automatically. (Serial KISS and AGW are architecturally anticipated but not yet implemented — only `kiss_tcp` ships today.)
- **Protocol layer** (`protocol/`): pure-Python, zero I/O. KISS framing (`kiss.py`), AX.25 UI frame encode/decode built on `ax253` (part of the `python-aprs` project) (`ax25.py`), hand-rolled APRS message/ACK parsing (`aprs_message.py` — deliberately not built on `aprs3`'s `Message` class, which silently truncates/mangles invalid input rather than rejecting it), dedupe (`dedupe.py`), and token-bucket rate limiting (`ratelimit.py`). Every function here has unit tests against known-good hex frames.
- **Bridge layer**: `bridge.py` (RF → mesh) and `mesh_bridge.py` (mesh → RF) — the actual routing, registration, ACK, and third-party-relay logic described above.
- **Registry** (`registry.py`): a small SQLite database (separate file from MeshDash's own DB) holding callsign↔node registrations, last-correspondent state, and conversation-routing history.
- **MeshDash integration** (`main.py`): a standard MeshDash plugin — `init_plugin(context)` wires everything up, subscribes to `meshtastic.receive` for real-time RX, and never opens its own connection to the radio (MeshDash owns that). Exposes a FastAPI `plugin_router` for the web UI.

See [`docs/RESEARCH.md`](docs/RESEARCH.md) for the full protocol/architecture research this was built from, and [`CLAUDE.md`](CLAUDE.md) for the detailed build spec and hard invariants this project follows.

## Requirements

- Python 3.9+
- A running [MeshDash](https://meshdash.co.uk/) instance with a connected Meshtastic node
- A KISS-over-TCP capable TNC (hardware TNC, or software like Direwolf / SoundModem) connected to an amateur radio transceiver
- An amateur radio license, and the willingness to take responsibility for everything this software transmits under your callsign

## Installation

1. Copy (or clone) this repository's `plugins/aprs_bridge/` directory into your MeshDash installation's `plugins/` directory.
2. Edit `plugins/aprs_bridge/config.json` (see [Configuration](#configuration) below) — at minimum, set `gateway_callsign` to your own licensed callsign-SSID and point `tnc_host`/`tnc_port` at your TNC.
3. Restart MeshDash. On first load, the plugin's `setup.py` installs its third-party dependencies (`kiss3`, `ax253`, `aprs3`) into MeshDash's environment automatically.
4. Open the **APRS Bridge** page from MeshDash's nav menu to confirm the bridge connected to your TNC.

## Configuration

`plugins/aprs_bridge/config.json`:

| Field | Description | Default |
|---|---|---|
| `tnc_mode` | TNC connection mode. Only `"kiss_tcp"` is currently supported. | *(required)* |
| `tnc_host` / `tnc_port` | TNC's KISS-over-TCP address. | *(required)*
| `kiss_port` | KISS port number to use in framing (usually `0`). | `0` |
| `gateway_callsign` | **Your own licensed callsign-SSID.** Used as the AX.25 source on every outbound frame. | *(required)* |
| `aprs_tocall` | APRS destination/tocall used in outgoing frames. | `"APZBRD"` |
| `digi_path` | Digipeater path for outgoing frames. | `["WIDE1-1", "WIDE2-1"]` |
| `mesh_channel_index` | Meshtastic channel index used when sending mesh DMs. | `0` |
| `dedupe_ttl_sec` | How long a message signature is remembered to suppress duplicates/digipeated repeats. | `30` |
| `rate_limit_per_min` / `rate_limit_burst` | Per-direction token-bucket rate limit. | `20` / `10` |
| `per_callsign_rate_limit_per_min` / `per_callsign_rate_limit_burst` | Per-sender token-bucket rate limit. | `6` / `3` |
| `ack_retry_intervals_sec` | Decaying retransmit schedule for un-ACKed outbound messages. | `[30, 60, 120]` |
| `ack_max_attempts` | Attempts before giving up on a message. | `4` |
| `mesh_fanout_delay_sec` | Gap between sequential RF→mesh deliveries when a message reaches more than one device at once (multi-device callsign, `!ALL`). Too short a value can silently drop a later delivery even though nothing errors — tune it up if that happens on your hardware. | `4.0` |

Configuration can also be viewed and edited from the web UI's Config tab; changes there are written back to `config.json` and take effect on the bridge's next restart.

## Usage

### From the mesh side

- **Register your node to a callsign** (optional, needed to receive RF messages addressed to that callsign specifically):
  ```
  !register W4BRD-13
  ```
- **Unregister:**
  ```
  !unregister
  ```
- **Send a message to an RF station:**
  ```
  WU2Z: hello from the mesh
  ```
- **Reply to whoever you last messaged**, with `#r message text` — this works even the very first time an RF station messages you, since receiving a message also sets who you last talked to, not just sending one. A bare DM with neither a `CALLSIGN:` prefix nor `#r` is left untouched, since it might be meant for a different MeshDash plugin.

Unregistered nodes can do all of the above except receive messages addressed specifically to a callsign — an RF sender can still reach them directly by mesh short name or node-id code (see below), or by simply replying to a message they sent.

### From the RF side

- Send a normal APRS message addressed to a registered callsign, and it's delivered to the Meshtastic device registered under that callsign — if there's more than one, only to whichever sent a message most recently (falling back to every device if none has yet). Start the message with `!ALL` (e.g. `!ALL check in`) to force delivery to every registered device regardless; whichever one replies afterward becomes the new most-recently-active device, so routing narrows back to just them automatically.
- Send a message addressed to a live mesh node's short name, or to the last 4 hex characters of its node id (e.g. a node `!a1b2c3d4` is reachable as `C3D4`), to reach an unregistered node directly. The node-id code is the more reliable option — a Meshtastic short name can be arbitrary unicode or unset, but the code is always ASCII, always present, and it's the same code you'll see as the sender's name on a message from a node that has no short/long name configured.
- Reply to a message you received from the gateway, and it's routed back to whichever mesh device the conversation belongs to — this works even if that device was never registered.

## Testing

```
pip install -e ".[dev]"
pytest
```

The test suite is fully self-contained — no hardware, no live MeshDash, and no network access required. Protocol-layer tests (`tests/protocol/`) verify KISS/AX.25/APRS encode-decode against known-good hex fixtures; plugin-layer tests (`tests/plugin/`) exercise the bridge logic against lightweight fakes of MeshDash's `connection_manager`, `meshtastic_data`, and event loop.

## Known limitations

- Only KISS-over-TCP TNCs are supported today; serial KISS and AGW (port 8000) are anticipated in the architecture but not implemented.
- No APRS-IS gateway integration.
- No EU-style LoRa duty-cycle enforcement (rate limiting is a simple token bucket, not a rolling 10%/hour cap) — relevant if operating under an ETSI region's duty-cycle rules rather than FCC Part 97.
- `!register` accepts any syntactically valid callsign with no license-ownership verification (see [Compliance model](#compliance-model--fcc-part-97)).

## Acknowledgements

The callsign↔node registration convention (`!register`/`!unregister`, `CALLSIGN: message` addressing) follows the pattern established by [aprstastic](https://github.com/afourney/aprstastic).

## License

BSD 3-Clause. See [`LICENSE`](LICENSE).
