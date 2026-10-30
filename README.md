# Bling-a-Ring

One self-hosted NVR for **Ring + Blink**, or for **Blink alone**: a single timeline of
clips, live view, a doorbell chime and motion push, and in-app device discovery.
Runs on Debian/Ubuntu (a mini-PC, a Pi, a VPS, an EC2 box). No subscription needed
beyond what the cameras themselves require.

## What it does

- **Records** Ring motion and doorbell (ding) clips, and pulls Blink motion/doorbell
  clips, into one `clips_dir`, shown in one timeline with source (Ring/Blink) and
  type (doorbell/motion) badges.
- **Live view** in the browser: Ring cameras through go2rtc, Blink Mini cameras
  through Blink's own live stream (the Blink Video Doorbell returns no live stream
  through blinkpy, so it shows clips and events only).
- **Doorbell chime + motion notifications** over one event bus:
  - phone and desktop notifications sent by the page itself (Web Push), arriving
    with the browser closed, no extra app,
  - in-browser chime + toast while the web UI is open (WebAudio, no asset needed),
  - optional chime on the host's own speaker; optional ntfy for ntfy-app users.
- **Per-camera motion on/off** and snapshots from the Cameras page.
- **In-app discovery**: Settings > Discover devices finds new Ring cameras (via
  ring-mqtt) and Blink devices (via your Blink login) and wires up the streams.

## Architecture (services)

```
ring-mqtt ──▶ mosquitto (127.0.0.1:1883) ◀── bar-blink (blinkpy + FCM)
                   │  ring/+/camera/+/{motion,ding}/state   blink/<net>/<dev>/<event>
                   ▼
   bar-recorder (Ring clips)      bar-notifier ──▶ bar/events ──▶ bar-web (SSE chime)
   bar-go2rtc-restream (live)     bar-notifier ──▶ Web Push (phones) + local speaker
   bar-blink (Blink clips+events) bar-web (Flask UI: http :8970 landing, https :8943 app)
```

All event sources land on MQTT; the notifier is the single fan-out to chime + push;
the web UI reads the same bus for its live in-browser chime.

In **Blink-only mode** the left column is not installed at all: no Node.js, no
ring-mqtt, no go2rtc, no recorder. What runs is mosquitto, bar-blink, bar-notifier
and bar-web.

## Install

A Debian or Ubuntu box (tested on Ubuntu 26.04), amd64 or arm64, with sudo. Not
Amazon Linux, Fedora or Alpine: the installer uses apt.

```
git clone https://github.com/twojast4ra/bling-a-ring.git
cd bling-a-ring

sudo BLINK_ONLY=1 bash install.sh        # Blink only
sudo bash install.sh                     # Ring + Blink
```

Options: `CLIPS_DIR=/mnt/ssd/clips` puts recordings somewhere else (default
`/var/lib/bling-a-ring/clips`). The installer is idempotent; re-run it to repair.

Then, on your phone or computer:

1. Open `http://<host>:8970`. First time on a device it shows two steps: download
   and install the box's certificate (so the browser may show notifications), then
   open the app at `https://<host>:8943`. Bookmark that, or add it to the Home Screen.
2. **Blink login** (once): Settings > Blink account, enter your Blink email and
   password, then the 2FA code Blink sends you. The Blink receiver starts by itself.
3. **Ring login** (Ring + Blink mode only, once): from your laptop run
   `ssh -L 55123:localhost:55123 <user>@<host>`, open `http://localhost:55123`,
   enter Ring email/password/2FA.
4. **Discover**: Settings > Discover devices. Your cameras appear on the Cameras page
   and in the Settings > Camera names list.
5. **Notifications**: Settings > Notifications > **Enable notifications on this
   device**, then **Send test notification**. Repeat on each phone.

The UI has no password until you turn one on in Settings > Access. Do that before
anyone else can reach the box.

## Notifications

The page sends them itself (Web Push through the browser's own push service), so they
arrive with the browser closed and no extra app is installed. Doorbell presses are
high-urgency with a long vibration; motion is normal. Tapping one opens the app.

- Browsers only allow this from an HTTPS page, which is why the installer creates a
  local certificate authority and the first visit asks you to trust it. Android shows
  a one-time "network may be monitored" notice afterwards; that is this certificate.
  iPhone and iPad also need the page added to the Home Screen first (Apple's rule).
- The certificate lists the box's hostname and addresses at install time. If the
  box's IP changes, run `sudo bash install.sh` again (it keeps the CA, so phones need
  nothing). For a name of your own: `EXTRA_SANS="DNS:nvr.example.com" sudo bash install.sh`.
- **In-browser**: the open page also chimes and shows a toast on every event.
- **Local speaker**: "Play chime on this box's speaker" in Settings, for a speaker
  wired to the host.
- **ntfy** (optional, for people who prefer the ntfy app): `WITH_NTFY=1 sudo bash install.sh`
  runs a local ntfy server on port 8099; Settings > Notifications has the fold-out.

## Remote access

Keep the box on your home network, or reach it over your own VPN. Never forward
ports 8970, 8943 or 8099 to the internet: the UI password is HTTP Basic and the
certificate is self-made.

## How Blink is read, and what has been tested

Blink has no local API. Everything goes through Blink's cloud, authenticated with your
own login, using [blinkpy](https://github.com/fronzbot/blinkpy).

Two kinds of Blink account exist and they behave differently:

| Account                         | Clips                                   | Real-time events                  |
|---------------------------------|-----------------------------------------|-----------------------------------|
| Sync Module with **USB local storage**, no subscription | pulled from the Sync Module's local-storage manifest (`local_poll_seconds`, default 60 s); the clip stays on the stick | none from Blink; the clip arriving IS the event, so chime/push fires on pull (motion and doorbell press cannot be told apart) |
| **Blink Subscription** (cloud clips) | on each push event the camera's latest cloud clip is fetched | FCM push, the same channel the Blink app uses: `button_press` = doorbell, `cv_motion` etc. = motion |

`poll_seconds` (default 1800 s) is how often the Blink session is refreshed and
saved; it is not a clip poll.

**Tested live**: the local-storage row, end to end (clips, timeline, chime, push,
motion toggle, snapshots, Blink Mini live view, online/offline state), on one account
with a Sync Module with USB storage, Blink Mini cameras and a Blink Video Doorbell,
EU region.

**Not yet tested live**: the subscription row. The FCM client registers with Blink's
push backend and the registration call returns success, but no account with a
subscription has been available to see a push arrive, and the clip fetch that follows
a push has never run. If you have a subscription, that is the most useful thing to
test: watch `journalctl -u bar-blink -f` while pressing the doorbell and look for the
event line and a `saved blink clip` line after it.

Blink's Firebase client config (sender id, app id, API key) in `blink_receiver.py` is
the Blink Android app's own, as shipped inside every app install. It identifies the
app to Google's push service; it is not an account credential.

## Testing checklist for a new box

1. `sudo systemctl status bar-blink bar-notifier bar-web mosquitto` all active.
2. Settings page: status row green, Blink "connected", Discover lists your devices.
3. Cameras page: a snapshot per camera, online dots, Motion toggle flips (check the
   Blink app agrees).
4. Walk in front of a camera: a clip appears on the Clips page within about a minute
   (local storage) and the browser chimes.
5. Live page: a Blink Mini streams; the doorbell says it does not support live view.
6. Settings > Notifications: subscribe on the phone as described above, press "Send
   test notification", get the push. Then trigger real motion and get that one too.
7. Logs when something is off: `journalctl -u bar-blink -n 100`,
   `journalctl -u bar-web -n 100`.

## Files

- `install.sh` — installer (services, deps, certificate, sudoers). `BLINK_ONLY=1` skips Ring.
- `webui.py` — Flask UI (timeline, cameras, live, SSE chime, discovery, settings,
  HTTPS landing page, push subscriptions, service worker).
- `blink_receiver.py` — Blink service: clip pull, FCM receiver, live-view relay, status.
- `notifier.py` — event bus → Web Push + chime + local sound (+ ntfy when enabled).
- `discovery.py` — Ring + Blink discovery (CLI and the in-app button).
- `recorder.py` — Ring clip recorder (motion + ding). Not installed in Blink-only mode.
- `blink_login_setup.py` — headless Blink 2FA login, if you cannot use the web UI.
- `config.sample.json` — config template; the live copy is `/opt/bling-a-ring/config.json`.

## Credits

Built on [blinkpy](https://github.com/fronzbot/blinkpy),
[firebase-messaging](https://github.com/sdb9696/firebase-messaging),
[ring-mqtt](https://github.com/tsightler/ring-mqtt), [go2rtc](https://github.com/AlexxIT/go2rtc),
[ntfy](https://ntfy.sh) and Mosquitto. Not affiliated with Amazon, Ring or Blink.
