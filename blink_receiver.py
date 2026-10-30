#!/usr/bin/env python3
"""Bling-a-Ring: Blink ingestion service (blinkpy 0.25.x + firebase-messaging 0.4.x).

Real-time events via FCM (the same channel the Blink app uses) + clip pull via blinkpy.
Login is done once from the web UI (Settings > Blink), which saves blink_creds.json;
this service runs headless off that token.

FCM values are the Blink Android app's (v50.1) own Firebase client config:
  sender 351117235468, app 1:351117235468:android:14fb3ef63df2a7c2,
  api key AIzaSyBcgwrpglzCZJPUNNeQQnpBbJAFlas_XAI, project blink-6c1ea.
Token is registered with Blink at POST {base}/clients/{client_id}/update as notification_key.
"""
import asyncio, json, os, time
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import paho.mqtt.client as mqtt
from blinkpy.blinkpy import Blink
from blinkpy.auth import Auth
from blinkpy.livestream import BlinkLiveStream
from blinkpy import api as blink_api
from firebase_messaging import FcmPushClient, FcmRegisterConfig

CFG = json.load(open(os.environ.get("BLING_CONFIG", "/opt/bling-a-ring/config.json")))
MQTT_HOST, MQTT_PORT = CFG["mqtt_host"], CFG["mqtt_port"]
CLIPS = CFG["clips_dir"]
BLINK = CFG.get("blink", {})
STATE_DIR = BLINK.get("state_dir", "/opt/bling-a-ring/blink-state")
CREDS_FILE = os.path.join(STATE_DIR, "blink_creds.json")
FCM_CREDS_FILE = os.path.join(STATE_DIR, "fcm_creds.json")
POLL = int(BLINK.get("poll_seconds", 1800))
LS_POLL = int(BLINK.get("local_poll_seconds", 60))   # local-storage manifest poll
LS_BACKFILL = int(BLINK.get("backfill", 15))          # clips to pull on first ever start
FFMPEG = CFG.get("ffmpeg", "ffmpeg")

FCM_PROJECT_ID = "blink-6c1ea"
FCM_APP_ID = "1:351117235468:android:14fb3ef63df2a7c2"
FCM_API_KEY = "AIzaSyBcgwrpglzCZJPUNNeQQnpBbJAFlas_XAI"
FCM_SENDER_ID = "351117235468"

EVENT_BUTTON_PRESS = "button_press"
MOTION_EVENTS = {"cv_motion", "cloud_motion", "cam2sm_motion", "motion"}

os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(CLIPS, exist_ok=True)

def log(*a): print(datetime.now().isoformat(), "blink", *a, flush=True)
def _safe(s): return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(s))

# --- MQTT (sync paho) ---
SAFE2NAME = {}          # safe camera key -> blink camera name
HOMESCREEN_STATUS = {}  # camera_id (str) -> "online"/"offline" (Blink homescreen)
LIVE_STREAMS = {}       # camsafe -> {"stream","port","task"} active immis sessions

def _on_cmd(c, u, msg):
    # bar/cmd/blink/<camsafe>/<action>  payload: on|off (motion/arm) or anything (snap)
    try:
        parts = msg.topic.split("/")
        if len(parts) < 5:
            return
        camsafe, action = parts[3], parts[4]
        value = msg.payload.decode(errors="replace").strip().lower()
        if LOOP is not None:
            asyncio.run_coroutine_threadsafe(do_cmd(camsafe, action, value), LOOP)
    except Exception as e:
        log("cmd error", e)

def _mqtt():
    try: c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except (AttributeError, TypeError): c = mqtt.Client()
    c.on_connect = lambda cl, u, f, rc, props=None: cl.subscribe("bar/cmd/blink/#")
    c.on_message = _on_cmd
    while True:
        try: c.connect(MQTT_HOST, MQTT_PORT, 60); break
        except Exception as e: log("mqtt retry", e); time.sleep(5)
    c.loop_start(); return c
MQTTC = _mqtt()

def publish_event(network, device, event_type, payload):
    topic = f"blink/{network}/{device}/{event_type}"
    MQTTC.publish(topic, json.dumps(payload), qos=1)
    log("event ->", topic, payload.get("camera", ""))

BLINK_REF = {"blink": None}
LOOP = None

async def blink_login():
    if not os.path.exists(CREDS_FILE):
        raise SystemExit("No Blink login yet. Log in from the web UI (Settings > Blink).")
    session = aiohttp.ClientSession()
    blink = Blink(session=session)
    blink.auth = Auth(json.load(open(CREDS_FILE)), no_prompt=True, session=session)
    await blink.start()           # refreshes token from saved creds; no 2FA needed
    await blink.save(CREDS_FILE)   # persist refreshed token
    return blink

async def register_fcm_with_blink(blink, fcm_token):
    base = getattr(blink.urls, "base_url", None)
    client_id = blink.auth.client_id
    if not base or not client_id:
        log(f"cannot register token yet (base={base} client_id={client_id})"); return False
    url = f"{base}/clients/{client_id}/update"
    body = {"app_version": "50.1", "os_version": "Android 13",
            "client_name": "bling-a-ring", "notification_key": fcm_token}
    try:
        async with blink.auth.session.post(url, data=json.dumps(body), headers=blink.auth.header) as r:
            txt = await r.text()
            log(f"token registered -> {url} [{r.status}] {txt[:120]}")
            return r.status < 400
    except Exception as e:
        log("token registration failed:", e); return False

async def pull_clip(name, event_type):
    blink = BLINK_REF["blink"]
    if blink is None: return
    try:
        await blink.refresh(force=True)
        cam = blink.cameras.get(name)
        if cam is None:
            log("pull_clip: camera not found:", name); return
        if not getattr(cam, "clip", None):
            log("pull_clip: no clip url yet for", name); return
        resp = await cam.get_video_clip()
        data = await resp.read()
        if not data or len(data) < 10000:
            log("pull_clip: empty clip for", name); return
        d = os.path.join(CLIPS, "blink_" + _safe(name)); os.makedirs(d, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out = os.path.join(d, ts + ".mp4")
        open(out, "wb").write(data)
        json.dump({"camera": name, "device_id": "blink_" + _safe(name), "start": ts,
                   "trigger": "ding" if event_type == EVENT_BUTTON_PRESS else "motion",
                   "source": "blink"}, open(out + ".json", "w"))
        try:
            import subprocess
            subprocess.run([FFMPEG, "-nostdin", "-ss", "1", "-i", out, "-vframes", "1",
                            "-y", out + ".jpg"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30)
        except Exception: pass
        log("saved blink clip", out)
    except Exception as e:
        log("pull_clip error:", e)

async def save_local_clip(blink, item, announce=True):
    """Download one local-storage (sync-module USB) motion clip into the timeline.
    prepare_download tells the sync module to upload the clip to Blink's servers,
    then download_video fetches it."""
    name = item.name
    camsafe = _safe(name)
    try:
        d = os.path.join(CLIPS, "blink_" + camsafe); os.makedirs(d, exist_ok=True)
        ts = item.created_at.astimezone().strftime("%Y-%m-%d_%H-%M-%S")
        out = os.path.join(d, ts + ".mp4")
        if os.path.exists(out):
            return False
        await item.prepare_download(blink)
        ok = await item.download_video(blink, out)
        if not ok or not os.path.exists(out) or os.path.getsize(out) < 2000:
            if os.path.exists(out):
                try: os.remove(out)
                except Exception: pass
            log("local clip download failed/empty", name, item.id); return False
        # show it at its real capture time (the timeline sorts clips by file mtime)
        try:
            epoch = item.created_at.timestamp(); os.utime(out, (epoch, epoch))
        except Exception: pass
        json.dump({"camera": name, "device_id": "blink_" + camsafe, "start": ts,
                   "trigger": "motion", "source": "blink", "storage": "local"},
                  open(out + ".json", "w"))
        try:
            import subprocess
            subprocess.run([FFMPEG, "-nostdin", "-ss", "1", "-i", out, "-vframes", "1",
                            "-y", out + ".jpg"], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30)
        except Exception: pass
        log("saved blink local clip", os.path.basename(out), name)
        if announce:
            # publish only the raw blink event; notifier.py owns bar/events (+chime,
            # +ntfy) and normalizes it, exactly like the FCM path does.
            payload = {"source": "blink", "kind": "motion", "event_type": "local_motion",
                       "device_type": "", "device_id": "blink_" + camsafe,
                       "camera": name, "network": "local",
                       "created_at": item.created_at.isoformat()}
            MQTTC.publish(f"blink/local/{camsafe}/motion", json.dumps(payload), qos=1)
        return True
    except Exception as e:
        log("save_local_clip error", name, e); return False

async def local_storage_poll():
    """Primary Blink event path: poll the sync module's local (USB) storage manifest
    for new motion clips and pull them into the timeline. This setup has local
    storage and no cloud subscription, so the cloud media list and FCM pushes stay
    empty; the recorded clips live on the sync module's USB."""
    seen_file = os.path.join(STATE_DIR, "ls_seen.json")
    seen = set()
    first_ever = not os.path.exists(seen_file)
    if not first_ever:
        try: seen = set(json.load(open(seen_file)))
        except Exception: pass
    while True:
        try:
            blink = BLINK_REF["blink"]
            if blink:
                for sync in list(blink.sync.values()):
                    if not getattr(sync, "local_storage", False):
                        continue
                    try:
                        await sync.update_local_storage_manifest()
                    except Exception as e:
                        log("manifest refresh error", e); continue
                    manifest = list(sync._local_storage.get("manifest") or [])
                    manifest.sort(key=lambda it: it.created_at)
                    new_items = [it for it in manifest if it.id not in seen]
                    if first_ever:
                        # don't pull the whole backlog on first start; take the most
                        # recent few so the page populates, mark the rest as seen.
                        for it in new_items:
                            seen.add(it.id)
                        for it in (new_items[-LS_BACKFILL:] if LS_BACKFILL > 0 else []):
                            await save_local_clip(blink, it, announce=False)
                    else:
                        for it in new_items:
                            seen.add(it.id)
                            await save_local_clip(blink, it, announce=True)
            try: json.dump(sorted(seen)[-5000:], open(seen_file, "w"))
            except Exception: pass
            first_ever = False
        except Exception as e:
            log("local storage poll error", e)
        await asyncio.sleep(LS_POLL)

async def refresh_homescreen_status():
    """Pull the Blink homescreen and cache each device's online/offline status.
    blinkpy does not surface this in camera.attributes, but it is the field the
    Blink app uses to show a camera as reachable vs offline (and it is why an
    offline camera silently ignores a motion-enable command)."""
    blink = BLINK_REF["blink"]
    if blink is None: return
    try:
        from blinkpy import api
        url = f"{blink.urls.base_url}/api/v3/accounts/{blink.account_id}/homescreen"
        data = await api.http_get(blink, url)   # returns a parsed dict
        m = {}
        for grp in ("owls", "doorbells", "cameras"):
            for it in (data.get(grp) or []):
                if "id" in it:
                    m[str(it["id"])] = it.get("status")
        HOMESCREEN_STATUS.clear(); HOMESCREEN_STATUS.update(m)
    except Exception as e:
        log("homescreen status error", e)

async def publish_one_status(name):
    blink = BLINK_REF["blink"]
    if blink is None: return
    cam = blink.cameras.get(name)
    if cam is None: return
    a = cam.attributes
    safe = _safe(name)
    status = HOMESCREEN_STATUS.get(str(getattr(cam, "camera_id", "")))
    st = {"name": name, "source": "blink",
          "online": status, "live": (a.get("type") == "owl" and status == "online"),
          "motion_enabled": a.get("motion_enabled"), "battery": a.get("battery"),
          "temperature": a.get("temperature"), "wifi": a.get("wifi_strength"),
          "motion_detected": a.get("motion_detected"), "type": a.get("type")}
    MQTTC.publish(f"bar/status/blink/{safe}", json.dumps(st), qos=1, retain=True)
    try:
        if a.get("thumbnail"):
            resp = await cam.get_thumbnail()
            data = await resp.read()
            if data and len(data) > 500:
                d = os.path.join(CLIPS, "blink_" + safe); os.makedirs(d, exist_ok=True)
                open(os.path.join(d, "snapshot.jpg"), "wb").write(data)
    except Exception as e:
        log("thumb error", e)

async def do_cmd(camsafe, action, value):
    blink = BLINK_REF["blink"]
    if blink is None: return
    name = SAFE2NAME.get(camsafe)
    if not name:
        log("cmd: unknown camera", camsafe); return
    cam = blink.cameras.get(name)
    on = value in ("on", "true", "1", "arm", "enable")
    try:
        if action == "motion":
            r = await cam.async_arm(on)   # per-camera motion enable/disable (set_motion_detect is deprecated and just forwards here)
            log(f"motion {'on' if on else 'off'} -> {name} ({str(r)[:80]})")
            # Blink queues this as an async command ('config_set', state 'new') and takes
            # ~3-6 s to apply it; blinkpy's wait_for_command polls that command until it
            # completes. async_arm also never touches cam.attributes, so without the
            # forced refresh the status we publish would still carry the OLD value for
            # up to a minute and the Cameras page would show the wrong state.
            if isinstance(r, dict) and r.get("id"):
                try:
                    done = await asyncio.wait_for(blink_api.wait_for_command(blink, r), timeout=20)
                    log(f"motion command {'completed' if done else 'not confirmed'} for {name}")
                except asyncio.TimeoutError:
                    log(f"motion command still pending after 20s for {name}")
            await blink.refresh(force=True)
        elif action == "snap":
            await cam.snap_picture(); await asyncio.sleep(3); await blink.refresh(force=True)
            log(f"snapshot -> {name}")
        elif action == "arm":
            sync = blink.sync.get(cam.sync.name)
            if sync: await sync.async_arm(on); log(f"system {'arm' if on else 'disarm'} -> {cam.sync.name}")
        await publish_one_status(name)
    except Exception as e:
        log("do_cmd error", e)

class FixedLiveStream(BlinkLiveStream):
    """blinkpy's recv() reads the immis header/payload with stream.read(n), which on
    TCP can return a short read; blinkpy then logs 'Insufficient data' and breaks,
    tearing the stream down after a second or two. readexactly() waits for the full
    frame, so the session lasts its real length."""
    async def recv(self):
        try:
            while not self.target_reader.at_eof():
                try:
                    header = await self.target_reader.readexactly(9)
                except asyncio.IncompleteReadError:
                    break
                msgtype = header[0]
                payload_length = int.from_bytes(header[5:9], byteorder="big")
                if payload_length <= 0:
                    continue
                try:
                    data = await self.target_reader.readexactly(payload_length)
                except asyncio.IncompleteReadError:
                    break
                if msgtype != 0x00 or data[:1] != b"\x47":
                    continue   # not a video TS packet
                for writer in list(self.clients):
                    if not writer.is_closing():
                        try:
                            writer.write(data); await writer.drain()
                        except Exception:
                            pass
                await asyncio.sleep(0)
        except Exception:
            pass
        finally:
            try: self.target_writer.close()
            except Exception: pass

async def start_livestream(cam):
    """init_livestream() equivalent that returns our FixedLiveStream (immis only)."""
    resp = await blink_api.request_camera_liveview(
        cam.sync.blink, cam.sync.network_id, cam.camera_id, camera_type=cam.camera_type)
    server = resp.get("server", "")
    if not server.startswith("immis://"):
        raise NotImplementedError(server or "no immis server")
    return FixedLiveStream(cam, resp)

async def handle_live(request):
    """Localhost control endpoint: start (or reuse) a Blink immis live session for
    one camera and return the local TCP port that blinkpy re-serves the MPEG-TS on.
    The web UI then restreams that port to the browser. Blink live view is only
    supported on the owl (Mini) cameras and only while they are online; the immis
    session auto-stops when the last client disconnects."""
    camsafe = request.match_info.get("camsafe", "")
    blink = BLINK_REF["blink"]
    if blink is None:
        return web.json_response({"error": "blink not ready"}, status=503)
    name = SAFE2NAME.get(camsafe)
    cam = blink.cameras.get(name) if name else None
    if cam is None:
        return web.json_response({"error": "unknown camera"}, status=404)
    ex = LIVE_STREAMS.get(camsafe)
    if ex and ex["stream"].is_serving:
        return web.json_response({"port": ex["port"], "reused": True})
    try:
        stream = await start_livestream(cam)   # raises NotImplementedError unless immis://
    except NotImplementedError as e:
        return web.json_response({"error": f"live view not supported for this camera: {e}"}, status=400)
    except Exception as e:
        log("live init error", camsafe, e)
        return web.json_response({"error": str(e)[:160]}, status=502)
    try:
        server = await stream.start("127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        task = asyncio.create_task(stream.feed())
        LIVE_STREAMS[camsafe] = {"stream": stream, "port": port, "task": task}
        def _cleanup(_t, cs=camsafe):
            LIVE_STREAMS.pop(cs, None); log("live session ended", cs)
        task.add_done_callback(_cleanup)
        log("live session started", camsafe, "port", port)
        return web.json_response({"port": port})
    except Exception as e:
        log("live start error", camsafe, e)
        return web.json_response({"error": str(e)[:160]}, status=502)

async def status_publisher():
    while True:
        try:
            blink = BLINK_REF["blink"]
            if blink:
                await blink.refresh()
                await refresh_homescreen_status()
                for name in list(blink.cameras.keys()):
                    await publish_one_status(name)
        except Exception as e:
            log("status pub error", e)
        await asyncio.sleep(60)

def on_fcm_notification(data, persistent_id=None, context=None):
    """firebase-messaging callback: data is the decrypted message dict."""
    try:
        d = data.get("data") if isinstance(data, dict) and "data" in data else data
        if not isinstance(d, dict): d = {}
        event_type = d.get("event_type") or d.get("type") or "unknown"
        device_type = d.get("device_type", "")
        camera = d.get("camera_name") or d.get("camera") or d.get("device_id") or "Blink"
        network = d.get("network") or d.get("network_id") or "net"
        if event_type == EVENT_BUTTON_PRESS:
            kind = "ding"
        elif event_type in MOTION_EVENTS:
            kind = "motion"
        else:
            kind = "other"
        payload = {"source": "blink", "kind": kind, "event_type": event_type,
                   "device_type": device_type, "device_id": str(d.get("device_id", camera)),
                   "camera": str(camera), "network": str(network),
                   "created_at": d.get("created_at") or datetime.now(timezone.utc).isoformat(),
                   "raw": d}
        publish_event(network, _safe(camera), event_type, payload)
        if kind in ("ding", "motion") and LOOP is not None:
            asyncio.run_coroutine_threadsafe(pull_clip(str(camera), event_type), LOOP)
    except Exception as e:
        log("on_fcm_notification error:", e)

async def run():
    blink = await blink_login()
    BLINK_REF["blink"] = blink
    SAFE2NAME.clear()
    for n in blink.cameras.keys():
        SAFE2NAME[_safe(n)] = n
    log("blink logged in; cameras:", list(blink.cameras.keys()))
    asyncio.create_task(status_publisher())
    asyncio.create_task(local_storage_poll())

    fcm_creds = json.load(open(FCM_CREDS_FILE)) if os.path.exists(FCM_CREDS_FILE) else None
    def on_creds_updated(creds):
        json.dump(creds, open(FCM_CREDS_FILE, "w")); log("FCM creds persisted")

    cfg = FcmRegisterConfig(project_id=FCM_PROJECT_ID, app_id=FCM_APP_ID,
                            api_key=FCM_API_KEY, messaging_sender_id=FCM_SENDER_ID)
    client = FcmPushClient(on_fcm_notification, cfg, fcm_creds, on_creds_updated)
    token = await client.checkin_or_register()
    log("FCM token:", (token or "")[:24], "...")
    await register_fcm_with_blink(blink, token)
    await client.start()
    log("FCM receiver started")

    # localhost control server for live view (immis -> local TCP the web UI restreams)
    live_port = int(BLINK.get("live_port", 8971))
    webapp = web.Application()
    webapp.router.add_get("/live/{camsafe}", handle_live)
    runner = web.AppRunner(webapp)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", live_port).start()
    log(f"live control server on 127.0.0.1:{live_port}")

    while True:
        await asyncio.sleep(POLL)
        try:
            await blink.refresh(); await blink.save(CREDS_FILE)
        except Exception as e:
            log("periodic refresh error:", e)

def main():
    global LOOP
    LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(LOOP)
    LOOP.run_until_complete(run())
    LOOP.run_forever()

if __name__ == "__main__":
    main()
