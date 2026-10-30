#!/usr/bin/env python3
"""Bling-a-Ring notifier: the single event bus for Ring + Blink.

Subscribes to the raw Ring (ring-mqtt) and Blink (blink_receiver) MQTT topics,
normalizes every doorbell/motion event into one shape, and:
  - republishes it to  bar/events  (the web UI streams this to the browser as SSE,
    which drives the in-browser chime and live alert toasts),
  - sends a phone push via ntfy (doorbell = high priority + chime sound; motion = default),
  - optionally plays a chime on this host's own speaker (for a speaker wired to the box).

One normalized event:
  {"source":"ring|blink","kind":"ding|motion","camera":"Front Door",
   "device":"<id>","ts":"<iso>","detection":"person|vehicle|motion|"}
"""
import json, os, time, subprocess, threading, urllib.request
from datetime import datetime
import paho.mqtt.client as mqtt

CFG = json.load(open(os.environ.get("BLING_CONFIG", "/opt/bling-a-ring/config.json")))
MQTT_HOST, MQTT_PORT = CFG["mqtt_host"], CFG["mqtt_port"]
NTFY = CFG.get("ntfy", {})
CHIME = CFG.get("chime", {})
NAMES = CFG.get("cameras", {})
EVENTS_TOPIC = "bar/events"

def log(*a): print(datetime.now().isoformat(), "notifier", *a, flush=True)
def name_of(dev): return NAMES.get(dev, dev)

# debounce: one notification per (device,kind) within this window
_last = {}
_DEBOUNCE = 6.0

def _should_fire(key):
    now = time.time()
    if now - _last.get(key, 0) < _DEBOUNCE:
        return False
    _last[key] = now
    return True

def send_ntfy(kind, camera, source, detection=""):
    if not NTFY.get("enabled"):
        return
    server = NTFY.get("server", "http://127.0.0.1:8099").rstrip("/")
    topic = NTFY.get("topic", "bling-a-ring")
    url = f"{server}/{topic}"
    # HTTP headers are ASCII; the bell emoji comes from the "bell" tag, which the ntfy
    # app renders in front of the title. Camera names are user text, so RFC 2047-encode
    # the title if anything in it is not ASCII (ntfy decodes that form).
    if kind == "ding":
        title = f"Doorbell - {camera}"
        body = "Someone is at the door"
        priority = "max"
        tags = "bell"
    else:
        det = f" ({detection})" if detection else ""
        title = f"Motion - {camera}"
        body = f"Motion detected{det}"
        priority = "default"
        tags = "movie_camera"
    try:
        title.encode("ascii")
    except UnicodeEncodeError:
        import base64
        title = "=?UTF-8?B?" + base64.b64encode(title.encode("utf-8")).decode("ascii") + "?="
    headers = {"Title": title, "Priority": priority, "Tags": tags}
    if NTFY.get("token"):
        headers["Authorization"] = "Bearer " + NTFY["token"]
    try:
        req = urllib.request.Request(url, data=body.encode("utf-8"), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=8) as r:
            log(f"ntfy sent ({r.status}): {title}")
    except Exception as e:
        log(f"ntfy error: {e}  (server {server}, topic {topic})")

PUSH = CFG.get("push", {})
PUSH_DIR = "/opt/bling-a-ring/push"
SUBS_PATH = PUSH_DIR + "/subscriptions.json"
VAPID_KEY = PUSH_DIR + "/vapid_private.pem"

def send_webpush(kind, camera, source, detection=""):
    """Browser notifications (Web Push) to every device that pressed 'Enable
    notifications' in Settings. Goes through the browser vendor's push service, so
    it arrives with the page closed. Runs in a thread: one HTTPS call per device."""
    if not PUSH.get("enabled", True):
        return
    try:
        subs = json.load(open(SUBS_PATH))
    except Exception:
        return
    if not subs or not os.path.isfile(VAPID_KEY):
        return
    try:
        from pywebpush import webpush, WebPushException
    except ImportError:
        log("pywebpush not installed; browser push off (re-run install.sh)")
        return
    if kind == "ding":
        title, body = f"Doorbell - {camera}", "Someone is at the door"
    else:
        det = f" ({detection})" if detection else ""
        title, body = f"Motion - {camera}", f"Motion detected{det}"
    payload = json.dumps({"title": title, "body": body, "kind": kind, "source": source,
                          "tag": f"{kind}-{camera}", "url": "/",
                          "ts": datetime.now().isoformat(timespec="seconds")})
    claims = {"sub": PUSH.get("contact", "mailto:admin@example.com")}
    def _send():
        gone, sent = [], 0
        for s in subs:
            try:
                webpush(subscription_info={"endpoint": s["endpoint"], "keys": s["keys"]},
                        data=payload, vapid_private_key=VAPID_KEY, vapid_claims=dict(claims),
                        ttl=120, timeout=10,
                        headers={"Urgency": "high" if kind == "ding" else "normal"})
                sent += 1
            except WebPushException as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code in (404, 410):
                    gone.append(s["endpoint"])     # browser dropped the subscription
                else:
                    log(f"webpush error {code}: {str(e)[:120]}")
            except Exception as e:
                log(f"webpush error: {str(e)[:120]}")
        if gone:
            try:
                cur = [c for c in json.load(open(SUBS_PATH)) if c.get("endpoint") not in gone]
                tmp = SUBS_PATH + ".tmp"
                json.dump(cur, open(tmp, "w"), indent=1); os.replace(tmp, SUBS_PATH)
                log(f"removed {len(gone)} expired push subscription(s)")
            except Exception as e:
                log("subs cleanup error", e)
        log(f"browser push: {title} -> {sent} device(s)")
    threading.Thread(target=_send, daemon=True).start()

def play_local_chime(kind):
    if kind != "ding" or not CHIME.get("local_enabled"):
        return
    sound = CHIME.get("doorbell_sound", "/opt/bling-a-ring/sounds/doorbell.wav")
    player = CHIME.get("player", "")  # autodetect if empty
    if not os.path.exists(sound):
        return
    def _play():
        cmds = []
        if player:
            cmds = [[player, sound]]
        else:
            cmds = [["paplay", sound], ["aplay", sound],
                    ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", sound]]
        for cmd in cmds:
            try:
                subprocess.run(cmd, timeout=20,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                return
            except FileNotFoundError:
                continue
            except Exception:
                return
    threading.Thread(target=_play, daemon=True).start()

def emit(ev, client):
    key = f"{ev['device']}/{ev['kind']}"
    if not _should_fire(key):
        return
    client.publish(EVENTS_TOPIC, json.dumps(ev), qos=1)
    log(f"event: {ev['source']} {ev['kind']} {ev['camera']}")
    send_webpush(ev["kind"], ev["camera"], ev["source"], ev.get("detection", ""))
    send_ntfy(ev["kind"], ev["camera"], ev["source"], ev.get("detection", ""))
    play_local_chime(ev["kind"])

def on_connect(c, u, f, rc, props=None):
    log("MQTT connected rc", rc)
    c.subscribe("ring/+/camera/+/motion/state")
    c.subscribe("ring/+/camera/+/ding/state")
    c.subscribe("blink/#")
    c.subscribe("bar/cmd/notify_test")

def on_message(c, u, msg):
    topic = msg.topic
    try:
        if topic == "bar/cmd/notify_test":
            # Settings > "Send test notification": goes down the exact same path as a
            # real doorbell press (browser chime, ntfy push, local chime), no debounce.
            ev = {"source": "test", "kind": "ding", "camera": "Test",
                  "device": "test", "ts": datetime.now().isoformat(), "detection": ""}
            c.publish(EVENTS_TOPIC, json.dumps(ev), qos=1)
            log("test notification")
            send_webpush("ding", "Test notification", "test")
            send_ntfy("ding", "Test notification", "test")
            play_local_chime("ding")
            return
        if topic.startswith("ring/"):
            p = topic.split("/")
            if len(p) < 6:
                return
            dev, kind = p[3], p[4]      # kind = motion | ding
            payload = msg.payload.decode(errors="replace").strip().upper()
            if payload != "ON":
                return
            ev = {"source": "ring", "kind": "ding" if kind == "ding" else "motion",
                  "camera": name_of(dev), "device": dev,
                  "ts": datetime.now().isoformat(), "detection": ""}
            emit(ev, c)
        elif topic.startswith("blink/"):
            try:
                d = json.loads(msg.payload.decode(errors="replace"))
            except Exception:
                return
            k = d.get("kind")
            if k not in ("ding", "motion"):
                return
            ev = {"source": "blink", "kind": k,
                  "camera": d.get("camera", d.get("device_id", "Blink")),
                  "device": d.get("device_id", "blink"),
                  "ts": d.get("created_at", datetime.now().isoformat()),
                  "detection": (d.get("raw", {}) or {}).get("detection_type", "")}
            emit(ev, c)
    except Exception as e:
        log("on_message error", e)

def main():
    try:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except (AttributeError, TypeError):
        c = mqtt.Client()
    c.on_connect = on_connect
    c.on_message = on_message
    while True:
        try:
            c.connect(MQTT_HOST, MQTT_PORT, 60); break
        except Exception as e:
            log("mqtt connect retry:", e); time.sleep(5)
    c.loop_forever()

if __name__ == "__main__":
    main()
