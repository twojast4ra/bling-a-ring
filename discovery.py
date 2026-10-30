#!/usr/bin/env python3
"""Shared device discovery for Bling-a-Ring.

Ring: listen to ring-mqtt for camera topics + Home Assistant discovery (names),
      then (re)write the go2rtc restream config.
Blink: list cameras/doorbells via blinkpy (needs the saved Blink login).

Used by the Settings > Discover devices button in the web UI (run as root via a
scoped sudoers rule) and runnable directly from the CLI.
"""
import json, os, subprocess, time, asyncio
import paho.mqtt.client as mqtt

APP = "/opt/bling-a-ring"
CFG_PATH = os.environ.get("BLING_CONFIG", APP + "/config.json")
SVC_USER = os.environ.get("SVC_USER", "blingaring")
STATUS = APP + "/discover-status.json"
SERVICES = ("bar-go2rtc-restream", "bar-recorder", "bar-blink", "bar-notifier", "bar-web")
RING_SERVICES = ("bar-go2rtc-restream", "bar-recorder")   # not installed in Blink-only mode

def _load(): return json.load(open(CFG_PATH))
def _save(cfg):
    tmp = CFG_PATH + ".tmp"
    json.dump(cfg, open(tmp, "w"), indent=2)
    os.replace(tmp, CFG_PATH)
def _status(**kw):
    try: json.dump({"ts": time.time(), **kw}, open(STATUS, "w"))
    except Exception: pass

def _safe(s):
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(s))

def discover_ring(cfg):
    devs = set(); names = {}
    def on_c(c, u, f, rc, props=None):
        c.subscribe("ring/#"); c.subscribe("homeassistant/#")
    def on_m(c, u, msg):
        p = msg.topic.split("/")
        if len(p) >= 4 and p[0] == "ring" and p[2] == "camera":
            devs.add(p[3])
        if p[0] == "homeassistant" and p[-1] == "config" and msg.payload:
            try:
                d = json.loads(msg.payload); dev = d.get("device", {})
                ids = dev.get("ids") or dev.get("identifiers"); nm = dev.get("name")
                if ids and nm:
                    names[ids[0] if isinstance(ids, list) else ids] = nm
            except Exception: pass
    try: c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    except (AttributeError, TypeError): c = mqtt.Client()
    c.on_connect = on_c; c.on_message = on_m
    c.connect(cfg["mqtt_host"], cfg["mqtt_port"], 30)
    c.loop_start(); time.sleep(12); c.loop_stop()
    cams = {dev: names.get(dev, dev) for dev in sorted(devs)}
    streams = "\n".join(f"  {dev}: rtsp://127.0.0.1:8554/{dev}_live" for dev in cams)
    yaml = ('log:\n  level: info\napi:\n  listen: "127.0.0.1:1984"\n'
            'rtsp:\n  listen: "127.0.0.1:8556"\nwebrtc:\n  listen: ""\n'
            f'streams:\n{streams}\n')
    open(os.path.join(APP, "go2rtc-restream.yaml"), "w").write(yaml)
    return cams

def discover_blink(cfg):
    bl = cfg.get("blink", {})
    if not bl.get("enabled"):
        return {}
    creds = os.path.join(bl.get("state_dir", APP + "/blink-state"), "blink_creds.json")
    if not os.path.exists(creds):
        _status(blink_note="no Blink login yet"); return {}
    async def _go():
        from blinkpy.blinkpy import Blink
        from blinkpy.auth import Auth
        import aiohttp
        s = aiohttp.ClientSession(); b = Blink(session=s)
        b.auth = Auth(json.load(open(creds)), no_prompt=True, session=s)
        await b.start(); await b.refresh()
        cams = {"blink_" + _safe(name): name for name in b.cameras.keys()}
        await s.close(); return cams
    try:
        return asyncio.run(_go())
    except Exception as e:
        _status(blink_error=str(e)); return {}

def ring_enabled(cfg):
    return bool(cfg.get("ring", {}).get("enabled", True))

def run_discovery(restart=True):
    cfg = _load()
    _status(state="running")
    ring = discover_ring(cfg) if ring_enabled(cfg) else {}
    blink = discover_blink(cfg)
    cams = dict(cfg.get("cameras", {})); cams.update(ring); cams.update(blink)
    cfg["cameras"] = cams
    if ring and not cfg.get("live_stream"):
        cfg["live_stream"] = next(iter(ring))
    _save(cfg)
    if restart:
        subprocess.run(["chown", "-R", f"{SVC_USER}:{SVC_USER}", APP])
        for s in SERVICES:
            if not ring_enabled(cfg) and s in RING_SERVICES:
                continue
            subprocess.run(["systemctl", "restart", s])
    _status(state="done", ring=list(ring.values()), blink=list(blink.values()))
    return {"ring": ring, "blink": blink}

if __name__ == "__main__":
    r = run_discovery()
    print("Ring cameras:", r["ring"])
    print("Blink devices:", r["blink"])
