#!/usr/bin/env python3
"""Bling-a-Ring web UI: one timeline for Ring + Blink clips, live view, a live
event feed (doorbell chime + motion toasts over SSE), in-app device discovery,
and settings. Non-redundant front door: live view is proxied so go2rtc stays
localhost-only, and the same MQTT event bus that drives phone push also drives
the in-browser chime here.
"""
import json, os, functools, shutil, subprocess, urllib.request, urllib.error, urllib.parse, threading, queue, time
from datetime import datetime
from flask import Flask, Response, request, send_file, abort, redirect, url_for
from markupsafe import escape

APP_DIR = "/opt/bling-a-ring"
CFG_PATH = os.environ.get("BLING_CONFIG", APP_DIR + "/config.json")
STATUS_PATH = APP_DIR + "/discover-status.json"
VENV_PY = APP_DIR + "/venv/bin/python"
app = Flask(__name__)

def load_cfg():
    with open(CFG_PATH) as f:
        return json.load(f)

def save_cfg(cfg):
    tmp = CFG_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, CFG_PATH)

def _safe(part):
    return part and "/" not in part and "\\" not in part and ".." not in part

def ring_on(cfg):
    """False on a Blink-only install (install.sh BLINK_ONLY=1 sets ring.enabled=false):
    no ring-mqtt, go2rtc or recorder exist, so the UI leaves Ring out everywhere."""
    return bool(cfg.get("ring", {}).get("enabled", True))

def authed(f):
    @functools.wraps(f)
    def w(*a, **k):
        cfg = load_cfg()
        if cfg.get("auth_enabled"):
            au = request.authorization
            if not au or au.username != cfg.get("ui_user", "nvr") or au.password != cfg.get("ui_password", ""):
                return Response("Auth required", 401,
                                {"WWW-Authenticate": 'Basic realm="Bling-a-Ring"'})
        return f(*a, **k)
    return w

def human_size(n):
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def read_sidecar(fp):
    try:
        return json.load(open(fp + ".json"))
    except Exception:
        return {}

def host_ips():
    """This box's IPv4 addresses (LAN, Tailscale), for the 'subscribe your phone to
    http://<ip>:8099' hint. The notifier posts to 127.0.0.1, the phone cannot."""
    ips = []
    try:
        out = subprocess.run(["ip", "-4", "-o", "addr", "show", "scope", "global"],
                             capture_output=True, text=True, timeout=5).stdout
        for line in out.splitlines():
            parts = line.split()
            if "inet" in parts:
                ips.append(parts[parts.index("inet") + 1].split("/")[0])
    except Exception:
        pass
    return ips

_MOUNT_FS = {"ext2", "ext3", "ext4", "xfs", "btrfs", "f2fs", "zfs", "vfat", "exfat",
             "ntfs", "ntfs3", "fuseblk", "nfs", "nfs4", "cifs", "smb3"}

def mounted_drives():
    """Real filesystems on this box (from /proc/mounts) with free space, for the
    storage-location picker. The clips folder lives on the box, not on the device
    the browser runs on, so a file dialog cannot pick it; this list can."""
    out = []
    try:
        for line in open("/proc/mounts"):
            p = line.split()
            if len(p) < 3 or p[2] not in _MOUNT_FS:
                continue
            mp = p[1].replace("\\040", " ")
            if mp.startswith(("/boot", "/snap/", "/var/lib/docker", "/proc", "/sys")):
                continue
            try:
                du = shutil.disk_usage(mp)
            except OSError:
                continue
            out.append({"mount": mp, "fstype": p[2], "free": du.free, "total": du.total})
    except Exception:
        pass
    out.sort(key=lambda d: (d["mount"] != "/", d["mount"]))
    return out

def suggested_clips_dir(mount):
    return "/var/lib/bling-a-ring/clips" if mount == "/" else mount.rstrip("/") + "/bling-a-ring/clips"

def list_clips(cfg):
    clips_dir = cfg["clips_dir"]; names = cfg.get("cameras", {})
    out = []
    if not os.path.isdir(clips_dir):
        return out
    for dev in os.listdir(clips_dir):
        ddir = os.path.join(clips_dir, dev)
        if not os.path.isdir(ddir):
            continue
        cam = names.get(dev, dev)
        for fn in os.listdir(ddir):
            if not fn.endswith(".mp4"):
                continue
            fp = os.path.join(ddir, fn)
            try:
                st = os.stat(fp)
            except OSError:
                continue
            if st.st_size < 10000:
                continue
            side = read_sidecar(fp)
            source = side.get("source", "blink" if dev.startswith("blink_") else "ring")
            if source != "blink" and not ring_on(cfg):
                continue    # Blink-only install: nothing but Blink in the timeline
            out.append({"dev": dev, "cam": side.get("camera", cam), "name": fn,
                        "size": st.st_size, "mtime": st.st_mtime,
                        "thumb": os.path.exists(fp + ".jpg"),
                        "source": source,
                        "trigger": side.get("trigger", "motion")})
    out.sort(key=lambda x: x["mtime"], reverse=True)
    return out

# ---------------------------------------------------------------------------
# live event feed: subscribe to bar/events on MQTT, fan out to SSE clients
# ---------------------------------------------------------------------------
_subs = set()
_subs_lock = threading.Lock()
_ring_cam = {}      # ring devid -> {"prefix","motion","wifi"}
_ring_snap = {}     # ring devid -> jpeg bytes
_blink_status = {}  # blink camsafe -> status dict
_ring_mirror = {}   # ring devid -> last published retained status json (dedupe)
_mqttc = None

def _publish(topic, payload, retain=False):
    try:
        if _mqttc is not None:
            _mqttc.publish(topic, payload, qos=1, retain=retain)
            return True
    except Exception:
        pass
    return False

def _mirror_ring(devid, cam):
    """Persist last-known Ring camera state (motion-toggle state + command prefix
    + wifi) as a retained topic. ring-mqtt publishes these non-retained and only
    on change/startup/motion, so without this mirror a freshly restarted web UI
    shows 'unknown' and can't toggle until the next Ring event. Mirrors the
    retained bar/status/blink/<cam> pattern blink_receiver already uses."""
    payload = json.dumps({"prefix": cam.get("prefix"), "motion": cam.get("motion"),
                          "wifi": cam.get("wifi")})
    if _ring_mirror.get(devid) != payload:
        _ring_mirror[devid] = payload
        _publish(f"bar/status/ring/{devid}", payload, retain=True)

def _event_pump():
    global _mqttc
    import paho.mqtt.client as mqtt
    cfg = load_cfg()
    def on_c(c, u, f, rc, props=None):
        c.subscribe("bar/events")
        c.subscribe("bar/status/blink/#")
        if not ring_on(cfg):
            return
        c.subscribe("bar/status/ring/#")
        c.subscribe("ring/+/camera/+/motion_detection/state")
        c.subscribe("ring/+/camera/+/motion/attributes")
        c.subscribe("ring/+/camera/+/snapshot/image")
        c.subscribe("ring/+/camera/+/info/state")
    def on_m(c, u, msg):
        t = msg.topic
        if t == "bar/events":
            data = msg.payload.decode(errors="replace")
            with _subs_lock:
                dead = []
                for q in _subs:
                    try: q.put_nowait(data)
                    except Exception: dead.append(q)
                for q in dead:
                    _subs.discard(q)
            return
        if t.startswith("bar/status/blink/"):
            try: _blink_status[t.split("/")[-1]] = json.loads(msg.payload.decode(errors="replace"))
            except Exception: pass
            return
        if t.startswith("bar/status/ring/"):
            # our own retained mirror: seed gaps on (re)connect, never override a
            # live value and never re-publish (so there is no feedback loop).
            try:
                devid = t.split("/")[-1]
                st = json.loads(msg.payload.decode(errors="replace"))
                cam = _ring_cam.setdefault(devid, {})
                for k in ("prefix", "motion", "wifi"):
                    if st.get(k) is not None and k not in cam:
                        cam[k] = st[k]
            except Exception: pass
            return
        if t.startswith("ring/"):
            p = t.split("/")
            if len(p) >= 5 and p[2] == "camera":
                loc, devid = p[1], p[3]
                ent = "/".join(p[4:])
                cam = _ring_cam.setdefault(devid, {})
                cam["prefix"] = f"ring/{loc}/camera/{devid}"
                if ent == "motion_detection/state":
                    cam["motion"] = msg.payload.decode(errors="replace").strip()
                elif ent == "motion/attributes":
                    # motion/attributes carries motionDetectionEnabled; use it to
                    # seed the toggle state when we don't have one yet.
                    try:
                        mde = json.loads(msg.payload.decode(errors="replace")).get("motionDetectionEnabled")
                        if mde is not None and "motion" not in cam:
                            cam["motion"] = "ON" if mde else "OFF"
                    except Exception: pass
                elif ent == "snapshot/image":
                    _ring_snap[devid] = msg.payload
                elif ent == "info/state":
                    try: cam["wifi"] = json.loads(msg.payload.decode(errors="replace")).get("wirelessSignal")
                    except Exception: pass
                _mirror_ring(devid, cam)
            return
    while True:
        try:
            try: c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
            except (AttributeError, TypeError): c = mqtt.Client()
            c.on_connect = on_c; c.on_message = on_m
            c.connect(cfg["mqtt_host"], cfg["mqtt_port"], 60)
            _mqttc = c
            c.loop_forever()
        except Exception:
            time.sleep(5)

threading.Thread(target=_event_pump, daemon=True).start()

# ---------------------------------------------------------------------------
# Blink login (in-app). blinkpy keeps the OAuth 2FA/PKCE state in memory on the
# auth object, so the password step and the code step must share one live auth.
# A persistent asyncio loop in this process holds it between the two requests.
# ---------------------------------------------------------------------------
import asyncio
_blink_loop = asyncio.new_event_loop()
threading.Thread(target=_blink_loop.run_forever, daemon=True).start()
_blink_pending = {}

def _blink_creds_path(cfg):
    sd = cfg.get("blink", {}).get("state_dir", APP_DIR + "/blink-state")
    return os.path.join(sd, "blink_creds.json")

def _run_blink(coro, timeout=90):
    return asyncio.run_coroutine_threadsafe(coro, _blink_loop).result(timeout=timeout)

async def _blink_start_login(email, password):
    import aiohttp
    from blinkpy.blinkpy import Blink
    from blinkpy.auth import Auth
    session = aiohttp.ClientSession()
    blink = Blink(session=session)
    blink.auth = Auth({"username": email, "password": password}, no_prompt=True, session=session)
    try:
        await blink.start()
        return ("done", blink, session)
    except Exception as e:
        if "TwoFA" in type(e).__name__ or "2FA" in str(e):
            return ("2fa", blink, session)
        try: await session.close()
        except Exception: pass
        raise

async def _blink_finish_2fa(blink, code):
    return await blink.send_2fa_code(code)

async def _blink_save(blink, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    await blink.save(path)

async def _blink_close(session):
    try: await session.close()
    except Exception: pass

def _start_blink_service():
    for cmd in (["systemctl", "enable", "--now", "bar-blink"],
                ["sudo", "-n", "systemctl", "enable", "--now", "bar-blink"]):
        try:
            r = subprocess.run(cmd, timeout=20)
            if r.returncode == 0:
                return
        except Exception:
            continue

# ---------------------------------------------------------------------------
# HTTPS + Web Push: the page itself sends notifications, no extra app.
# Browsers only allow notifications from a secure origin, so install.sh makes a
# local CA + a server certificate, bar-web serves HTTPS on https.port, and plain
# HTTP on ui_port is a landing page that hands out the CA certificate and sends
# the browser on to the HTTPS address. Subscriptions are stored on the box and
# the notifier pushes to them through the browser vendor's push service.
# ---------------------------------------------------------------------------
import re, base64
TLS_DIR = APP_DIR + "/tls"
PUSH_DIR = APP_DIR + "/push"
VAPID_KEY = PUSH_DIR + "/vapid_private.pem"
SUBS_PATH = PUSH_DIR + "/subscriptions.json"
_subs_file_lock = threading.Lock()

def https_cfg(cfg):
    h = cfg.get("https", {})
    return bool(h.get("enabled", True)), int(h.get("port", 8943))

def tls_ready():
    return os.path.isfile(TLS_DIR + "/server.crt") and os.path.isfile(TLS_DIR + "/server.key")

def cert_names():
    """Addresses the server certificate covers (its subjectAltName)."""
    try:
        out = subprocess.run(["openssl", "x509", "-in", TLS_DIR + "/server.crt", "-noout",
                              "-ext", "subjectAltName"], capture_output=True, text=True, timeout=5).stdout
        return re.findall(r"(?:DNS|IP Address):([^,\s]+)", out)
    except Exception:
        return []

def vapid_public_key():
    """Base64url of the uncompressed P-256 public point: the applicationServerKey the
    browser's PushManager.subscribe() wants. Generates the key pair on first use."""
    try:
        from py_vapid import Vapid
        from cryptography.hazmat.primitives import serialization
        os.makedirs(PUSH_DIR, exist_ok=True)
        if not os.path.isfile(VAPID_KEY):
            v = Vapid(); v.generate_keys(); v.save_key(VAPID_KEY)
        v = Vapid.from_file(VAPID_KEY)
        raw = v.public_key.public_bytes(serialization.Encoding.X962,
                                        serialization.PublicFormat.UncompressedPoint)
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    except Exception:
        return None

def load_subs():
    try:
        return json.load(open(SUBS_PATH))
    except Exception:
        return []

def save_subs(subs):
    os.makedirs(PUSH_DIR, exist_ok=True)
    tmp = SUBS_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(subs, f, indent=1)
    os.replace(tmp, SUBS_PATH)

SW_JS = r"""
self.addEventListener('install',function(e){self.skipWaiting();});
self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim());});
// tell the box what happened to each push (shown / error), so Settings can show
// "last delivered to this device" and a failure is visible in the bar-web log
function ack(d,st,err){
  return self.registration.pushManager.getSubscription().then(function(s){
    return fetch('/push/ack',{method:'POST',credentials:'include',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({endpoint:s?s.endpoint:'',tag:d.tag||'',status:st,error:err||'',
        permission:(self.Notification&&self.Notification.permission)||'?'})});
  }).catch(function(){});
}
self.addEventListener('push',function(e){
  var d={};try{d=e.data?e.data.json():{};}catch(x){d={title:'Bling-a-Ring',body:e.data?e.data.text():''};}
  var ding=d.kind==='ding';
  var shown=self.registration.showNotification(d.title||'Bling-a-Ring',{
    body:d.body||'',tag:d.tag||'bar',renotify:true,requireInteraction:ding,
    vibrate:ding?[200,100,200,100,400]:[150],data:{url:d.url||'/'}});
  e.waitUntil(shown.then(function(){return ack(d,'shown','');},function(err){return ack(d,'error',String(err));}));
});
self.addEventListener('notificationclick',function(e){
  e.notification.close();
  var url=(e.notification.data&&e.notification.data.url)||'/';
  e.waitUntil(self.clients.matchAll({type:'window',includeUncontrolled:true}).then(function(cs){
    for(var i=0;i<cs.length;i++){if('focus' in cs[i]){cs[i].navigate(url);return cs[i].focus();}}
    return self.clients.openWindow(url);
  }));
});
"""

@app.route("/sw.js")
def sw_js():
    return Response(SW_JS, mimetype="application/javascript",
                    headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"})

@app.route("/ca.crt")
def ca_crt():
    fp = TLS_DIR + "/ca.crt"
    if not os.path.isfile(fp):
        abort(404)
    # application/x-x509-ca-cert makes iOS offer it as a profile and Android as a certificate
    return send_file(fp, mimetype="application/x-x509-ca-cert", as_attachment=True,
                     download_name="bling-a-ring-ca.crt")

@app.route("/push/key")
@authed
def push_key():
    k = vapid_public_key()
    if not k:
        return Response(json.dumps({"error": "push unavailable on this box (py_vapid missing?)"}), 503,
                        mimetype="application/json")
    return Response(json.dumps({"key": k, "count": len(load_subs())}), mimetype="application/json")

@app.route("/push/subscribe", methods=["POST"])
@authed
def push_subscribe():
    sub = request.get_json(silent=True) or {}
    if not isinstance(sub.get("endpoint"), str) or not sub["endpoint"].startswith("https://") \
            or not isinstance(sub.get("keys"), dict):
        abort(400)
    with _subs_file_lock:
        subs = [s for s in load_subs() if s.get("endpoint") != sub["endpoint"]]
        subs.append({"endpoint": sub["endpoint"], "keys": sub["keys"],
                     "ua": request.headers.get("User-Agent", "")[:120],
                     "added": datetime.now().isoformat(timespec="seconds")})
        save_subs(subs)
    return Response(json.dumps({"ok": True, "count": len(subs)}), mimetype="application/json")

@app.route("/push/status")
@authed
def push_status():
    ep = request.args.get("endpoint", "")
    for s in load_subs():
        if s.get("endpoint") == ep:
            return Response(json.dumps({"last": s.get("last")}), mimetype="application/json")
    return Response(json.dumps({"last": None, "subscribed": False}), mimetype="application/json")

@app.route("/push/ack", methods=["POST"])
def push_ack():
    # unauthenticated on purpose: the service worker calls it in the background, when the
    # page may be closed and no auth prompt is possible. Carries no secret, only a status.
    d = request.get_json(silent=True) or {}
    ep = str(d.get("endpoint", ""))
    with _subs_file_lock:
        subs = load_subs()
        for s in subs:
            if s.get("endpoint") == ep:
                s["last"] = {"ts": datetime.now().isoformat(timespec="seconds"),
                             "status": str(d.get("status", ""))[:20], "tag": str(d.get("tag", ""))[:60],
                             "error": str(d.get("error", ""))[:200],
                             "permission": str(d.get("permission", ""))[:12]}
        save_subs(subs)
    print(f"push ack: {d.get('status')} tag={d.get('tag')} perm={d.get('permission')} err={str(d.get('error',''))[:120]}", flush=True)
    return Response(json.dumps({"ok": True}), mimetype="application/json")

@app.route("/push/unsubscribe", methods=["POST"])
@authed
def push_unsubscribe():
    sub = request.get_json(silent=True) or {}
    with _subs_file_lock:
        subs = [s for s in load_subs() if s.get("endpoint") != sub.get("endpoint")]
        save_subs(subs)
    return Response(json.dumps({"ok": True, "count": len(subs)}), mimetype="application/json")

# --- the plain-HTTP landing app on ui_port: certificate hand-out + redirect ---
landing = Flask("bling-a-ring-landing")

def _https_url(https_port):
    host = request.host.rsplit(":", 1)[0] if not request.host.endswith("]") else request.host
    return f"https://{host}:{https_port}/"

@landing.route("/ca.crt")
def landing_ca():
    return ca_crt()

@landing.route("/", defaults={"path": ""})
@landing.route("/<path:path>")
def landing_page(path):
    cfg = load_cfg()
    _, hport = https_cfg(cfg)
    url = _https_url(hport)
    if path:
        return redirect(url + path)   # an old http bookmark lands on the same page over https
    names = cert_names()
    here = request.host.rsplit(":", 1)[0]
    covered = here in names or here.strip("[]") in names
    warn = "" if covered else (
        '<div class="banner" style="background:#301616;border-color:#6a2c2c;color:#f0bdbd">'
        f'You opened this box as <span class="path">{escape(here)}</span>, which is not in its certificate '
        f'(covered: {escape(", ".join(names) or "none")}). The padlock will fail. On the box run '
        '<span class="path">sudo bash install.sh</span> again after an address change, or add '
        f'<span class="path">EXTRA_SANS=IP:{escape(here)}</span> (or DNS:name) in front of it.</div>')
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<title>Bling-a-Ring - first time on this device</title><style>" + CSS +
        ".step{display:flex;gap:14px;margin:18px 0}.step .n{flex:0 0 32px;height:32px;border-radius:50%;background:var(--accent);color:#07131f;font-weight:700;display:flex;align-items:center;justify-content:center}"
        ".step h3{margin:4px 0 6px;font-size:15px}.step p{margin:0 0 8px;color:var(--muted);font-size:13px;line-height:1.55}"
        "details{margin:6px 0}summary{cursor:pointer;color:var(--accent);font-size:13px}details p{margin:6px 0 0 14px}"
        "</style></head><body><header><span class='title'><span class='b'>Bling</span>-a-<span class='r'>Ring</span></span></header>"
        "<div class='wrap'>" + warn +
        "<div class='sec'><h2>First time on this device</h2>"
        "<p class='sub'>This box uses its own certificate so your browser can show doorbell and motion "
        "notifications, with no extra app. Two steps, once per device.</p>"
        "<div class='step'><div class='n'>1</div><div><h3>Trust this box</h3>"
        "<p>Download the certificate, then install it from your phone's Settings (not by tapping the file).</p>"
        "<a class='btn primary' href='/ca.crt'>Download certificate</a>"
        "<details id='and' open><summary>Android</summary>"
        "<p>If you tap the downloaded file, Android shows this and installs nothing:</p>"
        "<p style='border-left:3px solid var(--ding);padding:8px 10px;background:var(--panel2);color:var(--text)'>"
        "&ldquo;This certificate from null must be installed in Settings. Only install CA certificates from "
        "organizations you trust.&rdquo;</p>"
        "<p>That is Android's rule for every CA certificate, not an error in the file. Do this instead:</p>"
        "<p>1. <a class='btn' href='intent:#Intent;action=android.settings.SECURITY_SETTINGS;end'>Open Android security settings</a> "
        "(or open Settings and search for <b>CA certificate</b>).<br>"
        "2. Samsung: <b>More security settings</b> &rsaquo; <b>Install from device storage</b>. "
        "Other Android: <b>Encryption &amp; credentials</b> &rsaquo; <b>Install a certificate</b>.<br>"
        "3. Tap <b>CA certificate</b>, then <b>Install anyway</b>.<br>"
        "4. Pick <span class='path'>bling-a-ring-ca.crt</span> from Downloads.</p>"
        "<p>Android then shows a &ldquo;network may be monitored&rdquo; notice: that is this certificate. "
        "Come back here for step 2.</p></details>"
        "<details><summary>iPhone / iPad</summary><p>Safari says &ldquo;Profile Downloaded&rdquo;. Open Settings &rsaquo; "
        "<b>Profile Downloaded</b> &rsaquo; Install. Then Settings &rsaquo; General &rsaquo; About &rsaquo; "
        "<b>Certificate Trust Settings</b> and switch Bling-a-Ring CA on.</p></details>"
        "<details><summary>Windows / Mac / Linux</summary><p>Chrome or Edge: Settings &rsaquo; Privacy and security &rsaquo; "
        "Security &rsaquo; Manage certificates &rsaquo; Authorities &rsaquo; Import. Firefox: Settings &rsaquo; "
        "Privacy &amp; Security &rsaquo; View Certificates &rsaquo; Authorities &rsaquo; Import.</p></details>"
        "</div></div>"
        "<div class='step'><div class='n'>2</div><div><h3>Open Bling-a-Ring</h3>"
        f"<p>Then bookmark it, or add it to your Home Screen.</p><a class='btn primary' id='go' href='{url}'>Open {escape(url)}</a>"
        "</div></div></div>"
        "<p class='sub' style='text-align:center'>Already done this here? <a id='go2' href='" + url + "'>Open Bling-a-Ring</a></p>"
        "</div><script>"
        "function mark(){try{localStorage.setItem('bar_trusted','1');}catch(e){}}"
        "if(!/Android/i.test(navigator.userAgent)){document.getElementById('and').removeAttribute('open');}"
        "document.getElementById('go').addEventListener('click',mark);document.getElementById('go2').addEventListener('click',mark);"
        "try{if(localStorage.getItem('bar_trusted')==='1')location.replace('" + url + "');}catch(e){}"
        "</script></body></html>")

CSS = """
:root{--bg:#0f1115;--panel:#181a20;--panel2:#1f222a;--border:#2a2e38;--text:#e7e9ee;
--muted:#9aa1ad;--accent:#4aa3ff;--accent-d:#2f7fe0;--ok:#5fd27a;--bad:#ff7a7a;
--ring:#4aa3ff;--blink:#8a7dff;--ding:#ffb454;--motion:#5fd27a}
*{box-sizing:border-box}html,body{margin:0}
body{background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;font-size:15px;line-height:1.45}
a{color:var(--accent);text-decoration:none}
header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:12px 16px;background:#14161c;border-bottom:1px solid var(--border)}
header .title{font-size:16px;font-weight:700;margin-right:4px;white-space:nowrap}
header .title .b{color:var(--blink)} header .title .r{color:var(--ring)}
header .spacer{flex:1 1 auto}header .count{color:var(--muted);font-size:13px}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;height:36px;padding:0 14px;border:1px solid var(--border);border-radius:8px;background:var(--panel2);color:var(--text);font-size:14px;font-weight:500;cursor:pointer;white-space:nowrap;line-height:1}
.btn:hover{background:#262a34;border-color:#3a3f4b}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#07131f;font-weight:650}
.btn .dot{width:8px;height:8px;border-radius:50%;background:currentColor;opacity:.85}
.wrap{max-width:820px;margin:0 auto;padding:16px}
.day{max-width:1100px;margin:14px auto 0;padding:4px 16px;color:var(--muted);font-size:13px;font-weight:600}
.grid{max-width:1100px;margin:8px auto 0;padding:0 16px;display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;overflow:hidden}
.card video{display:block;width:100%;aspect-ratio:16/9;background:#000;object-fit:cover}
.card .meta{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:9px 12px}
.card .meta .t{font-size:14px;font-weight:600}.card .meta .s{font-size:12px;color:var(--muted)}
.badge{display:inline-block;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;padding:2px 6px;border-radius:5px;margin-right:5px}
.badge.ring{background:rgba(74,163,255,.15);color:var(--ring)}
.badge.blink{background:rgba(138,125,255,.15);color:var(--blink)}
.badge.ding{background:rgba(255,180,84,.15);color:var(--ding)}
.badge.motion{background:rgba(95,210,122,.13);color:var(--motion)}
.card .dl{height:32px;width:32px;display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:16px}
.empty{max-width:1100px;margin:0 auto;padding:48px 16px;color:var(--muted);text-align:center}
.sec{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:16px}
.sec h2{margin:0 0 2px;font-size:15px;font-weight:650}.sec .sub{margin:0 0 14px;color:var(--muted);font-size:13px}
.field{margin:14px 0}.field:first-of-type{margin-top:4px}
.field label{display:block;margin-bottom:6px;font-size:13px;color:var(--muted);font-weight:500}
.field input[type=text],.field input[type=password],.field input[type=number]{width:100%;max-width:360px;height:38px;padding:0 12px;background:#0e1014;border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:15px}
.switch{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.switch input{width:20px;height:20px;accent-color:var(--accent)}
.statgrid{display:flex;flex-wrap:wrap;gap:10px 24px;font-size:14px}.statgrid .k{color:var(--muted);font-size:12px}.statgrid .v{font-weight:650}
.svc{display:flex;flex-wrap:wrap;gap:8px 16px;margin-top:12px;font-size:13px;color:var(--muted)}.svc .ok{color:var(--ok)}.svc .bad{color:var(--bad)}
.banner{background:#16301c;border:1px solid #2c6a3b;color:#bdf0cc;border-radius:10px;padding:11px 14px;margin-bottom:16px;font-size:14px}
.actions{display:flex;gap:10px;flex-wrap:wrap}
video.live{display:block;width:100%;max-height:74vh;background:#000;border-radius:12px}
.note{margin-top:12px;color:var(--muted);font-size:13px}
.path{font-family:ui-monospace,Consolas,monospace;font-size:12.5px;color:var(--text);word-break:break-all}
#toasts{position:fixed;right:14px;bottom:14px;z-index:50;display:flex;flex-direction:column;gap:8px}
.toast{background:var(--panel2);border:1px solid var(--border);border-left:4px solid var(--motion);border-radius:8px;padding:10px 14px;min-width:220px;box-shadow:0 6px 20px rgba(0,0,0,.4);animation:sl .2s ease}
.toast.ding{border-left-color:var(--ding)}
.toast .h{font-weight:650;font-size:14px}.toast .b{color:var(--muted);font-size:12px}
@keyframes sl{from{transform:translateY(8px);opacity:0}to{transform:none;opacity:1}}
header a.btn.active{background:var(--panel2);border-color:var(--accent);color:var(--accent)}
.filters{max-width:1100px;margin:14px auto 0;padding:0 16px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.filters .grp{display:flex;gap:4px;background:var(--panel);border:1px solid var(--border);border-radius:9px;padding:4px}
.chip{height:30px;padding:0 12px;border:0;border-radius:6px;background:transparent;color:var(--muted);font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap}
.chip:hover{color:var(--text)}
.chip.on{background:var(--accent);color:#07131f}
.filters select{height:38px;padding:0 10px;background:var(--panel);border:1px solid var(--border);border-radius:9px;color:var(--text);font-size:13px}
details.day{max-width:1100px;margin:14px auto 0;padding:0 16px}
details.day>summary{list-style:none;cursor:pointer;display:flex;align-items:center;gap:10px;padding:10px 12px;background:var(--panel);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:14px;font-weight:600;user-select:none}
details.day>summary::-webkit-details-marker{display:none}
details.day>summary::before{content:"\25B8";color:var(--muted);transition:transform .15s;font-size:12px}
details.day[open]>summary::before{transform:rotate(90deg)}
details.day>summary .cnt{color:var(--muted);font-weight:500;font-size:13px}
details.day>summary .thumbs{margin-left:auto;display:flex;gap:4px}
details.day>summary .thumbs img{width:40px;height:24px;object-fit:cover;border-radius:3px;background:#000}
details.day .grid{margin-top:10px}
.thumb{position:relative;cursor:pointer;aspect-ratio:16/9;background:#000;display:block;width:100%}
.thumb img{width:100%;height:100%;object-fit:cover;display:block}
.thumb .play{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#fff;opacity:.85;font-size:34px;text-shadow:0 2px 8px rgba(0,0,0,.6)}
.thumb:hover .play{opacity:1}
.thumb.empty{display:flex;align-items:center;justify-content:center;color:var(--muted);font-size:12px}
.clipvid{display:block;width:100%;aspect-ratio:16/9;background:#000}
.camsel{max-width:1100px;margin:14px auto 0;padding:0 16px;display:flex;flex-wrap:wrap;gap:8px}
.camsel a{display:inline-flex;align-items:center;gap:7px;height:38px;padding:0 14px;border:1px solid var(--border);border-radius:9px;background:var(--panel2);color:var(--text);font-size:14px;font-weight:500}
.camsel a.on{background:var(--accent);border-color:var(--accent);color:#07131f;font-weight:650}
.camsel a.off{opacity:.5}
.camsel a .sdot{width:8px;height:8px;border-radius:50%}
"""

# browser chime + live toasts via SSE; WebAudio two-tone ding, no audio asset needed
FEED_JS = """
(function(){
  var ac=null;
  function tone(f,t0,dur,gain){var o=ac.createOscillator(),g=ac.createGain();
    o.frequency.value=f;o.type='sine';o.connect(g);g.connect(ac.destination);
    g.gain.setValueAtTime(0.0001,t0);g.gain.exponentialRampToValueAtTime(gain,t0+0.02);
    g.gain.exponentialRampToValueAtTime(0.0001,t0+dur);o.start(t0);o.stop(t0+dur);}
  function ctx(){if(!ac)ac=new (window.AudioContext||window.webkitAudioContext)();
    if(ac.state==='suspended'){try{ac.resume();}catch(e){}}return ac;}
  function chime(){try{ctx();var n=ac.currentTime;tone(880,n,0.5,0.25);tone(660,n+0.35,0.7,0.25);}catch(e){}}
  function beep(){try{ctx();var n=ac.currentTime;tone(520,n,0.18,0.12);}catch(e){}}
  // mobile browsers only allow audio after a user gesture: create + resume the context
  // on the first tap/click, then later chimes are allowed.
  function unlock(){try{ctx();}catch(e){}document.removeEventListener('click',unlock);document.removeEventListener('touchstart',unlock);}
  document.addEventListener('click',unlock);document.addEventListener('touchstart',unlock);
  // keep the push service worker registered on every page (it is what shows the
  // notifications when the page is closed; Settings is where a device subscribes)
  if(window.isSecureContext&&'serviceWorker' in navigator){navigator.serviceWorker.register('/sw.js').catch(function(){});}
  function toast(ev){var wrap=document.getElementById('toasts');if(!wrap)return;
    var d=document.createElement('div');d.className='toast'+(ev.kind==='ding'?' ding':'');
    var icon=ev.kind==='ding'?'\\uD83D\\uDD14 ':'';
    var det=ev.detection?(' ('+ev.detection+')'):'';
    d.innerHTML='<div class="h">'+icon+(ev.kind==='ding'?'Doorbell':'Motion')+' &middot; '+(ev.camera||'')+'</div>'+
      '<div class="b">'+(ev.source||'')+det+' &middot; '+new Date().toLocaleTimeString()+'</div>';
    wrap.appendChild(d);setTimeout(function(){d.remove();},8000);}
  try{
    var es=new EventSource('/api/events');
    es.onmessage=function(m){var ev;try{ev=JSON.parse(m.data);}catch(e){return;}
      if(ev.kind==='ding')chime();else beep();toast(ev);};
  }catch(e){}
})();
"""

PUSH_JS = """
<script>
(function(){
  var on=document.getElementById('pushon'),off=document.getElementById('pushoff'),st=document.getElementById('pushst');
  if(!on)return;
  var cnt=document.getElementById('pushcount');
  if(/iPhone|iPad|iPod/.test(navigator.userAgent)&&!window.navigator.standalone){document.getElementById('iosnote').style.display='block';}
  function b64(s){var p='='.repeat((4-s.length%4)%4),b=atob((s+p).replace(/-/g,'+').replace(/_/g,'/')),a=new Uint8Array(b.length);for(var i=0;i<b.length;i++)a[i]=b.charCodeAt(i);return a;}
  function say(t,bad){st.textContent=t;st.style.color=bad?'var(--bad)':'var(--text)';}
  function supported(){return window.isSecureContext&&'serviceWorker' in navigator&&'PushManager' in window&&'Notification' in window;}
  async function current(){try{var r=await navigator.serviceWorker.getRegistration('/');return r?await r.pushManager.getSubscription():null;}catch(e){return null;}}
  async function last(s){
    var el=document.getElementById('pushlast');if(!el||!s)return;
    try{var r=await fetch('/push/status?endpoint='+encodeURIComponent(s.endpoint),{credentials:'same-origin'});var j=await r.json();
      if(j.last){el.textContent='Last push to this device: '+j.last.status+' at '+j.last.ts+(j.last.error?(' ('+j.last.error+')'):'')+', browser permission '+j.last.permission+'.';}
      else{el.textContent='No push has reached this device yet. Browser permission: '+Notification.permission+'.';}
    }catch(e){}
  }
  async function refresh(){
    if(!supported()){say('This browser cannot do push notifications'+(window.isSecureContext?'':' on a non-HTTPS page')+'.',true);on.style.display='none';return;}
    var s=await current();
    if(s){say('Notifications are ON for this device.');on.style.display='none';off.style.display='';last(s);}
    else if(Notification.permission==='denied'){say('Notifications are blocked for this site in the browser settings. Allow them there, then try again.',true);}
    else{say('Notifications are off for this device.');on.style.display='';off.style.display='none';}
  }
  on.addEventListener('click',async function(){
    try{
      say('Asking for permission...');
      var perm=await Notification.requestPermission();
      if(perm!=='granted'){say('Permission not granted.',true);return;}
      var reg=await navigator.serviceWorker.register('/sw.js');await navigator.serviceWorker.ready;
      var kr=await fetch('/push/key',{credentials:'same-origin'});var kj=await kr.json();
      if(!kj.key){say(kj.error||'Push is not available on this box.',true);return;}
      var sub=await reg.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:b64(kj.key)});
      var r=await fetch('/push/subscribe',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify(sub.toJSON())});
      var j=await r.json();if(cnt)cnt.textContent=j.count;
      await refresh();say('Notifications are ON for this device. Press "Send test notification" to check.');
    }catch(e){say('Could not enable: '+(e&&e.message||e),true);}
  });
  off.addEventListener('click',async function(){
    try{var s=await current();if(s){var ep=s.endpoint;await s.unsubscribe();
      var r=await fetch('/push/unsubscribe',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({endpoint:ep})});
      var j=await r.json();if(cnt)cnt.textContent=j.count;}
      await refresh();
    }catch(e){say('Could not turn off: '+(e&&e.message||e),true);}
  });
  refresh();
})();
</script>
"""

CLIPS_JS = """
<script>
function playClip(el){
  var v=document.createElement('video');
  v.src=el.getAttribute('data-src'); v.controls=true; v.autoplay=true;
  v.setAttribute('playsinline',''); v.className='clipvid';
  el.replaceWith(v);
}
(function(){
  var f={source:'all',kind:'all',cam:'all'};
  function apply(){
    document.querySelectorAll('details.day').forEach(function(d){
      var shown=0;
      d.querySelectorAll('.card.clip').forEach(function(c){
        var ok=(f.source=='all'||c.dataset.source==f.source)
             &&(f.kind=='all'||c.dataset.kind==f.kind)
             &&(f.cam=='all'||c.dataset.cam==f.cam);
        c.style.display=ok?'':'none'; if(ok)shown++;
      });
      d.style.display=shown?'':'none';
    });
  }
  document.querySelectorAll('.grp').forEach(function(g){
    g.addEventListener('click',function(e){
      var b=e.target.closest('.chip'); if(!b)return;
      g.querySelectorAll('.chip').forEach(function(x){x.classList.remove('on')});
      b.classList.add('on'); f[g.dataset.filter]=b.dataset.v; apply();
    });
  });
  var cs=document.getElementById('camfilter');
  if(cs)cs.addEventListener('change',function(){f.cam=cs.value;apply();});
})();
</script>
"""

def page_head(cfg, count=None, active=""):
    cnt = f'<span class="count">{count} clips</span>' if count is not None else ""
    def tab(href, label, key):
        a = " active" if key == active else ""
        return f"<a class='btn{a}' href='{href}'>{label}</a>"
    return (
        "<!doctype html><html lang=en><head><meta charset=utf-8>"
        "<meta name=viewport content='width=device-width,initial-scale=1'>"
        "<title>Bling-a-Ring</title><style>" + CSS + "</style></head><body>"
        "<div id='toasts'></div>"
        "<header>"
        "<span class='title'><span class='b'>Bling</span>-a-<span class='r'>Ring</span></span>"
        + tab("/", "Clips", "clips")
        + tab("/cams", "Cameras", "cams")
        + tab("/live", "<span class='dot'></span>Live", "live")
        + tab("/settings", "Settings", "settings")
        + "<span class='spacer'></span>" + cnt +
        "</header>"
    )

def page_tail():
    return f"<script>{FEED_JS}</script></body></html>"

@app.route("/")
@authed
def index():
    cfg = load_cfg()
    clips = list_clips(cfg)
    h = [page_head(cfg, len(clips), active="clips")]
    brands = "Ring and Blink clips" if ring_on(cfg) else "Blink clips"
    if not clips:
        h.append(f'<div class="empty">No clips yet. {brands} appear here on motion or a doorbell press.</div>')
        h.append(page_tail())
        return "".join(h)

    cam_names = sorted({c["cam"] for c in clips})
    camopts = "".join(f'<option value="{escape(c)}">{escape(c)}</option>' for c in cam_names)
    source_chips = (
        '<div class="grp" data-filter="source">'
        '<button class="chip on" data-v="all">All</button>'
        '<button class="chip" data-v="ring">Ring</button>'
        '<button class="chip" data-v="blink">Blink</button></div>') if ring_on(cfg) else ""
    h.append(
        '<div class="filters">' + source_chips +
        '<div class="grp" data-filter="kind">'
        '<button class="chip on" data-v="all">All</button>'
        '<button class="chip" data-v="motion">Motion</button>'
        '<button class="chip" data-v="ding">Doorbell</button></div>'
        f'<select id="camfilter"><option value="all">All cameras</option>{camopts}</select>'
        '</div>')

    # group clips into days (already sorted newest-first)
    groups = []
    last = None
    for c in clips:
        day = datetime.fromtimestamp(c["mtime"]).strftime("%A, %d %B %Y")
        if day != last:
            groups.append((day, [])); last = day
        groups[-1][1].append(c)

    for gi, (day, gclips) in enumerate(groups):
        openattr = " open" if gi == 0 else ""   # most recent day expanded, rest collapsed
        thumbs = "".join(
            f'<img loading="lazy" src="/thumb/{escape(c["dev"])}/{escape(c["name"])}.jpg">'
            for c in gclips[:3] if c["thumb"])
        h.append(f'<details class="day"{openattr}><summary>{escape(day)} '
                 f'<span class="cnt">{len(gclips)} clips</span>'
                 f'<span class="thumbs">{thumbs}</span></summary><div class="grid">')
        for c in gclips:
            t = datetime.fromtimestamp(c["mtime"]).strftime("%H:%M:%S")
            src = f'/clip/{escape(c["dev"])}/{escape(c["name"])}'
            kind = "ding" if c["trigger"] == "ding" else "motion"
            srcbadge = f'<span class="badge {escape(c["source"])}">{escape(c["source"])}</span>'
            kindbadge = f'<span class="badge {kind}">{"doorbell" if kind=="ding" else "motion"}</span>'
            if c["thumb"]:
                thumb = (f'<div class="thumb" data-src="{src}" onclick="playClip(this)">'
                         f'<img loading="lazy" src="/thumb/{escape(c["dev"])}/{escape(c["name"])}.jpg">'
                         f'<div class="play">&#9654;</div></div>')
            else:
                thumb = (f'<div class="thumb empty" data-src="{src}" onclick="playClip(this)">'
                         f'<div class="play">&#9654;</div></div>')
            h.append(
                f'<div class="card clip" data-source="{escape(c["source"])}" data-kind="{kind}" data-cam="{escape(c["cam"])}">'
                f'{thumb}'
                f'<div class="meta"><span><span class="t">{t}</span><br>'
                f'<span class="s">{srcbadge}{kindbadge}{escape(c["cam"])} &middot; {human_size(c["size"])}</span></span>'
                f'<a class="dl" href="{src}?dl=1" title="Download" download>&#8681;</a></div></div>')
        h.append('</div></details>')
    h.append("<div style='height:24px'></div>")
    h.append(CLIPS_JS)
    h.append(page_tail())
    return "".join(h)

@app.route("/clip/<dev>/<name>")
@authed
def clip(dev, name):
    cfg = load_cfg()
    if not (_safe(dev) and _safe(name)) or not name.endswith(".mp4"):
        abort(400)
    fp = os.path.join(cfg["clips_dir"], dev, name)
    if not os.path.isfile(fp):
        abort(404)
    r = send_file(fp, mimetype="video/mp4", conditional=True)
    if request.args.get("dl"):
        r.headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return r

@app.route("/thumb/<dev>/<name>")
@authed
def thumb(dev, name):
    cfg = load_cfg()
    if not (_safe(dev) and _safe(name)):
        abort(400)
    fp = os.path.join(cfg["clips_dir"], dev, name)
    if not os.path.isfile(fp):
        abort(404)
    return send_file(fp, mimetype="image/jpeg", conditional=True)

@app.route("/api/events")
@authed
def api_events():
    q = queue.Queue(maxsize=50)
    with _subs_lock:
        _subs.add(q)
    def gen():
        try:
            yield "retry: 3000\n\n"
            while True:
                try:
                    data = q.get(timeout=20)
                    yield f"data: {data}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        except GeneratorExit:
            pass
        finally:
            with _subs_lock:
                _subs.discard(q)
    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/live")
@authed
def live():
    cfg = load_cfg()
    names = cfg.get("cameras", {})
    # entry: (devid, name, source, online, live_ok)
    entries = []
    for devid, name in names.items():
        if devid.startswith("blink_"):
            st = _blink_status.get(devid[6:], {})
            entries.append((devid, name, "blink", st.get("online"), bool(st.get("live"))))
        elif ring_on(cfg):
            entries.append((devid, name, "ring", "online", True))
    sel = request.args.get("cam", "")
    if sel not in {d for d, *_ in entries}:
        sel = next((d for d, _, _, _, ok in entries if ok), entries[0][0] if entries else "")

    h = [page_head(cfg, active="live")]
    if not entries:
        h.append('<div class="empty">No cameras yet. Add one in Settings.</div>')
        h.append(page_tail()); return "".join(h)

    h.append('<div class="camsel">')
    for devid, name, source, online, live_ok in entries:
        cls = "on" if devid == sel else ("off" if not live_ok else "")
        dot = "var(--ok)" if (source == "ring" or online == "online") else (
              "var(--bad)" if online == "offline" else "var(--muted)")
        h.append(f'<a class="{cls}" href="/live?cam={escape(devid)}">'
                 f'<span class="sdot" style="background:{dot}"></span>{escape(name)}</a>')
    h.append('</div>')

    h.append('<div class="wrap">')
    cur = next((e for e in entries if e[0] == sel), None)
    devid, name, source, online, live_ok = cur
    if source == "ring":
        h.append('<video class="live" autoplay muted playsinline controls src="/api/live.mp4"></video>')
        h.append(f'<p class="note">Live view of &ldquo;{escape(name)}&rdquo; (Ring). '
                 'Tap and unmute for audio.</p>')
    elif live_ok:
        h.append(f'<video class="live" autoplay muted playsinline controls '
                 f'src="/api/blink_live.mp4?cam={escape(devid[6:])}"></video>')
        h.append(f'<p class="note">Live view of &ldquo;{escape(name)}&rdquo; (Blink). Blink live sessions '
                 'are time-limited; reload to start a new one. Tap and unmute for audio.</p>')
    else:
        reason = "is offline" if online == "offline" else "does not support live view"
        h.append(f'<div class="empty">&ldquo;{escape(name)}&rdquo; {reason}. Pick another camera above.</div>')
    h.append('</div>')
    h.append(page_tail())
    return "".join(h)

@app.route("/api/live.mp4")
@authed
def live_mp4():
    cfg = load_cfg()
    up = cfg["go2rtc_local"] + "/api/stream.mp4?src=" + cfg.get("live_stream", "")
    try:
        r = urllib.request.urlopen(up, timeout=35)
    except Exception as e:
        return Response(f"live stream unavailable: {e}", 502)
    def gen():
        try:
            while True:
                chunk = r.read(65536)
                if not chunk:
                    break
                yield chunk
        except (GeneratorExit, Exception):
            pass
        finally:
            try: r.close()
            except Exception: pass
    return Response(gen(), mimetype="video/mp4")

@app.route("/blive/<camsafe>")
@authed
def blive(camsafe):
    # kept for backward compatibility: the live view is now the unified /live page
    if not _safe(camsafe):
        abort(400)
    return redirect(url_for("live", cam="blink_" + camsafe))

@app.route("/api/blink_live.mp4")
@authed
def blink_live_mp4():
    cfg = load_cfg()
    cam = request.args.get("cam", "")
    if not _safe(cam):
        abort(400)
    live_port = int(cfg.get("blink", {}).get("live_port", 8971))
    # ask the Blink receiver (which holds the authenticated session) to open an
    # immis live stream and tell us the local TCP port it re-serves the TS on.
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{live_port}/live/{cam}", timeout=25) as r:
            info = json.loads(r.read().decode())
        port = info.get("port")
    except urllib.error.HTTPError as e:
        try: msg = json.loads(e.read().decode()).get("error", str(e))
        except Exception: msg = str(e)
        return Response(f"Blink live unavailable: {msg}", 502)
    except Exception as e:
        return Response(f"Blink live unavailable: {e}", 502)
    if not port:
        return Response("Blink live: no stream port", 502)
    ff = cfg.get("ffmpeg", "ffmpeg")
    # TS carries H264 + AAC already: copy video, re-wrap audio as AAC, fragment for <video>.
    cmd = [ff, "-nostdin", "-hide_banner", "-loglevel", "error",
           "-fflags", "+genpts", "-analyzeduration", "5M", "-probesize", "5M",
           "-i", f"tcp://127.0.0.1:{port}",
           "-c:v", "copy", "-c:a", "aac", "-f", "mp4",
           "-movflags", "frag_keyframe+empty_moov+default_base_moof", "pipe:1"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    def gen():
        try:
            while True:
                chunk = proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        except (GeneratorExit, Exception):
            pass
        finally:
            try:
                proc.kill(); proc.wait(timeout=5)   # reap so it doesn't linger as a zombie
            except Exception: pass
    return Response(gen(), mimetype="video/mp4")

@app.route("/cams")
@authed
def cams():
    cfg = load_cfg(); names = cfg.get("cameras", {})
    h = [page_head(cfg, active="cams")]
    h.append('<div class="grid" style="margin-top:14px">')
    for devid, name in names.items():
        src = "blink" if devid.startswith("blink_") else "ring"
        if src == "ring" and not ring_on(cfg):
            continue
        ostate = None       # "online" / "offline" / None (unknown or n/a)
        live_href = None
        if src == "ring":
            cam = _ring_cam.get(devid, {})
            mstate = cam.get("motion")
            on = (mstate == "ON")
            known = mstate is not None
            snap = f"/snap/ring/{escape(devid)}.jpg"
            w = cam.get("wifi")
            sub = (f"wifi {w} dBm" if w is not None else "Ring")
            live_href = f"/live?cam={escape(devid)}"
        else:
            camsafe = devid[6:]
            st = _blink_status.get(camsafe, {})
            me = st.get("motion_enabled")
            # blinkpy reports the string "unknown" when Blink omits the flag; bool("unknown")
            # is True, which showed every such camera as "Motion: on". Only a real bool counts.
            known = isinstance(me, bool)
            on = me if known else False
            snap = f"/snap/blink/{escape(camsafe)}.jpg"
            bat = st.get("battery"); temp = st.get("temperature")
            ostate = st.get("online")
            sub = "Blink" + (f" &middot; {escape(str(bat))}" if bat else "") + (f" &middot; {escape(str(temp))}&deg;" if temp else "")
            if st.get("live"):
                live_href = f"/live?cam={escape(devid)}"
        badge = f'<span class="badge {src}">{src}</span>'
        if ostate == "online":
            dot = '<span style="color:var(--ok)" title="online">&#9679; online</span> &middot; '
        elif ostate == "offline":
            dot = '<span style="color:var(--bad)" title="offline">&#9679; offline</span> &middot; '
        else:
            dot = ''
        imgstyle = "width:100%;aspect-ratio:16/9;object-fit:cover;background:#000"
        if ostate == "offline":
            imgstyle += ";opacity:.5;filter:grayscale(.7)"
        mtxt = ("on" if on else "off") if known else "unknown"
        nextval = "off" if on else "on"
        live_btn = f'<a class="btn" href="{live_href}">Live</a>' if live_href else ''
        h.append(
            f'<div class="card">'
            f'<img src="{snap}" style="{imgstyle}" onerror="this.style.opacity=.15">'
            f'<div class="meta"><span><span class="t">{escape(name)}</span><br>'
            f'<span class="s">{badge}{dot}{sub}</span></span></div>'
            f'<div style="padding:0 12px 12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
            f'<form method="post" action="/cam/{src}/{escape(devid)}/motion" style="margin:0">'
            f'<input type="hidden" name="v" value="{nextval}">'
            f'<button class="btn {"primary" if on else ""}" type="submit">Motion: {mtxt}</button></form>'
            f'<form method="post" action="/cam/{src}/{escape(devid)}/snap" style="margin:0">'
            f'<button class="btn" type="submit">Snapshot</button></form>'
            + live_btn
            + '</div></div>')
    h.append('</div>')
    if not names:
        h.append('<div class="empty">No cameras yet. Connect Blink and press Discover in Settings.</div>')
    h.append(page_tail())
    return "".join(h)

@app.route("/snap/ring/<dev>.jpg")
@authed
def snap_ring(dev):
    if not _safe(dev): abort(400)
    b = _ring_snap.get(dev)
    if not b: abort(404)
    return Response(b, mimetype="image/jpeg")

@app.route("/snap/blink/<camsafe>.jpg")
@authed
def snap_blink(camsafe):
    cfg = load_cfg()
    if not _safe(camsafe): abort(400)
    fp = os.path.join(cfg["clips_dir"], "blink_" + camsafe, "snapshot.jpg")
    if not os.path.isfile(fp): abort(404)
    return send_file(fp, mimetype="image/jpeg", conditional=True)

@app.route("/cam/<src>/<dev>/motion", methods=["POST"])
@authed
def cam_motion(src, dev):
    v = (request.form.get("v") or "").strip().lower()
    on = v in ("on", "1", "true")
    if src == "ring":
        prefix = _ring_cam.get(dev, {}).get("prefix")
        if prefix:
            _publish(f"{prefix}/motion_detection/command", "ON" if on else "OFF")
    else:
        camsafe = dev[6:] if dev.startswith("blink_") else dev
        _publish(f"bar/cmd/blink/{camsafe}/motion", "on" if on else "off")
        # the receiver waits for Blink to apply the change (measured 3-6 s), force-refreshes
        # and republishes the retained status; wait for that (up to 15 s) so the page we
        # redirect to shows the new state instead of the old one. An offline camera ignores
        # the command, in which case the state stays as it was, which is the truth.
        deadline = time.time() + 15.0
        while time.time() < deadline:
            if _blink_status.get(camsafe, {}).get("motion_enabled") is on:
                break
            time.sleep(0.25)
    return redirect(url_for("cams"))

@app.route("/cam/<src>/<dev>/snap", methods=["POST"])
@authed
def cam_snap(src, dev):
    if src == "ring":
        prefix = _ring_cam.get(dev, {}).get("prefix")
        if prefix:
            _publish(f"{prefix}/take_snapshot/command", "ON")
    else:
        camsafe = dev[6:] if dev.startswith("blink_") else dev
        _publish(f"bar/cmd/blink/{camsafe}/snap", "now")
    return redirect(url_for("cams"))

def svc_status():
    cfg = load_cfg()
    def is_active(s):
        try:
            return subprocess.run(["systemctl", "is-active", s], capture_output=True,
                                  text=True, timeout=5).stdout.strip()
        except Exception:
            return "unknown"
    out = {}
    units = (("ring-mqtt", "mosquitto", "bar-recorder", "bar-blink", "bar-notifier", "bar-web")
             if ring_on(cfg) else ("mosquitto", "bar-blink", "bar-notifier", "bar-web"))
    for s in units:
        out[s] = is_active(s)
    if ring_on(cfg):
        # go2rtc restreamer: install.sh names it bar-go2rtc-restream, but a box reusing an
        # existing ring-nvr stack names it ring-nvr-go2rtc-restream. Report whichever is up.
        g = "inactive"
        for unit in ("bar-go2rtc-restream", "ring-nvr-go2rtc-restream"):
            if is_active(unit) == "active":
                g = "active"; break
        out["go2rtc-restream"] = g
    # ntfy only appears when it's actually configured, so an unused install doesn't
    # show a permanent red line for a service the user never set up.
    if cfg.get("ntfy", {}).get("enabled"):
        out["ntfy"] = is_active("ntfy")
    return out

@app.route("/notify/test", methods=["POST"])
@authed
def notify_test():
    # the notifier turns this into a doorbell-shaped event: browser chime + ntfy push
    ok = _publish("bar/cmd/notify_test", "now")
    return redirect(url_for("settings", tested=1 if ok else 0))

@app.route("/discover", methods=["POST"])
@authed
def discover():
    # scoped sudoers rule lets the service user run this as root (see install.sh)
    try:
        subprocess.Popen(["sudo", "-n", VENV_PY, APP_DIR + "/discovery.py"])
    except Exception as e:
        return Response("discover failed: " + str(e), 500)
    return redirect(url_for("settings", discovering=1))

@app.route("/blink/login", methods=["POST"])
@authed
def blink_login_route():
    cfg = load_cfg()
    email = (request.form.get("blink_email") or "").strip()
    password = request.form.get("blink_password") or ""
    if not email or not password:
        return redirect(url_for("settings", blink_err="Enter email and password"))
    try:
        status, blink, session = _run_blink(_blink_start_login(email, password))
    except Exception as e:
        return redirect(url_for("settings", blink_err=str(e)[:100]))
    if status == "2fa":
        _blink_pending["blink"] = blink
        _blink_pending["session"] = session
        return redirect(url_for("settings", blink_2fa=1))
    try:
        _run_blink(_blink_save(blink, _blink_creds_path(cfg)))
        _run_blink(_blink_close(session))
    except Exception as e:
        return redirect(url_for("settings", blink_err=str(e)[:100]))
    _start_blink_service()
    return redirect(url_for("settings", blink_ok=1))

@app.route("/blink/verify", methods=["POST"])
@authed
def blink_verify_route():
    cfg = load_cfg()
    code = (request.form.get("blink_code") or "").strip()
    blink = _blink_pending.get("blink")
    session = _blink_pending.get("session")
    if not blink:
        return redirect(url_for("settings", blink_err="No pending login; start again"))
    try:
        ok = _run_blink(_blink_finish_2fa(blink, code))
    except Exception as e:
        return redirect(url_for("settings", blink_err=str(e)[:100]))
    if not ok:
        return redirect(url_for("settings", blink_err="2FA code rejected"))
    try:
        _run_blink(_blink_save(blink, _blink_creds_path(cfg)))
        _run_blink(_blink_close(session))
    except Exception as e:
        return redirect(url_for("settings", blink_err=str(e)[:100]))
    _blink_pending.clear()
    _start_blink_service()
    return redirect(url_for("settings", blink_ok=1))

@app.route("/settings", methods=["GET", "POST"])
@authed
def settings():
    cfg = load_cfg()
    if request.method == "POST":
        f = request.form
        cfg["auth_enabled"] = (f.get("auth_enabled") == "on")
        cfg["ui_user"] = (f.get("ui_user") or "nvr").strip()
        if f.get("ui_password"):
            cfg["ui_password"] = f.get("ui_password")
        def clamp(key, lo, hi, default):
            try: v = int(f.get(key, default))
            except (TypeError, ValueError): v = default
            return max(lo, min(hi, v))
        cfg["clip_max_seconds"] = clamp("clip_max_seconds", 10, 600, cfg.get("clip_max_seconds", 120))
        cfg["post_motion_seconds"] = clamp("post_motion_seconds", 0, 120, cfg.get("post_motion_seconds", 8))
        cfg["retention_days"] = clamp("retention_days", 1, 365, cfg.get("retention_days", 14))
        cfg["max_clips_gb"] = clamp("max_clips_gb", 1, 100000, cfg.get("max_clips_gb", 20))
        # storage location: only apply if the new path can be created and written
        old_clips = cfg.get("clips_dir", "")
        new_clips = (f.get("clips_dir") or old_clips).strip()
        clips_changed = False; clips_err = ""
        if new_clips and new_clips != old_clips:
            import getpass
            me = getpass.getuser()
            fix = (f"the service user ({me}) cannot write there. On the box run: "
                   f"sudo mkdir -p '{new_clips}' && sudo chown {me}:{me} '{new_clips}'")
            if not os.path.isabs(new_clips):
                clips_err = "must be an absolute path on the box, e.g. /mnt/ssd/bling-a-ring/clips"
            else:
                try:
                    os.makedirs(new_clips, exist_ok=True)
                    if os.access(new_clips, os.W_OK):
                        cfg["clips_dir"] = new_clips; clips_changed = True
                    else:
                        clips_err = fix
                except PermissionError:
                    clips_err = fix
                except Exception as e:
                    clips_err = str(e)[:120]
        # notifications
        ntfy = dict(cfg.get("ntfy", {}))
        ntfy["enabled"] = (f.get("ntfy_enabled") == "on")
        ntfy["server"] = (f.get("ntfy_server") or ntfy.get("server", "http://127.0.0.1:8099")).strip()
        ntfy["topic"] = (f.get("ntfy_topic") or ntfy.get("topic", "bling-a-ring")).strip()
        if f.get("ntfy_token"):
            ntfy["token"] = f.get("ntfy_token").strip()
        cfg["ntfy"] = ntfy
        chime = dict(cfg.get("chime", {}))
        chime["local_enabled"] = (f.get("chime_local") == "on")
        cfg["chime"] = chime
        # camera names
        cams = dict(cfg.get("cameras", {}))
        for dev in list(cams.keys()):
            nm = f.get("cam_" + dev)
            if nm:
                cams[dev] = nm.strip()
        cfg["cameras"] = cams
        save_cfg(cfg)
        # recorder + notifier always pick up settings via restart; bar-blink only
        # needs a restart when the clips folder moved (it caches the path at startup).
        services = ["bar-recorder", "bar-notifier"] if ring_on(cfg) else ["bar-notifier"]
        if clips_changed:
            services.append("bar-blink")
        for s in services:
            try: subprocess.run(["sudo", "-n", "systemctl", "restart", s], timeout=20)
            except Exception: pass
        args = {"saved": 1}
        if clips_err:
            args["clips_err"] = clips_err
        return redirect(url_for("settings", **args))

    svc = svc_status()
    clips = list_clips(cfg)
    total = sum(c["size"] for c in clips)
    du = shutil.disk_usage(cfg["clips_dir"]) if os.path.isdir(cfg["clips_dir"]) else None
    free = human_size(du.free) if du else "?"
    checked = "checked" if cfg.get("auth_enabled") else ""
    ntfy = cfg.get("ntfy", {}); chime = cfg.get("chime", {})
    nchecked = "checked" if ntfy.get("enabled") else ""
    cchecked = "checked" if chime.get("local_enabled") else ""
    svc_html = "".join(
        f'<span>{escape(k)}: <span class="{"ok" if v=="active" else "bad"}">{escape(v)}</span></span>'
        for k, v in svc.items())
    cams_html = "".join(
        f'<div class="field"><label>{escape(dev)}</label>'
        f'<input type="text" name="cam_{escape(dev)}" value="{escape(nm)}"></div>'
        for dev, nm in cfg.get("cameras", {}).items()
        if dev.startswith("blink_") or ring_on(cfg)) or '<p class="sub">No devices yet. Use Discover below.</p>'

    disc = {}
    try: disc = json.load(open(STATUS_PATH))
    except Exception: pass

    h = [page_head(cfg, active="settings")]
    h.append('<div class="wrap">')
    if request.args.get("saved"):
        h.append('<div class="banner">Settings saved.</div>')
    if request.args.get("clips_err"):
        h.append('<div class="banner" style="background:#301616;border-color:#6a2c2c;color:#f0bdbd">'
                 f'Storage location not changed: {escape(request.args.get("clips_err"))}</div>')
    if request.args.get("discovering"):
        h.append('<div class="banner">Discovering devices... this takes ~15s. Reload Settings to see results.</div>')
    if request.args.get("tested") == "1":
        h.append('<div class="banner">Test notification sent. The browser should have chimed; '
                 'check the ntfy app on your phone. If nothing arrived, see the ntfy line in Status above '
                 'and <span class="path">journalctl -u bar-notifier -n 20</span> on the box.</div>')
    elif request.args.get("tested") == "0":
        h.append('<div class="banner" style="background:#301616;border-color:#6a2c2c;color:#f0bdbd">'
                 'Test not sent: the web UI is not connected to the event bus (mosquitto).</div>')
    if request.args.get("blink_ok"):
        h.append('<div class="banner">Blink connected. Discover devices to list the Blink cameras.</div>')
    if request.args.get("blink_err"):
        h.append(f'<div class="banner" style="background:#301616;border-color:#6a2c2c;color:#f0bdbd">Blink login: {escape(request.args.get("blink_err"))}</div>')
    h.append(
        '<div class="sec"><h2>Status</h2><div class="statgrid">'
        f'<span><span class="v">{len(clips)}</span> <span class="k">clips</span></span>'
        f'<span><span class="v">{human_size(total)}</span> <span class="k">recorded</span></span>'
        f'<span><span class="v">{free}</span> <span class="k">free</span></span></div>'
        f'<div class="svc">{svc_html}</div></div>')
    # discovery
    dstate = disc.get("state")
    dinfo = ""
    ring = ring_on(cfg)
    if dstate == "done":
        found_ring = (f'Ring: {escape(", ".join(disc.get("ring", [])) or "none")}; ' if ring else "")
        dinfo = (f'<p class="sub">Last scan found {found_ring}'
                 f'Blink: {escape(", ".join(disc.get("blink", [])) or "none")}.</p>')
    elif disc.get("blink_error"):
        dinfo = f'<p class="sub">Blink discovery error: {escape(str(disc.get("blink_error")))}</p>'
    dev_src = ('Ring cameras come from ring-mqtt; Blink devices from your Blink login.' if ring
               else 'Devices come from your Blink login (connect it below first).')
    h.append(
        '<div class="sec"><h2>Devices</h2>'
        '<p class="sub">Add the doorbell or a new camera, then scan. ' + dev_src + '</p>' + dinfo +
        '<form method="post" action="/discover"><button class="btn primary" type="submit">Discover devices</button></form>'
        '</div>')
    # --- Blink account (standalone forms, not part of the main settings form) ---
    creds_exist = os.path.exists(_blink_creds_path(cfg))
    pending = bool(_blink_pending.get("blink")) or request.args.get("blink_2fa")
    h.append('<div class="sec"><h2>Blink account</h2>')
    if pending:
        h.append('<p class="sub">Blink sent a verification code to your email or phone. '
                 'Enter it to finish connecting.</p>'
                 '<form method="post" action="/blink/verify">'
                 '<div class="field"><label>2FA code</label>'
                 '<input type="text" name="blink_code" autocomplete="one-time-code"></div>'
                 '<button class="btn primary" type="submit">Verify</button></form>')
    elif creds_exist:
        h.append('<p class="sub">Blink is connected. To switch accounts, log in again below.</p>')
        h.append('<form method="post" action="/blink/login">'
                 '<div class="field"><label>Blink email</label><input type="text" name="blink_email"></div>'
                 '<div class="field"><label>Blink password</label><input type="password" name="blink_password"></div>'
                 '<button class="btn" type="submit">Re-connect Blink</button></form>')
    else:
        h.append('<p class="sub">Connect your Blink account to pull Blink clips and get '
                 'doorbell/motion alerts. You will get a 2FA code to enter next.</p>'
                 '<form method="post" action="/blink/login">'
                 '<div class="field"><label>Blink email</label><input type="text" name="blink_email"></div>'
                 '<div class="field"><label>Blink password</label><input type="password" name="blink_password"></div>'
                 '<button class="btn primary" type="submit">Connect Blink</button></form>')
    h.append('</div>')
    h.append('<form method="post">')
    h.append(
        '<div class="sec"><h2>Access</h2>'
        '<p class="sub">When off, anyone on the network can open this without a password.</p>'
        f'<div class="field"><div class="switch"><input id="ae" type="checkbox" name="auth_enabled" {checked}>'
        '<span><label for="ae" style="margin:0;color:var(--text)">Require a password</label></span></div></div>'
        f'<div class="field"><label>Username</label><input type="text" name="ui_user" value="{escape(cfg.get("ui_user","nvr"))}"></div>'
        '<div class="field"><label>Password</label><input type="password" name="ui_password" placeholder="leave blank to keep current"></div>'
        '</div>')
    # phone-side subscription hint: the box's own addresses on the ntfy port
    ntfy_port = "8099"
    try:
        ntfy_port = str(urllib.parse.urlsplit(ntfy.get("server", "")).port or 8099)
    except Exception:
        pass
    phone_urls = "".join(f'<span class="path">http://{escape(ip)}:{ntfy_port}</span> '
                         for ip in host_ips()) or '<span class="path">http://&lt;this-box&gt;:8099</span>'
    ntfy_state = svc.get("ntfy")
    if ntfy.get("enabled") and ntfy_state != "active":
        ntfy_note = ('<p class="sub" style="color:var(--bad)">Phone push is on but the ntfy server on this box '
                     f'is {escape(str(ntfy_state))}. Re-run install.sh, or point "ntfy server" at another ntfy server.</p>')
    else:
        ntfy_note = ""
    # --- browser notifications (Web Push): the primary route, no app needed ---
    n_subs = len(load_subs())
    https_on, hport = https_cfg(cfg)
    http_port = cfg.get("ui_port", 8970)
    if request.is_secure:
        push_block = (
            '<div class="field"><div id="pushst" class="hint" style="color:var(--text);font-size:13px;margin-bottom:8px">Checking this device...</div>'
            '<div class="actions">'
            '<button class="btn primary" type="button" id="pushon">Enable notifications on this device</button>'
            '<button class="btn" type="button" id="pushoff" style="display:none">Turn off on this device</button>'
            '</div>'
            f'<div class="hint" style="color:var(--muted);font-size:12px;margin-top:8px"><span id="pushcount">{n_subs}</span> device(s) subscribed on this box. '
            'Notifications arrive even with the page closed. Repeat this on each phone or computer.</div>'
            '<div id="pushlast" class="hint" style="color:var(--muted);font-size:12px;margin-top:4px"></div>'
            '<div class="hint" style="color:var(--muted);font-size:12px;margin-top:8px">If the test chimes here but nothing appears on the phone: '
            'Android Settings &rsaquo; Apps &rsaquo; Chrome &rsaquo; Notifications must be on, and in Chrome &rsaquo; Site settings &rsaquo; Notifications this site must be allowed. '
            'Samsung also hides notifications while battery saver or a Focus mode is active.</div>'
            '<div id="iosnote" class="hint" style="display:none;color:var(--ding);font-size:12px;margin-top:6px">iPhone/iPad: first add this page to the Home Screen (Share &rsaquo; Add to Home Screen) and open it from there; Apple only allows notifications for Home Screen web apps.</div>'
            '</div>')
    else:
        if https_on and tls_ready():
            where = f'<span class="path">https://{escape(request.host.rsplit(":", 1)[0])}:{hport}/settings</span>'
            push_block = (f'<div class="field"><div class="hint" style="color:var(--ding);font-size:13px;line-height:1.6">Browsers only allow notifications from an HTTPS page. '
                          f'Open {where} instead (first time on a device: open <span class="path">http://{escape(request.host.rsplit(":", 1)[0])}:{http_port}/</span> and follow the two steps).</div></div>')
        else:
            push_block = ('<div class="field"><div class="hint" style="color:var(--ding);font-size:13px;line-height:1.6">Browsers only allow notifications from an HTTPS page, and HTTPS is not set up on this box. '
                          'Re-run <span class="path">sudo bash install.sh</span>; it creates the certificate and turns HTTPS on.</div></div>')
    h.append(
        '<div class="sec"><h2>Notifications</h2>'
        '<p class="sub">Doorbell press plays a chime; motion sends an alert. '
        + ('Ring and Blink alike. ' if ring else '')
        + 'This page sends the notifications itself, no extra app. The browser also chimes while the page is open.</p>'
        + push_block +
        '<div class="field"><button class="btn" type="submit" formaction="/notify/test" formmethod="post" formnovalidate>Send test notification</button>'
        '<div class="hint" style="color:var(--muted);font-size:12px;margin-top:6px">A doorbell-style alert through the same path as a real one: this page chimes, every subscribed device gets the notification.</div></div>'
        f'<div class="field"><div class="switch"><input id="ce" type="checkbox" name="chime_local" {cchecked}>'
        '<span><label for="ce" style="margin:0;color:var(--text)">Play chime on this box\'s speaker</label></span></div></div>'
        '<details style="margin-top:6px"><summary style="cursor:pointer;color:var(--muted);font-size:13px">Optional: push through the ntfy app instead</summary>'
        '<div style="margin-top:10px">' + ntfy_note +
        f'<div class="field"><div class="switch"><input id="ne" type="checkbox" name="ntfy_enabled" {nchecked}>'
        '<span><label for="ne" style="margin:0;color:var(--text)">Phone push via ntfy</label></span></div></div>'
        f'<div class="field"><label>ntfy server (where this box posts to)</label><input type="text" name="ntfy_server" value="{escape(ntfy.get("server","http://127.0.0.1:8099"))}"></div>'
        f'<div class="field"><label>ntfy topic</label><input type="text" name="ntfy_topic" value="{escape(ntfy.get("topic","bling-a-ring"))}"></div>'
        '<div class="field"><label>On the phone</label>'
        '<div class="hint" style="color:var(--muted);font-size:13px;line-height:1.6">'
        'ntfy app, tap +, "Use another server", enter ' + phone_urls +
        f' and the topic <span class="path">{escape(ntfy.get("topic","bling-a-ring"))}</span>.</div></div>'
        '<div class="field" style="margin-bottom:0"><label>ntfy token (optional)</label><input type="password" name="ntfy_token" placeholder="leave blank to keep current"></div>'
        '</div></details>'
        '</div>')
    h.append(PUSH_JS)
    if ring:
        # these drive the Ring recorder only; Blink clips arrive whole from Blink
        h.append(
            '<div class="sec"><h2>Recording</h2>'
            f'<div class="field"><label>Max clip length (seconds)</label><input type="number" name="clip_max_seconds" min="10" max="600" value="{cfg.get("clip_max_seconds",120)}"></div>'
            f'<div class="field"><label>Stop delay after motion (seconds)</label><input type="number" name="post_motion_seconds" min="0" max="120" value="{cfg.get("post_motion_seconds",8)}"></div>'
            '</div>')
    drives = mounted_drives()
    drive_opts = '<option value="">Pick a drive on the box...</option>' + "".join(
        f'<option value="{escape(suggested_clips_dir(d["mount"]))}">{escape(d["mount"])} '
        f'&middot; {human_size(d["free"])} free of {human_size(d["total"])} ({escape(d["fstype"])})</option>'
        for d in drives)
    h.append(
        '<div class="sec"><h2>Storage</h2>'
        f'<div class="field"><label>Keep clips for (days)</label><input type="number" name="retention_days" min="1" max="365" value="{cfg.get("retention_days",14)}"></div>'
        f'<div class="field"><label>Max storage (GB)</label><input type="number" name="max_clips_gb" min="1" max="100000" value="{cfg.get("max_clips_gb",20)}"></div>'
        '<div class="field" style="margin-bottom:0"><label>Storage location (a folder on the box running Bling-a-Ring)</label>'
        f'<select id="drivesel" style="display:block;max-width:360px;width:100%;height:38px;margin-bottom:8px;padding:0 10px;background:#0e1014;border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:14px">{drive_opts}</select>'
        f'<input type="text" id="clipsdir" name="clips_dir" value="{escape(cfg["clips_dir"])}" placeholder="/mnt/ssd/bling-a-ring/clips">'
        '<div class="hint" style="color:var(--muted);font-size:12px;margin-top:6px">'
        f'Current: <span class="path">{escape(cfg["clips_dir"])}</span> &middot; {free} free. '
        'Clips are stored on the box, not on the device you are browsing from, so there is no file dialog: '
        'pick a drive above (it fills in a folder on it) or type any path. '
        'New clips go to the new folder; existing clips stay in the old one.</div></div>'
        '</div>'
        '<script>document.getElementById("drivesel").addEventListener("change",function(){'
        'if(this.value)document.getElementById("clipsdir").value=this.value;});</script>')
    h.append('<div class="sec"><h2>Camera names</h2>' + cams_html + '</div>')
    h.append('<div class="actions"><button class="btn primary" type="submit">Save settings</button>'
             '<a class="btn" href="/">Cancel</a></div>')
    h.append('<div style="height:24px"></div></form></div>')
    h.append(page_tail())
    return "".join(h)

if __name__ == "__main__":
    cfg = load_cfg()
    host = cfg.get("host_ip", "0.0.0.0")
    port = int(cfg.get("ui_port", 8970))
    https_on, hport = https_cfg(cfg)
    if https_on and tls_ready():
        # ui_port: plain HTTP landing (certificate + redirect). https.port: the app.
        from werkzeug.serving import make_server
        srv = make_server(host, port, landing, threaded=True)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"web: landing http://{host}:{port}  app https://{host}:{hport}  cert covers {cert_names()}", flush=True)
        app.run(host=host, port=hport, threaded=True,
                ssl_context=(TLS_DIR + "/server.crt", TLS_DIR + "/server.key"))
    else:
        print(f"web: http://{host}:{port} (no HTTPS: browser notifications unavailable)", flush=True)
        app.run(host=host, port=port, threaded=True)
