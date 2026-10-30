#!/usr/bin/env python3
"""Bling-a-Ring recorder: records the Ring go2rtc restream to disk on every
Ring event ring-mqtt reports (motion and doorbell ding), tagging each clip with
its trigger. Blink clips are handled separately by blink_receiver.py; both land
in the same clips_dir and show in one timeline.

Records through the restreamer (record_rtsp), which owns a single managed Ring
connection shared with live view, so recording and live view never trigger
duelling Ring sessions.
"""
import json, os, subprocess, threading, time
from datetime import datetime
import paho.mqtt.client as mqtt

CFG = json.load(open(os.environ.get("BLING_CONFIG", "/opt/bling-a-ring/config.json")))
MQTT_HOST, MQTT_PORT = CFG["mqtt_host"], CFG["mqtt_port"]
CLIPS = CFG["clips_dir"]
REC_RTSP = CFG.get("record_rtsp", "rtsp://127.0.0.1:8556")
CLIP_MAX, POST = CFG["clip_max_seconds"], CFG["post_motion_seconds"]
FFMPEG = CFG.get("ffmpeg", "ffmpeg")
NAMES = CFG.get("cameras", {})
MIN_OK = 50000
MAX_ATTEMPTS = 4
RETRY_DELAY = 8

state = {}
lock = threading.Lock()

def log(*a): print(datetime.now().isoformat(), "recorder", *a, flush=True)
def name_of(d): return NAMES.get(d, d)

def start_rec(dev, trigger="motion"):
    with lock:
        s = state.get(dev)
        if s and s["running"]:
            s["stop"] = False
            if s["timer"]:
                s["timer"].cancel(); s["timer"] = None
            # a ding arriving during a motion recording upgrades the clip's tag
            if trigger == "ding":
                s["trigger"] = "ding"
            return
        state[dev] = {"stop": False, "proc": None, "timer": None, "running": True, "trigger": trigger}
    threading.Thread(target=_worker, args=(dev,), daemon=True).start()

def _worker(dev):
    cam = name_of(dev)
    url = f"{REC_RTSP}/{dev}"
    d = os.path.join(CLIPS, dev); os.makedirs(d, exist_ok=True)
    saved = False
    for attempt in range(MAX_ATTEMPTS):
        with lock:
            if state[dev]["stop"]:
                break
            trigger = state[dev]["trigger"]
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out = os.path.join(d, ts + ".mp4")
        part = out + ".part"
        cmd = [FFMPEG, "-nostdin", "-rtsp_transport", "tcp", "-i", url,
               "-t", str(CLIP_MAX), "-c", "copy", "-movflags", "+faststart",
               "-f", "mp4", "-y", part]   # -f mp4: .part extension can't imply the muxer
        log(f"[{cam}] START ({trigger}) attempt {attempt+1} -> {os.path.basename(out)}")
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=open(part + ".log", "wb"))
        with lock:
            state[dev]["proc"] = proc
        proc.wait()
        sz = os.path.getsize(part) if os.path.exists(part) else 0
        if sz > MIN_OK:
            os.replace(part, out)
            try: os.remove(part + ".log")
            except Exception: pass
            with lock:
                trigger = state[dev]["trigger"]
            json.dump({"camera": cam, "device_id": dev, "start": ts,
                       "trigger": trigger, "source": "ring"},
                      open(out + ".json", "w"))
            try:
                subprocess.run([FFMPEG, "-nostdin", "-ss", "2", "-i", out, "-vframes", "1",
                                "-y", out + ".jpg"], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=30)
            except Exception: pass
            log(f"[{cam}] SAVED {os.path.basename(out)} ({sz} bytes)")
            saved = True
            break
        for f in (part, part + ".log"):
            try: os.remove(f)
            except Exception: pass
        with lock:
            if state[dev]["stop"]:
                break
        log(f"[{cam}] attempt {attempt+1} empty ({sz}b), retrying in {RETRY_DELAY}s")
        time.sleep(RETRY_DELAY)
    with lock:
        if state[dev]["timer"]:
            state[dev]["timer"].cancel()
        state[dev]["running"] = False
        state[dev]["proc"] = None
    if not saved:
        log(f"[{cam}] gave up after {MAX_ATTEMPTS} attempts")

def stop_soon(dev):
    with lock:
        s = state.get(dev)
        if not s or not s["running"] or s["timer"]:
            return
        cam = name_of(dev)
        def _s():
            with lock:
                ss = state.get(dev)
                if ss and ss["running"]:
                    ss["stop"] = True
                    log(f"[{cam}] motion cleared, stopping")
                    if ss["proc"]:
                        try: ss["proc"].terminate()
                        except Exception: pass
        t = threading.Timer(POST, _s)
        s["timer"] = t
        t.start()

def on_connect(c, u, f, rc, props=None):
    log("MQTT connected rc", rc)
    c.subscribe("ring/+/camera/+/motion/state")
    c.subscribe("ring/+/camera/+/ding/state")

def on_message(c, u, msg):
    p = msg.topic.split("/")
    if len(p) < 6:
        return
    dev, kind = p[3], p[4]
    payload = msg.payload.decode(errors="replace").strip().upper()
    if payload == "ON":
        start_rec(dev, "ding" if kind == "ding" else "motion")
    elif payload == "OFF":
        stop_soon(dev)

def _prune_to_size(limit_bytes):
    clips = []
    for root, _, files in os.walk(CLIPS):
        for fn in files:
            if fn.endswith(".mp4"):
                fp = os.path.join(root, fn)
                try:
                    st = os.stat(fp)
                    clips.append((st.st_mtime, fp, st.st_size))
                except OSError:
                    pass
    total = sum(c[2] for c in clips)
    if total <= limit_bytes:
        return
    clips.sort()
    for _mtime, fp, size in clips:
        if total <= limit_bytes:
            break
        try: os.remove(fp)
        except Exception: pass
        for sc in (fp + ".json", fp + ".jpg"):
            try: os.remove(sc)
            except Exception: pass
        total -= size
        log("size prune removed", os.path.basename(fp))

def retention():
    while True:
        try:
            cutoff = time.time() - CFG["retention_days"] * 86400
            for root, _, files in os.walk(CLIPS):
                for fn in files:
                    fp = os.path.join(root, fn)
                    try:
                        if os.path.getmtime(fp) < cutoff:
                            os.remove(fp)
                    except Exception: pass
            cap_gb = CFG.get("max_clips_gb", 0)
            if cap_gb and cap_gb > 0:
                _prune_to_size(int(cap_gb * 1024**3))
        except Exception as e:
            log("retention error", e)
        time.sleep(600)

def main():
    os.makedirs(CLIPS, exist_ok=True)
    threading.Thread(target=retention, daemon=True).start()
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
