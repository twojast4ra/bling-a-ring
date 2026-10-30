#!/usr/bin/env bash
# Bling-a-Ring installer - Debian/Ubuntu, amd64 or arm64 (EC2, VPS, mini-PC, Pi).
# Ring + Blink NVR: one timeline, doorbell chime + motion push, in-app discovery.
#   sudo bash install.sh                         Ring + Blink
#   sudo BLINK_ONLY=1 bash install.sh            Blink only (no Node, ring-mqtt, go2rtc)
#   sudo CLIPS_DIR=/mnt/ssd/clips bash install.sh
#   EXTRA_SANS="DNS:nvr.example.com" sudo bash install.sh   extra names for the certificate
#   WITH_NTFY=1 sudo bash install.sh             also run a local ntfy server (optional)
set -euo pipefail

APP=/opt/bling-a-ring
CLIPS_DIR="${CLIPS_DIR:-/var/lib/bling-a-ring/clips}"
BLINK_ONLY="${BLINK_ONLY:-0}"
SVC_USER=blingaring
RINGMQTT_VERSION=v5.9.3
GO2RTC_VERSION=v1.9.14
NTFY_VERSION=v2.35.0
HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"

[ "$(id -u)" = 0 ] || { echo "Please run with sudo."; exit 1; }
if [ "$BLINK_ONLY" = 1 ]; then
    MODE="Blink only"
    RING_SERVICES=""
else
    MODE="Ring + Blink"
    RING_SERVICES="ring-mqtt bar-go2rtc-restream bar-recorder"
fi
echo "==> Bling-a-Ring install  ($MODE  app=$APP  clips=$CLIPS_DIR)"

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq mosquitto mosquitto-clients ffmpeg jq git curl ca-certificates \
    python3 python3-venv python3-pip sudo

if [ "$BLINK_ONLY" != 1 ]; then
    echo "==> Node.js (for ring-mqtt)..."
    if ! command -v node >/dev/null 2>&1 || [ "$(node -v | sed 's/v\([0-9]*\).*/\1/')" -lt 20 ]; then
        curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
        apt-get install -y nodejs
    fi
    NODE="$(command -v node)"
fi

echo "==> Service user..."
id -u "$SVC_USER" >/dev/null 2>&1 || useradd --system --home "$APP" --shell /usr/sbin/nologin "$SVC_USER"

ARCH_RAW="$(uname -m)"
if [ "$BLINK_ONLY" != 1 ]; then
    echo "==> go2rtc $GO2RTC_VERSION..."
    case "$ARCH_RAW" in
        x86_64)  GA=linux_amd64;  SHA=32d616af226bd731678ffde328b94cfb94e30339bfefc469cfb76323144615a6 ;;
        aarch64) GA=linux_arm64;  SHA=359fabade8a7a51e81a55fe6df6b0ef81764a5e1d63179577534eaaa71904b50 ;;
        armv7l)  GA=linux_arm;    SHA=4d7e1639af5a2722a28e864468fd8099b3c1682565446c798bf9e3b38fde12e4 ;;
        *) echo "Unsupported arch $ARCH_RAW"; exit 1 ;;
    esac
    curl -fsSL -o /usr/local/bin/go2rtc \
        "https://github.com/AlexxIT/go2rtc/releases/download/${GO2RTC_VERSION}/go2rtc_${GA}"
    echo "${SHA}  /usr/local/bin/go2rtc" | sha256sum -c -
    chmod +x /usr/local/bin/go2rtc
fi

echo "==> Mosquitto (localhost only)..."
cat > /etc/mosquitto/conf.d/blingaring.conf <<'EOF'
listener 1883 127.0.0.1
allow_anonymous true
EOF
systemctl enable mosquitto >/dev/null 2>&1 || true
systemctl restart mosquitto

# Optional: a local ntfy server for people who prefer the ntfy app over browser
# notifications. Off by default (one less thing); WITH_NTFY=1 turns it on.
# Pinned + checksum-verified like go2rtc.
NTFY_OK=0
NA=""
if [ "${WITH_NTFY:-0}" = 1 ]; then
    echo "==> ntfy $NTFY_VERSION (optional phone-app push)..."
    case "$ARCH_RAW" in
        x86_64)  NA=linux_amd64; NSHA=da694a751d8c1681a39bd45682631837508dfd5721ad6226768617fac181a9bc ;;
        aarch64) NA=linux_arm64; NSHA=c0dd07b61c42d28ce9b710750d9f6920041146f517f29da5ed4c00a1892062b3 ;;
        armv7l)  NA=linux_armv7; NSHA=c514a8f4a9f9c894829f2d82fa6001545a4fd318202f28b946278ce83bb09d76 ;;
        *)       NA=""; NSHA="" ;;
    esac
fi
if [ -n "$NA" ]; then
    if [ "$(/usr/local/bin/ntfy --version 2>/dev/null | awk '{print $3}')" = "${NTFY_VERSION#v}" ]; then
        NTFY_OK=1
    else
        NTGZ="ntfy_${NTFY_VERSION#v}_${NA}.tar.gz"
        if curl -fsSL -o "/tmp/$NTGZ" "https://github.com/binwiederhier/ntfy/releases/download/${NTFY_VERSION}/${NTGZ}" \
           && echo "${NSHA}  /tmp/$NTGZ" | sha256sum -c - >/dev/null \
           && tar -xzf "/tmp/$NTGZ" -C /tmp "ntfy_${NTFY_VERSION#v}_${NA}/ntfy" \
           && install -m 0755 "/tmp/ntfy_${NTFY_VERSION#v}_${NA}/ntfy" /usr/local/bin/ntfy; then
            NTFY_OK=1
        fi
        rm -rf "/tmp/$NTGZ" "/tmp/ntfy_${NTFY_VERSION#v}_${NA}"
    fi
fi
if [ "$NTFY_OK" = 1 ]; then
    mkdir -p /etc/ntfy /var/cache/ntfy
    # Listens on every interface: the phone subscribes to http://<this-host>:8099 over
    # the LAN or Tailscale. Never open 8099 on a public firewall (see README).
    cat > /etc/ntfy/server.yml <<'EOF'
listen-http: ":8099"
cache-file: "/var/cache/ntfy/cache.db"
cache-duration: "12h"
behind-proxy: false
EOF
    cat > /etc/systemd/system/ntfy.service <<EOF
[Unit]
Description=ntfy push server
After=network-online.target
[Service]
ExecStart=/usr/local/bin/ntfy serve --config /etc/ntfy/server.yml
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
elif [ "${WITH_NTFY:-0}" = 1 ]; then
    echo "    ntfy could not be installed; ntfy push stays off in the config. Re-run to retry."
fi

echo "==> App dirs..."
mkdir -p "$APP" "$APP/logs" "$APP/blink-state" "$APP/sounds" "$CLIPS_DIR"

if [ "$BLINK_ONLY" != 1 ]; then
    echo "==> ring-mqtt $RINGMQTT_VERSION..."
    if [ ! -f "$APP/ring-mqtt/package.json" ]; then
        git clone --depth 1 --branch "$RINGMQTT_VERSION" https://github.com/tsightler/ring-mqtt.git "$APP/ring-mqtt"
    fi
    cd "$APP/ring-mqtt"
    npm install --no-audit --no-fund
    chmod +x "$APP/ring-mqtt/scripts/"*.sh
    mkdir -p "$APP/ring-mqtt/config"
    cat > "$APP/ring-mqtt/config.json" <<'EOF'
{ "mqtt_url": "mqtt://localhost:1883", "mqtt_options": "", "livestream_user": "",
  "livestream_pass": "", "disarm_code": "", "enable_cameras": true, "enable_modes": false,
  "enable_panic": false, "hass_topic": "homeassistant/status", "ring_topic": "ring",
  "location_ids": [] }
EOF
fi

echo "==> App files..."
cp "$HERE/recorder.py" "$HERE/webui.py" "$HERE/blink_receiver.py" "$HERE/notifier.py" \
   "$HERE/discovery.py" "$HERE/blink_login_setup.py" "$APP/"

echo "==> Python venv..."
python3 -m venv "$APP/venv"
"$APP/venv/bin/pip" install -q --upgrade pip
"$APP/venv/bin/pip" install -q paho-mqtt flask blinkpy firebase-messaging aiohttp requests pywebpush

echo "==> HTTPS certificate (browsers only allow page notifications over HTTPS)..."
# A local CA (made once) signs a server certificate (remade on every run so it
# carries the box's current addresses). Each phone trusts the CA once, from the
# landing page at http://<box>:8970. Add your own names with
#   EXTRA_SANS="DNS:nvr.example.com,IP:203.0.113.5" sudo bash install.sh
mkdir -p "$APP/tls" "$APP/push"
HN="$(hostname -s 2>/dev/null || hostname)"
if [ ! -f "$APP/tls/ca.key" ]; then
    openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
        -keyout "$APP/tls/ca.key" -out "$APP/tls/ca.crt" \
        -subj "/CN=Bling-a-Ring CA ($HN)" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        -addext "subjectKeyIdentifier=hash" >/dev/null 2>&1
fi
SANS="DNS:localhost,IP:127.0.0.1,DNS:${HN},DNS:${HN}.local,DNS:${HN}.lan,DNS:${HN}.home"
FQDN="$(hostname -f 2>/dev/null || true)"
if [ -n "$FQDN" ] && [ "$FQDN" != "$HN" ] && [ "$FQDN" != "localhost" ]; then SANS="$SANS,DNS:$FQDN"; fi
for ip in $(hostname -I 2>/dev/null); do SANS="$SANS,IP:$ip"; done
if [ -n "${EXTRA_SANS:-}" ]; then SANS="$SANS,$EXTRA_SANS"; fi
openssl req -new -newkey rsa:2048 -sha256 -nodes \
    -keyout "$APP/tls/server.key" -out "$APP/tls/server.csr" \
    -subj "/CN=Bling-a-Ring ($HN)" >/dev/null 2>&1
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\nbasicConstraints=CA:FALSE\n' \
    "$SANS" > "$APP/tls/server.ext"
# 820 days: Apple refuses to trust TLS server certificates valid for more than 825
openssl x509 -req -sha256 -days 820 -in "$APP/tls/server.csr" \
    -CA "$APP/tls/ca.crt" -CAkey "$APP/tls/ca.key" -CAcreateserial \
    -extfile "$APP/tls/server.ext" -out "$APP/tls/server.crt" >/dev/null 2>&1
rm -f "$APP/tls/server.csr" "$APP/tls/server.ext"
chmod 600 "$APP/tls/"*.key
echo "    certificate covers: $SANS"

echo "==> Doorbell sound for local chime..."
"$APP/venv/bin/python" - <<'PY'
import wave, struct, math
fr=44100
def tones(seq):
    data=[]
    for freq,dur in seq:
        for i in range(int(fr*dur)):
            data.append(int(32767*0.3*math.sin(2*math.pi*freq*i/fr)*math.exp(-3*i/(fr*dur))))
    return data
w=wave.open("/opt/bling-a-ring/sounds/doorbell.wav","w")
w.setnchannels(1); w.setsampwidth(2); w.setframerate(fr)
for s in tones([(880,0.45),(660,0.75)]):
    w.writeframes(struct.pack("<h", max(-32768,min(32767,s))))
w.close()
PY

echo "==> Config..."
if [ ! -f "$APP/config.json" ]; then
    PW="$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 16)"
    sed -e "s#\"clips_dir\": \"[^\"]*\"#\"clips_dir\": \"${CLIPS_DIR}\"#" \
        -e "s#\"ui_password\": \"[^\"]*\"#\"ui_password\": \"${PW}\"#" \
        "$HERE/config.sample.json" > "$APP/config.json"
else
    PW="$(jq -r '.ui_password' "$APP/config.json")"
fi
if [ "$BLINK_ONLY" = 1 ]; then
    # tells the web UI and discovery to leave Ring out entirely
    jq '.ring = ((.ring // {}) + {enabled: false})' "$APP/config.json" > "$APP/config.json.tmp" \
        && mv "$APP/config.json.tmp" "$APP/config.json"
fi
if [ "$NTFY_OK" = 1 ]; then
    jq '.ntfy = ((.ntfy // {}) + {enabled: true})' "$APP/config.json" > "$APP/config.json.tmp" \
        && mv "$APP/config.json.tmp" "$APP/config.json"
elif [ "${WITH_NTFY:-0}" = 1 ]; then
    # asked for but not installed: keep it off rather than log a refused connection per event
    jq '.ntfy = ((.ntfy // {}) + {enabled: false})' "$APP/config.json" > "$APP/config.json.tmp" \
        && mv "$APP/config.json.tmp" "$APP/config.json"
fi
# older config.json files (before HTTPS/push existed) get the new sections with defaults
jq '.https = ({enabled: true, port: 8943} + (.https // {})) | .push = ({enabled: true, contact: "mailto:admin@example.com"} + (.push // {}))' \
    "$APP/config.json" > "$APP/config.json.tmp" && mv "$APP/config.json.tmp" "$APP/config.json"
if [ "$BLINK_ONLY" != 1 ]; then
    [ -f "$APP/go2rtc-restream.yaml" ] || cat > "$APP/go2rtc-restream.yaml" <<'EOF'
log:
  level: info
api:
  listen: "127.0.0.1:1984"
rtsp:
  listen: "127.0.0.1:8556"
webrtc:
  listen: ""
streams: {}
EOF
fi

chown -R "$SVC_USER:$SVC_USER" "$APP" "$CLIPS_DIR"

echo "==> sudoers (lets the web UI run discovery + restart its own services)..."
# enable --now bar-blink: the in-app Blink login starts the receiver once creds are saved
RESTARTABLE="/usr/bin/systemctl restart bar-notifier, /usr/bin/systemctl restart bar-blink, /usr/bin/systemctl enable --now bar-blink, /usr/bin/systemctl restart bar-web"
if [ "$BLINK_ONLY" != 1 ]; then
    RESTARTABLE="$RESTARTABLE, /usr/bin/systemctl restart bar-recorder, /usr/bin/systemctl restart bar-go2rtc-restream"
fi
cat > /etc/sudoers.d/bling-a-ring <<EOF
$SVC_USER ALL=(root) NOPASSWD: $APP/venv/bin/python $APP/discovery.py
$SVC_USER ALL=(root) NOPASSWD: $RESTARTABLE
EOF
chmod 0440 /etc/sudoers.d/bling-a-ring

echo "==> systemd services..."
svc() { cat > "/etc/systemd/system/$1.service"; }

if [ "$BLINK_ONLY" != 1 ]; then
svc ring-mqtt <<EOF
[Unit]
Description=ring-mqtt (Ring bridge)
After=network-online.target mosquitto.service
Wants=network-online.target
[Service]
Type=simple
User=$SVC_USER
WorkingDirectory=$APP/ring-mqtt
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=NODE_ENV=production
Environment=RUNMODE=standard
ExecStart=$NODE $APP/ring-mqtt/ring-mqtt.js
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

svc bar-go2rtc-restream <<EOF
[Unit]
Description=Bling-a-Ring go2rtc restreamer
After=ring-mqtt.service
[Service]
Type=simple
User=$SVC_USER
ExecStart=/usr/local/bin/go2rtc -config $APP/go2rtc-restream.yaml
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

svc bar-recorder <<EOF
[Unit]
Description=Bling-a-Ring recorder (Ring)
After=ring-mqtt.service mosquitto.service
Wants=ring-mqtt.service
[Service]
Type=simple
User=$SVC_USER
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=BLING_CONFIG=$APP/config.json
ExecStart=$APP/venv/bin/python $APP/recorder.py
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
fi   # end of the Ring-only units

svc bar-blink <<EOF
[Unit]
Description=Bling-a-Ring Blink receiver (FCM + clips)
After=network-online.target mosquitto.service
Wants=network-online.target
[Service]
Type=simple
User=$SVC_USER
Environment=PATH=/usr/local/bin:/usr/bin:/bin
Environment=BLING_CONFIG=$APP/config.json
ExecStart=$APP/venv/bin/python $APP/blink_receiver.py
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
EOF

svc bar-notifier <<EOF
[Unit]
Description=Bling-a-Ring notifier (chime + push)
After=mosquitto.service
[Service]
Type=simple
User=$SVC_USER
Environment=BLING_CONFIG=$APP/config.json
ExecStart=$APP/venv/bin/python $APP/notifier.py
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

svc bar-web <<EOF
[Unit]
Description=Bling-a-Ring web UI
After=network-online.target
[Service]
Type=simple
User=$SVC_USER
Environment=BLING_CONFIG=$APP/config.json
ExecStart=$APP/venv/bin/python $APP/webui.py
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
if [ "$NTFY_OK" = 1 ]; then
    systemctl enable ntfy >/dev/null 2>&1 || true
    systemctl restart ntfy || echo "    WARNING: ntfy failed to start; see: journalctl -u ntfy"
fi
# shellcheck disable=SC2086  (RING_SERVICES is a deliberate word list, empty in Blink-only mode)
systemctl enable $RING_SERVICES bar-blink bar-notifier bar-web >/dev/null 2>&1
systemctl restart $RING_SERVICES bar-notifier bar-web
# bar-blink is started after the Blink login step below

echo
echo "====================================================================="
echo " Bling-a-Ring installed ($MODE)."
echo
STEP=1
if [ "$BLINK_ONLY" != 1 ]; then
    cat <<EOF
 $STEP) Ring login (one time). From your laptop:
        ssh -L 55123:localhost:55123 <user>@<this-host>
    then open  http://localhost:55123  and enter Ring email/pass/2FA.

EOF
    STEP=$((STEP + 1))
fi
cat <<EOF
 $STEP) Blink login (one time). Open the web UI (step $((STEP + 2))), go to
    Settings > Blink account, enter your Blink email/password and the 2FA
    code Blink sends you. The Blink receiver (bar-blink) starts by itself.
    Headless alternative, on this host:
        sudo $APP/venv/bin/python $APP/blink_login_setup.py
        sudo systemctl enable --now bar-blink

 $((STEP + 1))) Discover devices: Settings > Discover devices (or on this host:
        sudo $APP/venv/bin/python $APP/discovery.py )

 $((STEP + 2))) Open  http://<this-host>:8970  on your phone or computer.
    This host's addresses: $(hostname -I 2>/dev/null || echo "?")
    First time on a device it shows two steps: trust this box's certificate,
    then open the app at https://<this-host>:8943. Bookmark that.
    (login nvr / ${PW} once you turn the password on in Settings > Access)

 Notifications: Settings > Notifications > "Enable notifications on this device",
    then "Send test notification". Repeat on each phone. No extra app needed.
EOF
if [ "$NTFY_OK" = 1 ]; then
    echo "    ntfy app alternative: server http://<this-host>:8099, topic bling-a-ring."
fi
cat <<EOF

 Keep ports 8970/8943 off the public internet (home network or a VPN only).
 Storage: ${CLIPS_DIR}
=====================================================================
EOF
