#!/usr/bin/env python3
"""One-time interactive Blink login for Bling-a-Ring.

Run once on the box (it prompts for the Blink email, password and the 2FA code
Blink emails/texts you), then saves the token so blink_receiver.py can run headless:

    sudo /opt/bling-a-ring/venv/bin/python /opt/bling-a-ring/blink_login_setup.py
"""
import asyncio, json, os

CFG = json.load(open(os.environ.get("BLING_CONFIG", "/opt/bling-a-ring/config.json")))
STATE_DIR = CFG.get("blink", {}).get("state_dir", "/opt/bling-a-ring/blink-state")
CREDS_FILE = os.path.join(STATE_DIR, "blink_creds.json")
os.makedirs(STATE_DIR, exist_ok=True)

async def main():
    from blinkpy.blinkpy import Blink
    from blinkpy.auth import Auth
    import aiohttp
    session = aiohttp.ClientSession()
    blink = Blink(session=session)
    email = input("Blink email: ").strip()
    password = input("Blink password: ").strip()
    auth = Auth({"username": email, "password": password}, no_prompt=True, session=session)
    blink.auth = auth
    await blink.start()
    # start() triggers a 2FA email/text; enter the code
    if not blink.available:
        code = input("2FA code from email/SMS: ").strip()
        await blink.auth.send_auth_key(blink, code)
        await blink.setup_post_verify()
    await blink.save(CREDS_FILE)
    print("Saved Blink credentials to", CREDS_FILE)
    print("Cameras found:", list(blink.cameras.keys()))
    await session.close()

if __name__ == "__main__":
    asyncio.run(main())
