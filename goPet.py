from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import queue
import random
import ssl
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

import pygame

try:
    import websockets
    from websockets.exceptions import WebSocketException
except ImportError:  # handled in main() so --simulate still works
    websockets = None
    WebSocketException = Exception  # type: ignore

log = logging.getLogger("study-tamagotchi")


# ======================================================================================
# Configuration
# ======================================================================================
@dataclass
class Config:
    # --- Emotiv / Cortex ---
    client_id: str = os.getenv("EMOTIV_CLIENT_ID", "")
    client_secret: str = os.getenv("EMOTIV_CLIENT_SECRET", "")
    license_id: str = os.getenv("EMOTIV_LICENSE", "")          # optional
    cortex_url: str = os.getenv("EMOTIV_CORTEX_URL", "wss://localhost:6868")
    cortex_cert: str = os.getenv("EMOTIV_CORTEX_CERT", "")     
    headset_id: str = os.getenv("EMOTIV_HEADSET_ID", "")       
    stress_metric: str = "str"       

    # --- Stress trigger (all adjustable) ---
    stress_threshold: float = 0.70   
    sustain_seconds: float = 30.0    
    min_samples: int = 2             
    stale_timeout: float = 45.0      
    grace_after_break: float = 60.0  

    # --- Pet / game ---
    alert_seconds: float = 3.0       
    breath_inhale: float = 4.0
    breath_hold: float = 2.0
    breath_exhale: float = 6.0
    breath_cycles: int = 4

    # --- Window ---
    width: int = 520
    height: int = 670
    fps: int = 60


# ======================================================================================
# Emotiv data handler (runs in a background thread with its own asyncio loop)
# ======================================================================================
# Default 'met' column order, used only if the subscribe response omits the columns.
DEFAULT_MET_COLS = [
    "eng.isActive", "eng", "exc.isActive", "exc", "lex",
    "str.isActive", "str", "rel.isActive", "rel",
    "int.isActive", "int", "foc.isActive", "foc",
]


class CortexError(Exception):
    """Raised when Cortex returns a JSON-RPC error or an unexpected response."""

    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code


# Errors that retrying will not fix (and that we must not hammer Cortex with).
FATAL_CODES = {
    -32021: "Invalid Client ID/Secret. Check the Cortex app and that the Launcher is "
            "signed in to the SAME EmotivID.",
    -32019: "Session limit reached. This account/headset needs a license or session "
            "credit (see the license line in the log).",
}


class EmotivClient(threading.Thread):
    """
    Talks to the Cortex API and pushes events onto a thread-safe queue:
        ("status", "text")           human-readable connection status
        ("streaming",)               subscription succeeded, data is flowing
        ("met", {"str": 0.42, ...})  one sample of mental-command metrics
        ("disconnected",)            connection lost (it will retry automatically)

    The UI never touches the socket; it only reads this queue.
    """

    def __init__(self, cfg: Config, out_q: "queue.Queue[tuple]"):
        super().__init__(daemon=True, name="emotiv-client")
        self.cfg = cfg
        self.out = out_q
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._main_task: Optional[asyncio.Task] = None
        self._stopping = threading.Event()
        self._req_id = 0

    # ----- thread lifecycle ---------------------------------------------------------
    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._main_task = self._loop.create_task(self._run_forever())
        try:
            self._loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            pass
        finally:
            self._loop.close()

    def stop(self) -> None:
        self._stopping.set()
        if self._loop and self._main_task:
            try:
                self._loop.call_soon_threadsafe(self._main_task.cancel)
            except RuntimeError:  # loop already closed
                pass

    # ----- helpers ------------------------------------------------------------------
    def _status(self, text: str) -> None:
        log.info(text)
        self.out.put(("status", text))

    def _ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.cfg.cortex_url.startswith("wss"):
            return None
        if self.cfg.cortex_cert:
            return ssl.create_default_context(cafile=self.cfg.cortex_cert)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False          # Cortex uses a self-signed cert on localhost
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    async def _call(self, ws, method: str, params: Optional[dict] = None, timeout: float = 20.0):
        """Send one JSON-RPC request and wait for the response with the matching id."""
        self._req_id += 1
        rid = self._req_id
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                                  "params": params or {}}))

        async def _wait():
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("id") != rid:
                    continue  # stray stream/warning message; ignore
                if "error" in msg:
                    err = msg["error"]
                    raise CortexError(f"{method}: {err.get('message')} (code {err.get('code')})",
                                      err.get("code"))
                return msg.get("result")

        return await asyncio.wait_for(_wait(), timeout)

    # ----- main reconnect loop ------------------------------------------------------
    async def _run_forever(self) -> None:
        backoff = 2.0
        while not self._stopping.is_set():
            try:
                await self._session()
                backoff = 2.0
            except asyncio.CancelledError:
                raise
            except CortexError as e:
                self._status(f"Cortex error: {e}")
                if e.code in FATAL_CODES:
                    self.out.put(("disconnected",))
                    self._status(f"STOPPED: {FATAL_CODES[e.code]}")
                    return  # no retry loop: it would just repeat the same failure
            except (OSError, WebSocketException, asyncio.TimeoutError) as e:
                self._status(f"Can't reach Cortex ({type(e).__name__}). Is EMOTIV Launcher running?")
            except Exception as e:  # never let the thread die silently
                log.exception("Unexpected Emotiv client error")
                self._status(f"Unexpected error: {e}")
            self.out.put(("disconnected",))
            self._status(f"Retrying in {int(backoff)}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    # ----- one full Cortex session --------------------------------------------------
    async def _session(self) -> None:
        cfg = self.cfg
        self._status("Connecting to Cortex...")
        async with websockets.connect(
            cfg.cortex_url, ssl=self._ssl_context(),
            ping_interval=None, max_size=None, open_timeout=10,
        ) as ws:
            # 1. requestAccess: user must click "Approve" in the Launcher the first time.
            access = {"clientId": cfg.client_id, "clientSecret": cfg.client_secret}
            for attempt in range(24):
                res = await self._call(ws, "requestAccess", access)
                if res.get("accessGranted"):
                    break
                self._status("Waiting for you to approve this app in EMOTIV Launcher...")
                await asyncio.sleep(5)
            else:
                raise CortexError("App was not approved in EMOTIV Launcher")

            # 2. authorize -> cortexToken
            params = dict(access)
            params["debit"] = 1
            if cfg.license_id:
                params["license"] = cfg.license_id
            token = (await self._call(ws, "authorize", params))["cortexToken"]
            self._status("Authorized.")

            # Diagnostic: show what license Cortex associates with this account.
            try:
                info = await self._call(ws, "getLicenseInfo", {"cortexToken": token})
                log.info("License info: %s", json.dumps(info)[:600])
            except CortexError as e:
                log.info("getLicenseInfo failed: %s", e)

            # 3. find (and connect) a headset
            headset = await self._find_headset(ws)

            # 4. create an active session
            session = await self._call(ws, "createSession", {
                "cortexToken": token, "headset": headset, "status": "active"})
            session_id = session["id"]

            # 5. subscribe to the 'met' (performance metrics) stream
            sub = await self._call(ws, "subscribe", {
                "cortexToken": token, "session": session_id, "streams": ["met"]})
            if sub.get("failure"):
                raise CortexError(f"subscribe failed: {sub['failure']}")
            cols = DEFAULT_MET_COLS
            for stream in sub.get("success", []):
                if stream.get("streamName") == "met" and stream.get("cols"):
                    cols = stream["cols"]
            self._status(f"Streaming from {headset}")
            self.out.put(("streaming",))

            # 6. read the stream until the socket closes
            active_flag = f"{cfg.stress_metric}.isActive"
            async for raw in ws:
                msg = json.loads(raw)
                if "met" in msg:
                    row = dict(zip(cols, msg["met"]))
                    if row.get(active_flag) is False:
                        continue  # sensor contact too poor; metric is not valid
                    clean = {k: v for k, v in row.items()
                             if isinstance(v, (int, float)) and not isinstance(v, bool)}
                    if cfg.stress_metric in clean:
                        self.out.put(("met", clean))
                elif "warning" in msg:
                    log.warning("Cortex warning: %s", msg["warning"])
                elif "error" in msg:
                    raise CortexError(str(msg["error"]))

    async def _find_headset(self, ws) -> str:
        """Pick a headset (configured id or first one found) and make sure it's connected."""
        for _ in range(20):
            headsets = await self._call(ws, "queryHeadsets")
            if self.cfg.headset_id:
                headsets = [h for h in headsets if h.get("id") == self.cfg.headset_id]
            if not headsets:
                self._status("No headset found. Turn it on / connect it in the Launcher.")
                await asyncio.sleep(3)
                continue

            h = headsets[0]
            if h.get("status") == "connected":
                return h["id"]
            if h.get("status") == "discovered":
                self._status(f"Connecting to {h['id']}...")
                await self._call(ws, "controlDevice", {"command": "connect", "headset": h["id"]})
            await asyncio.sleep(2)
        raise CortexError("Headset never reached 'connected' state")


# ======================================================================================
# Stress monitor: pure logic, no UI, no networking (easy to unit test)
# ======================================================================================
class StressMonitor:
    """Decides whether stress has been above threshold for long enough to trigger."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.latest: Optional[float] = None
        self.last_sample_ts = 0.0
        self._above_since: Optional[float] = None
        self._above_count = 0
        self._armed_after = 0.0  # monotonic time before which we never trigger

    def push(self, value: float, ts: Optional[float] = None) -> None:
        ts = time.monotonic() if ts is None else ts
        self.latest, self.last_sample_ts = value, ts
        if value >= self.cfg.stress_threshold:
            if self._above_since is None:
                self._above_since, self._above_count = ts, 0
            self._above_count += 1
        else:
            self.reset_streak()

    def reset_streak(self) -> None:
        self._above_since, self._above_count = None, 0

    def hold_off(self, seconds: float) -> None:
        self._armed_after = time.monotonic() + seconds

    def is_stale(self, now: float) -> bool:
        return self.latest is None or now - self.last_sample_ts > self.cfg.stale_timeout

    def streak_seconds(self, now: float) -> float:
        return 0.0 if self._above_since is None else now - self._above_since

    def should_trigger(self, now: float) -> bool:
        if now < self._armed_after or self._above_since is None:
            return False
        if self.is_stale(now):
            self.reset_streak()
            return False
        return (self.streak_seconds(now) >= self.cfg.sustain_seconds
                and self._above_count >= self.cfg.min_samples)


# ======================================================================================
# Breathing mini-game logic (no drawing here)
# ======================================================================================
class BreathingGame:
    """
    A ring grows while you breathe in and shrinks while you breathe out.
    Hold SPACE to make your circle grow, release to let it shrink. Keep your
    circle matched to the ring. Accuracy is the average match over the game.
    """
    R_MIN, R_MAX = 50.0, 140.0

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.reset()

    def reset(self) -> None:
        self.t = 0.0
        self.player_r = self.R_MIN
        self._sync_sum = 0.0
        self.finished = False

    @property
    def cycle_len(self) -> float:
        c = self.cfg
        return c.breath_inhale + c.breath_hold + c.breath_exhale

    @property
    def total_time(self) -> float:
        return self.cycle_len * self.cfg.breath_cycles

    @property
    def cycle_number(self) -> int:
        return min(int(self.t // self.cycle_len) + 1, self.cfg.breath_cycles)

    @property
    def accuracy(self) -> float:
        return 0.0 if self.t <= 0 else self._sync_sum / self.t

    def phase(self) -> tuple[str, float]:
        c = self.cfg
        pos = self.t % self.cycle_len
        if pos < c.breath_inhale:
            return "Breathe in", pos / c.breath_inhale
        pos -= c.breath_inhale
        if pos < c.breath_hold:
            return "Hold", pos / c.breath_hold
        pos -= c.breath_hold
        return "Breathe out", pos / c.breath_exhale

    def target_radius(self) -> float:
        name, p = self.phase()
        frac = p if name == "Breathe in" else 1.0 if name == "Hold" else 1.0 - p
        return self.R_MIN + (self.R_MAX - self.R_MIN) * frac

    def update(self, dt: float, holding: bool) -> None:
        if self.finished:
            return
        span = self.R_MAX - self.R_MIN
        rate = span / (self.cfg.breath_inhale if holding else self.cfg.breath_exhale)
        self.player_r += rate * dt * (1 if holding else -1)
        self.player_r = max(self.R_MIN, min(self.R_MAX, self.player_r))

        error = abs(self.player_r - self.target_radius())
        self._sync_sum += max(0.0, 1.0 - error / (0.4 * span)) * dt
        self.t += dt
        if self.t >= self.total_time:
            self.t = self.total_time
            self.finished = True


# ======================================================================================
# Rendering: the pet and misc. drawing helpers
# ======================================================================================
BG = (30, 34, 48)
WHITE = (245, 245, 250)
GREY = (150, 155, 175)
INK = (40, 40, 55)


class Mode(Enum):
    IDLE = auto()
    STUDYING = auto()
    ALERT = auto()        # pet reacts, timer paused, short transition
    BREATHING = auto()    # mini-game
    BREAK_DONE = auto()   # mini-game finished, waiting for the user


class Mood(Enum):
    IDLE = auto()
    STUDYING = auto()
    STRESSED = auto()
    HAPPY = auto()


BODY_COLORS = {
    Mood.IDLE: (255, 214, 102),
    Mood.STUDYING: (140, 200, 255),
    Mood.STRESSED: (255, 140, 120),
    Mood.HAPPY: (160, 230, 170),
}


def _r(x, y, w, h) -> pygame.Rect:
    return pygame.Rect(int(x), int(y), int(w), int(h))


def _pt(x, y) -> tuple[int, int]:
    return int(x), int(y)


def render_pet(mood: Mood, t: float) -> pygame.Surface:
    """Draw the pet with plain shapes onto a transparent 360x360 surface."""
    size = 360
    surf = pygame.Surface((size, size), pygame.SRCALPHA)
    cx, cy = size // 2, 190
    bob = math.sin(t * 2.2) * 5 if mood != Mood.STRESSED else 0
    shake = math.sin(t * 45) * 4 if mood == Mood.STRESSED else 0
    x, y = cx + shake, cy + bob

    body = BODY_COLORS[mood]
    dark = tuple(int(c * 0.65) for c in body)

    # shadow, ears, body, feet
    pygame.draw.ellipse(surf, (0, 0, 0, 70), _r(cx - 85, cy + 92, 170, 26))
    for sx in (-70, 70):
        pygame.draw.circle(surf, body, _pt(x + sx, y - 70), 28)
        pygame.draw.circle(surf, dark, _pt(x + sx, y - 70), 28, 3)
    pygame.draw.ellipse(surf, body, _r(x - 95, y - 85, 190, 175))
    pygame.draw.ellipse(surf, dark, _r(x - 95, y - 85, 190, 175), 4)
    for fx in (-50, 10):
        pygame.draw.ellipse(surf, dark, _r(x + fx, y + 80, 40, 20))

    # eyes
    blink = (t % 4.0) < 0.12
    for side in (-1, 1):
        ex, ey = x + side * 35, y - 20
        if mood == Mood.HAPPY:
            pygame.draw.arc(surf, INK, _r(ex - 14, ey - 4, 28, 26), 0.15 * math.pi, 0.85 * math.pi, 4)
        elif blink and mood != Mood.STRESSED:
            pygame.draw.line(surf, INK, _pt(ex - 13, ey), _pt(ex + 13, ey), 4)
        elif mood == Mood.STRESSED:
            pygame.draw.circle(surf, WHITE, _pt(ex, ey), 19)
            pygame.draw.circle(surf, INK, _pt(ex, ey), 19, 2)
            jitter = math.sin(t * 30 + side) * 3
            pygame.draw.circle(surf, INK, _pt(ex + jitter, ey), 4)
            # worried eyebrows: inner ends raised
            inner, outer = (ex - side * 14, ey - 36), (ex + side * 16, ey - 26)
            pygame.draw.line(surf, INK, _pt(*inner), _pt(*outer), 4)
        else:
            look_down = 6 if mood == Mood.STUDYING else 0
            look_x = math.sin(t * 0.9) * 3 if mood == Mood.IDLE else 0
            pygame.draw.circle(surf, WHITE, _pt(ex, ey), 15)
            pygame.draw.circle(surf, INK, _pt(ex + look_x, ey + look_down), 7)
            pygame.draw.circle(surf, WHITE, _pt(ex + look_x + 2, ey + look_down - 2), 2)

    if mood == Mood.STUDYING:  # glasses
        for side in (-1, 1):
            pygame.draw.circle(surf, INK, _pt(x + side * 35, y - 20), 23, 3)
        pygame.draw.line(surf, INK, _pt(x - 12, y - 20), _pt(x + 12, y - 20), 3)

    # cheeks
    if mood in (Mood.IDLE, Mood.HAPPY):
        for side in (-1, 1):
            pygame.draw.circle(surf, (255, 150, 160), _pt(x + side * 62, y + 12), 9)

    # mouth
    if mood == Mood.IDLE:
        pygame.draw.arc(surf, INK, _r(x - 22, y + 4, 44, 34), 1.15 * math.pi, 1.85 * math.pi, 4)
    elif mood == Mood.HAPPY:
        pygame.draw.arc(surf, INK, _r(x - 30, y - 2, 60, 50), 1.1 * math.pi, 1.9 * math.pi, 4)
    elif mood == Mood.STUDYING:
        pygame.draw.line(surf, INK, _pt(x - 11, y + 28), _pt(x + 11, y + 28), 4)
    else:  # stressed: zig-zag mouth
        pts = [(x - 28 + i * 14, y + 32 + (6 if i % 2 else -6)) for i in range(5)]
        pygame.draw.lines(surf, INK, False, [_pt(*p) for p in pts], 4)

    # book while studying
    if mood == Mood.STUDYING:
        page = (245, 240, 225)
        left = [(x - 80, y + 52), (x, y + 64), (x, y + 122), (x - 80, y + 110)]
        right = [(x + 80, y + 52), (x, y + 64), (x, y + 122), (x + 80, y + 110)]
        for poly in (left, right):
            pygame.draw.polygon(surf, page, [_pt(*p) for p in poly])
            pygame.draw.polygon(surf, INK, [_pt(*p) for p in poly], 2)
        for i in range(3):  # text lines
            off = 78 + i * 10
            pygame.draw.line(surf, GREY, _pt(x - 65, y + off - 14), _pt(x - 12, y + off - 8), 2)
            pygame.draw.line(surf, GREY, _pt(x + 65, y + off - 14), _pt(x + 12, y + off - 8), 2)

    # sweat drop while stressed
    if mood == Mood.STRESSED:
        drop_y = y - 70 + (t * 70) % 40
        pygame.draw.circle(surf, (120, 200, 255), _pt(x + 88, drop_y + 8), 8)
        pygame.draw.polygon(surf, (120, 200, 255),
                            [_pt(x + 88, drop_y - 12), _pt(x + 80, drop_y + 4), _pt(x + 96, drop_y + 4)])

    return surf


def lerp_color(a, b, f):
    f = max(0.0, min(1.0, f))
    return tuple(int(a[i] + (b[i] - a[i]) * f) for i in range(3))


def fmt_time(seconds: float) -> str:
    return f"{int(seconds // 60):02d}:{int(seconds % 60):02d}"


# ======================================================================================
# The application: UI + glue between the monitor, the client and the game
# ======================================================================================
class StudyTamagotchi:
    def __init__(self, cfg: Config, simulate: bool):
        self.cfg = cfg
        self.simulate = simulate

        pygame.init()
        pygame.display.set_caption("Study Tamagotchi")
        self.screen = pygame.display.set_mode((cfg.width, cfg.height))
        self.clock = pygame.time.Clock()
        self.f_big = pygame.font.Font(None, 60)
        self.f_med = pygame.font.Font(None, 32)
        self.f_small = pygame.font.Font(None, 22)

        self.monitor = StressMonitor(cfg)
        self.game = BreathingGame(cfg)
        self.events: "queue.Queue[tuple]" = queue.Queue()
        self.client = None if simulate else EmotivClient(cfg, self.events)

        self.mode = Mode.IDLE
        self.study_seconds = 0.0
        self.breaks_taken = 0
        self.alert_t = 0.0
        self.last_result = ""
        self.metrics: dict[str, float] = {}
        self.status = "Simulation mode (no headset)" if simulate else "Starting..."
        self.live = simulate
        self.running = True
        self.t = 0.0

        # simulation state
        self.sim_target = 0.3
        self.sim_level = 0.3
        self._sim_timer = 0.0

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        if self.client:
            self.client.start()
        try:
            while self.running:
                dt = self.clock.tick(self.cfg.fps) / 1000.0
                self.t += dt
                self.handle_input()
                self.drain_events()
                self.update(dt)
                self.draw()
        finally:
            if self.client:
                self.client.stop()
            pygame.quit()

    # ------------------------------------------------------------------ input
    def handle_input(self) -> None:
        for ev in pygame.event.get():
            if ev.type == pygame.QUIT:
                self.running = False
            elif ev.type == pygame.KEYDOWN:
                self.on_key(ev.key)

    def on_key(self, key: int) -> None:
        cfg = self.cfg
        if key == pygame.K_LEFTBRACKET:
            cfg.stress_threshold = max(0.05, round(cfg.stress_threshold - 0.05, 2))
        elif key == pygame.K_RIGHTBRACKET:
            cfg.stress_threshold = min(0.95, round(cfg.stress_threshold + 0.05, 2))
        elif key == pygame.K_r:
            self.study_seconds = 0.0
        elif key == pygame.K_t and self.mode == Mode.STUDYING:
            self.trigger_stress_break()
        elif key == pygame.K_ESCAPE:
            self.mode = Mode.IDLE
            self.monitor.reset_streak()
        elif key == pygame.K_UP and self.simulate:
            self.sim_target = min(1.0, self.sim_target + 0.1)
        elif key == pygame.K_DOWN and self.simulate:
            self.sim_target = max(0.0, self.sim_target - 0.1)
        elif key == pygame.K_SPACE:
            if self.mode == Mode.IDLE:
                self.start_studying()
            elif self.mode == Mode.STUDYING:
                self.mode = Mode.IDLE
        elif key in (pygame.K_RETURN, pygame.K_KP_ENTER) and self.mode == Mode.BREAK_DONE:
            self.start_studying()
            self.monitor.hold_off(cfg.grace_after_break)

    # ------------------------------------------------------------------ state changes
    def start_studying(self) -> None:
        self.mode = Mode.STUDYING
        self.monitor.reset_streak()

    def trigger_stress_break(self) -> None:
        """Stress was sustained: pause the tracker and have the pet react."""
        log.info("Stress threshold exceeded for %.0fs: pausing study", self.cfg.sustain_seconds)
        self.mode = Mode.ALERT
        self.alert_t = 0.0
        self.breaks_taken += 1
        self.monitor.reset_streak()

    def start_breathing(self) -> None:
        self.game.reset()
        self.mode = Mode.BREATHING

    # ------------------------------------------------------------------ data in
    def drain_events(self) -> None:
        """Move everything the Emotiv thread produced into app state."""
        while True:
            try:
                ev = self.events.get_nowait()
            except queue.Empty:
                return
            kind = ev[0]
            if kind == "status":
                self.status = ev[1]
            elif kind == "streaming":
                self.live = True
            elif kind == "disconnected":
                self.live = False
                self.monitor.reset_streak()
            elif kind == "met":
                self.metrics = ev[1]
                self.monitor.push(ev[1][self.cfg.stress_metric])

    def simulate_samples(self, dt: float) -> None:
        """Feed fake 1 Hz stress samples so the app can be tried without a headset."""
        self._sim_timer += dt
        if self._sim_timer >= 1.0:
            self._sim_timer = 0.0
            self.sim_level += (self.sim_target - self.sim_level) * 0.4
            value = max(0.0, min(1.0, self.sim_level + random.gauss(0, 0.03)))
            self.metrics = {self.cfg.stress_metric: value}
            self.monitor.push(value)

    # ------------------------------------------------------------------ update
    def update(self, dt: float) -> None:
        if self.simulate:
            self.simulate_samples(dt)
        now = time.monotonic()

        if self.mode == Mode.STUDYING:
            self.study_seconds += dt
            if self.monitor.should_trigger(now):
                self.trigger_stress_break()
        elif self.mode == Mode.ALERT:  # study timer intentionally NOT advancing
            self.alert_t += dt
            if self.alert_t >= self.cfg.alert_seconds:
                self.start_breathing()
        elif self.mode == Mode.BREATHING:
            holding = bool(pygame.key.get_pressed()[pygame.K_SPACE])
            self.game.update(dt, holding)
            if self.game.finished:
                acc = self.game.accuracy
                self.last_result = ("Perfectly in sync!" if acc >= 0.8 else
                                    "Nice rhythm!" if acc >= 0.55 else "Good effort, every breath helps.")
                self.mode = Mode.BREAK_DONE

    # ------------------------------------------------------------------ drawing
    def text(self, s, font, color, pos, anchor="midtop") -> None:
        img = font.render(s, True, color)
        self.screen.blit(img, img.get_rect(**{anchor: pos}))

    def current_mood(self) -> Mood:
        return {Mode.IDLE: Mood.IDLE, Mode.STUDYING: Mood.STUDYING, Mode.ALERT: Mood.STRESSED,
                Mode.BREATHING: Mood.IDLE, Mode.BREAK_DONE: Mood.HAPPY}[self.mode]

    def draw(self) -> None:
        s, w = self.screen, self.cfg.width
        s.fill(BG)
        self.draw_hud()

        breathing = self.mode == Mode.BREATHING
        pet = render_pet(self.current_mood(), self.t)
        if breathing:
            pet = pygame.transform.smoothscale(pet, (162, 162))
            s.blit(pet, pet.get_rect(center=(w // 2, 165)))
            self.draw_breathing()
        else:
            s.blit(pet, pet.get_rect(center=(w // 2, 330)))
            self.draw_caption()

        if self.mode == Mode.ALERT:
            overlay = pygame.Surface((w, self.cfg.height), pygame.SRCALPHA)
            overlay.fill((255, 60, 60, int(40 + 30 * math.sin(self.t * 8))))
            s.blit(overlay, (0, 0))

        hints = "SPACE start/stop   ESC end   [ ] threshold   R reset timer"
        if self.simulate:
            hints += "   UP/DOWN fake stress"
        self.text(hints, self.f_small, GREY, (w // 2, self.cfg.height - 28))
        pygame.display.flip()

    def draw_hud(self) -> None:
        s, cfg, w = self.screen, self.cfg, self.cfg.width
        now = time.monotonic()
        bar = pygame.Rect(20, 34, 240, 16)

        self.text("STRESS", self.f_small, GREY, (20, 12), "topleft")
        pygame.draw.rect(s, (55, 60, 80), bar, border_radius=8)
        val = self.monitor.latest
        stale = self.monitor.is_stale(now) and not self.simulate
        if val is not None and not stale:
            fill = bar.copy()
            fill.width = int(bar.width * val)
            pygame.draw.rect(s, lerp_color((90, 200, 120), (230, 80, 80), val), fill, border_radius=8)
        tx = bar.x + int(bar.width * cfg.stress_threshold)
        pygame.draw.line(s, WHITE, (tx, bar.y - 4), (tx, bar.bottom + 4), 2)
        label = "--" if (val is None or stale) else f"{val:.2f}"
        self.text(f"{label} / limit {cfg.stress_threshold:.2f}", self.f_small, WHITE,
                  (bar.right + 10, bar.y), "topleft")

        # sustained-stress progress (fills while above threshold)
        frac = min(1.0, self.monitor.streak_seconds(now) / cfg.sustain_seconds)
        pygame.draw.rect(s, (55, 60, 80), (20, 56, 240, 5), border_radius=3)
        if frac > 0 and self.mode == Mode.STUDYING:
            pygame.draw.rect(s, (255, 170, 60), (20, 56, int(240 * frac), 5), border_radius=3)

        self.text(self.status, self.f_small, (110, 200, 130) if self.live else (230, 150, 90),
                  (20, 70), "topleft")

        self.text("STUDY TIME", self.f_small, GREY, (w - 20, 12), "topright")
        self.text(fmt_time(self.study_seconds), self.f_big, WHITE, (w - 20, 28), "topright")
        extras = [f"{k} {self.metrics[k]:.2f}" for k in ("foc", "rel") if k in self.metrics]
        if extras:
            self.text("  ".join(extras), self.f_small, GREY, (w - 20, 84), "topright")

    def draw_caption(self) -> None:
        w, y = self.cfg.width // 2, 520
        if self.mode == Mode.IDLE:
            self.text("Press SPACE to start studying", self.f_med, WHITE, (w, y))
            if self.breaks_taken:
                self.text(f"Calm breaks taken: {self.breaks_taken}", self.f_small, GREY, (w, y + 36))
        elif self.mode == Mode.STUDYING:
            self.text("Studying... stay focused!", self.f_med, WHITE, (w, y))
        elif self.mode == Mode.ALERT:
            self.text("Whoa, stress is high!", self.f_med, (255, 200, 190), (w, y))
            self.text("Study timer paused. Let's breathe.", self.f_small, WHITE, (w, y + 36))
        elif self.mode == Mode.BREAK_DONE:
            self.text(self.last_result, self.f_med, WHITE, (w, y))
            self.text(f"Breathing accuracy: {self.game.accuracy * 100:.0f}%", self.f_small, GREY, (w, y + 34))
            self.text("ENTER to resume studying   ESC to stop", self.f_small, WHITE, (w, y + 60))

    def draw_breathing(self) -> None:
        s, g = self.screen, self.game
        center = (self.cfg.width // 2, 420)
        target = int(g.target_radius())

        pygame.draw.circle(s, (90, 170, 255), center, int(g.player_r))      # you
        pygame.draw.circle(s, WHITE, center, target, 3)                      # guide ring
        pygame.draw.circle(s, (70, 80, 110), center, int(g.R_MAX) + 6, 1)    # max boundary

        name, _ = g.phase()
        self.text(name, self.f_big, WHITE, (center[0], 575))
        self.text(f"Hold SPACE to inhale, release to exhale   |   "
                  f"cycle {g.cycle_number}/{self.cfg.breath_cycles}   |   sync {g.accuracy * 100:.0f}%",
                  self.f_small, GREY, (center[0], 612))


# ======================================================================================
# Entry point
# ======================================================================================
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Study Tamagotchi (Emotiv-powered study buddy)")
    p.add_argument("--simulate", action="store_true", help="no headset: fake stress via UP/DOWN keys")
    p.add_argument("--threshold", type=float, help="stress threshold 0..1 (default 0.70)")
    p.add_argument("--sustain", type=float, help="seconds above threshold before triggering (default 30)")
    p.add_argument("--metric", help="met column to watch (default 'str')")
    p.add_argument("--url", help="Cortex WebSocket URL (default wss://localhost:6868)")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Config()
    if args.threshold is not None:
        cfg.stress_threshold = max(0.0, min(1.0, args.threshold))
    if args.sustain is not None:
        cfg.sustain_seconds = max(1.0, args.sustain)
    if args.metric:
        cfg.stress_metric = args.metric
    if args.url:
        cfg.cortex_url = args.url

    if not args.simulate:
        if websockets is None:
            print("Missing dependency: pip install websockets   (or run with --simulate)")
            return 1
        if not (cfg.client_id and cfg.client_secret):
            print("Set EMOTIV_CLIENT_ID and EMOTIV_CLIENT_SECRET (see the setup notes at the top\n"
                  "of this file), or run with --simulate to try the app without a headset.")
            return 1

    StudyTamagotchi(cfg, simulate=args.simulate).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
