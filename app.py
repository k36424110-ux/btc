import asyncio, json, os, statistics, time
from collections import deque
from aiohttp import web, ClientSession, WSMsgType

FEE = float(os.getenv("FEE", "0.0004"))        # frais par côté (0,04 %)
SLIP = float(os.getenv("SLIP", "1.0"))         # slippage en $ par côté
LIQ_MIN = float(os.getenv("LIQ_MIN", "3"))     # BTC liquidés en 60 s = niveau
MAXCOST = float(os.getenv("MAXCOST", "0.25"))  # coûts max = 25 % du gain visé
RR, MAXHOLD, COOLDOWN = 2.0, 300, 120
CN = ["Niveau", "Épuisement", "Retournement", "Accord exchanges", "OI en baisse", "Carnet"]
now = time.time


class X:
    def __init__(s): s.tr, s.ts = deque(), 0.0
    def delta(s, w): t = now() - w; return sum(q for ts, q in s.tr if ts >= t)


ex = {k: X() for k in ("Binance", "Bybit", "OKX")}
B = {"bid": 0.0, "ask": 0.0, "bd": 0.0, "ad": 0.0, "ts": 0.0}   # carnet Binance = référence
liq, oi, mids = deque(), deque(), deque()
bars, dsamp = deque(maxlen=60), deque(maxlen=120)
trades, signals = [], []
state = {"L": None, "S": None}
open_t, last_sig, DB = None, 0.0, None


async def tg(text):
    tok, chat = os.getenv("TG_TOKEN"), os.getenv("TG_CHAT")
    if tok and chat:
        try:
            async with ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{tok}/sendMessage", json={"chat_id": chat, "text": text})
        except Exception:
            pass


async def db_init():
    global DB
    url = os.getenv("DATABASE_URL")
    if not url:
        return
    import asyncpg
    DB = await asyncpg.create_pool(url, min_size=1, max_size=2, statement_cache_size=0)
    await DB.execute("create table if not exists ev(id bigserial primary key, kind text, d jsonb)")
    for r in await DB.fetch("select kind, d from ev order by id"):
        (trades if r["kind"] == "trade" else signals).append(json.loads(r["d"]))


async def save(kind, d):
    if DB:
        try:
            await DB.execute("insert into ev(kind, d) values($1, $2)", kind, json.dumps(d))
        except Exception as e:
            print("db:", e, flush=True)


def on_trade(name, q):
    e, t = ex[name], now()
    e.tr.append((t, q)); e.ts = t
    while e.tr and e.tr[0][0] < t - 60: e.tr.popleft()


def check(side, t):
    sg = 1 if side == "L" else -1
    mid, spread = (B["bid"] + B["ask"]) / 2, B["ask"] - B["bid"]
    atr = statistics.mean(h - l for h, l in bars) or 1.0
    typ = statistics.mean(abs(d) for d in dsamp) or 1e-9
    d30 = sum(e.delta(30) for e in ex.values()) * sg
    p30 = next((m for ts, m in mids if ts >= t - 30), mid)
    rec = [m for ts, m in mids if ts >= t - 10]
    far = [m for ts, m in mids if ts >= t - 600]
    near = (mid - min(far)) if sg == 1 else (max(far) - mid)
    bounce = (mid - min(rec)) if sg == 1 else (max(rec) - mid)
    lq = sum(q for ts, s_, q in liq if s_ == side and ts >= t - 60)
    pts = [v for ts, v in oi if ts >= t - 75]
    c = [bool(near <= 1.5 * atr or lq >= LIQ_MIN),
         bool(d30 < -1.5 * typ and (mid - p30) * sg > -0.5 * atr),
         bool(sum(e.delta(5) for e in ex.values()) * sg > 0 and bounce > 0.2 * atr),
         bool(sum(e.delta(5) * sg > 0 for e in ex.values()) >= 2),
         bool(len(pts) >= 8 and pts[-1] - pts[0] <= 0),
         bool(B["bd"] > 1.2 * B["ad"] if sg == 1 else B["ad"] > 1.2 * B["bd"])]
    ok = bool(spread <= 0.0002 * mid and atr >= 4 * spread and sum(t - e.ts < 5 for e in ex.values()) >= 2)
    return {"c": c, "ok": ok, "atr": atr}


def maybe_enter(t):
    global open_t, last_sig
    if open_t or t - last_sig < COOLDOWN:
        return
    for side in "LS":
        r = state[side]
        c = r["c"]
        if not (c[0] and c[1] and c[2] and sum(c) >= 5 and r["ok"]):
            continue
        last_sig = t
        sg = 1 if side == "L" else -1
        entry = B["ask"] + SLIP if sg == 1 else B["bid"] - SLIP
        rec = [m for ts, m in mids if ts >= t - 60]
        risk = max(abs(entry - (min(rec) if sg == 1 else max(rec))) + 0.5 * r["atr"], 2 * r["atr"])
        cost = entry * FEE * 2 + SLIP
        sig = {"t": int(t), "side": side, "c": c, "entry": round(entry, 1), "sl": round(entry - sg * risk, 1),
               "tp": round(entry + sg * RR * risk, 1), "cost": round(cost, 1),
               "taken": cost <= MAXCOST * RR * risk, "why": ""}
        if not sig["taken"]:
            sig["why"] = "coûts trop élevés"
        signals.append(sig)
        asyncio.create_task(save("signal", sig))
        if sig["taken"]:
            open_t = {**sig, "risk": risk, "h": time.gmtime(t).tm_hour, "ts": t}
            asyncio.create_task(tg(f"{'LONG' if sg == 1 else 'SHORT'} BTC ({sum(c)}/6)\nEntrée {sig['entry']}\nSL {sig['sl']}\nTP {sig['tp']}"))
        return


def manage(t):
    global open_t
    o = open_t
    if not o:
        return
    sg = 1 if o["side"] == "L" else -1
    px = B["bid"] if sg == 1 else B["ask"]            # prix de sortie réellement exécutable
    hit = "TP" if (px - o["tp"]) * sg >= 0 else "SL" if (px - o["sl"]) * sg <= 0 else None
    if not hit and t - o["ts"] > MAXHOLD:
        hit = "TIME"
    if not hit:
        return
    exit_p = (o["tp"] if hit == "TP" else px) - sg * SLIP
    net = (exit_p - o["entry"]) * sg - o["entry"] * FEE - exit_p * FEE
    rec = {"t": o["t"], "side": o["side"], "c": o["c"], "h": o["h"], "entry": o["entry"], "exit": round(exit_p, 1),
           "why": hit, "net": round(net, 1), "R": round(net / o["risk"], 2), "dur": int(t - o["ts"])}
    trades.append(rec)
    open_t = None
    asyncio.create_task(save("trade", rec))
    asyncio.create_task(tg(f"Sortie {hit} : {rec['R']} R"))


async def engine():
    n = 0
    while True:
        await asyncio.sleep(1)
        n, t = n + 1, now()
        for q in (liq, mids):
            while q and q[0][0] < t - (900 if q is mids else 120): q.popleft()
        if not (B["bid"] and t - B["ts"] < 3):
            continue
        mid = (B["bid"] + B["ask"]) / 2
        mids.append((t, mid))
        if n % 10 == 0:
            w = [m for ts, m in mids if ts >= t - 10]
            bars.append((max(w), min(w)))
        if n % 5 == 0:
            dsamp.append(sum(e.delta(30) for e in ex.values()))
        manage(t)
        if len(bars) >= 20 and len(dsamp) >= 20:
            state["L"], state["S"] = check("L", t), check("S", t)
            maybe_enter(t)


async def binance(s):
    url = "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@depth10@100ms/btcusdt@forceOrder"
    async with s.ws_connect(url, heartbeat=20) as ws:
        async for m in ws:
            if m.type != WSMsgType.TEXT: continue
            d = json.loads(m.data); k, d = d.get("stream", ""), d["data"]
            if k.endswith("aggTrade"):
                on_trade("Binance", -float(d["q"]) if d["m"] else float(d["q"]))
            elif "depth" in k:
                b, a = d.get("b") or d.get("bids"), d.get("a") or d.get("asks")
                if b and a:
                    B.update(bid=float(b[0][0]), ask=float(a[0][0]), ts=now(),
                             bd=sum(float(x[1]) for x in b), ad=sum(float(x[1]) for x in a))
            elif k.endswith("forceOrder"):
                o = d["o"]; liq.append((now(), "L" if o["S"] == "SELL" else "S", float(o["q"])))


async def bybit(s):
    async with s.ws_connect("wss://stream.bybit.com/v5/public/linear", heartbeat=20) as ws:
        await ws.send_json({"op": "subscribe", "args": ["publicTrade.BTCUSDT", "allLiquidation.BTCUSDT"]})
        async for m in ws:
            if m.type != WSMsgType.TEXT: continue
            d = json.loads(m.data); tp = d.get("topic", "")
            for t in d["data"] if isinstance(d.get("data"), list) else []:
                if tp.startswith("publicTrade"):
                    on_trade("Bybit", float(t["v"]) * (1 if t["S"] == "Buy" else -1))
                elif tp.startswith("allLiquidation"):
                    liq.append((now(), "L" if t["S"] == "Buy" else "S", float(t["v"])))


async def okx(s):
    async with s.ws_connect("wss://ws.okx.com:8443/ws/v5/public", heartbeat=20) as ws:
        await ws.send_json({"op": "subscribe", "args": [{"channel": "trades", "instId": "BTC-USDT-SWAP"}]})
        async for m in ws:
            if m.type == WSMsgType.TEXT and '"trades"' in m.data:
                for t in json.loads(m.data).get("data", []):
                    on_trade("OKX", float(t["sz"]) * 0.01 * (1 if t["side"] == "buy" else -1))


async def oi_poll(s):
    while True:
        async with s.get("https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT") as r:
            oi.append((now(), float((await r.json())["openInterest"])))
        while oi and oi[0][0] < now() - 900: oi.popleft()
        await asyncio.sleep(5)


async def keep(fn):
    while True:
        try:
            async with ClientSession() as s:
                await fn(s)
        except Exception as e:
            print(fn.__name__, "erreur:", e, flush=True)
        await asyncio.sleep(5)


def agg(ts):
    n = len(ts)
    return {"n": n, "win": round(100 * sum(x["net"] > 0 for x in ts) / n) if n else None,
            "avgR": round(sum(x["R"] for x in ts) / n, 2) if n else None}


def snap():
    st = {"all": agg(trades), "skipped": sum(not s["taken"] for s in signals),
          "h": {h: agg([x for x in trades if x["h"] == h]) for h in range(24)},
          "c": {n: agg([x for x in trades if x["c"][i]]) for i, n in enumerate(CN)}}
    return {"bid": B["bid"], "ask": B["ask"], "cn": CN, "db": bool(DB), "L": state["L"], "S": state["S"],
            "warm": len(bars) < 20 or len(dsamp) < 20, "open": open_t, "stats": st, "trades": trades[-30:][::-1],
            "ex": {k: {"d5": round(e.delta(5), 2), "live": now() - e.ts < 5} for k, e in ex.items()}}


clients = set()


async def pusher():
    while True:
        await asyncio.sleep(1)
        if clients:
            msg = json.dumps(snap())
            for c in list(clients):
                try: await c.send_str(msg)
                except Exception: clients.discard(c)


async def ws_h(r):
    ws = web.WebSocketResponse(); await ws.prepare(r); clients.add(ws)
    try:
        async for _ in ws: pass
    finally:
        clients.discard(ws)
    return ws


async def start(app):
    await db_init()
    app["tasks"] = [asyncio.create_task(keep(f)) for f in (binance, bybit, okx, oi_poll)] + \
                   [asyncio.create_task(engine()), asyncio.create_task(pusher())]


def make_app():
    app = web.Application()
    app.add_routes([web.get("/", lambda r: web.FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))),
                    web.get("/ws", ws_h), web.get("/health", lambda r: web.Response(text="ok")),
                    web.get("/export", lambda r: web.json_response({"trades": trades, "signals": signals}))])
    app.on_startup.append(start)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), port=int(os.getenv("PORT", 8000)))
