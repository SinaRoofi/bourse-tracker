"""
مقایسه‌ی requests و curl_cffi (impersonate=chrome) روی همون endpointهای پروژه.
فقط برای تست؛ هیچ‌جای پروژه ازش استفاده نمی‌شه و چیزی نمی‌نویسه/نمی‌فرسته.

ENV:
  API_BASE_URL  (الزامی)  - چاپ نمی‌شه؛ فقط slug درخواست‌ها لاگ می‌شه
  WORKERS       (پیش‌فرض 12)
  ROUNDS        (پیش‌فرض 2)  - هر دور: هر دو کلاینت، با ترتیب متناوب
  TIMEOUT       (پیش‌فرض 10) - مثل پروژه
  CSV_URL       (اختیاری)
"""
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import FUND_TYPES, INDUSTRY_CODES  # noqa: E402

BASE = (os.getenv("API_BASE_URL") or "").rstrip("/")
WORKERS = int(os.getenv("WORKERS", "12"))
ROUNDS = int(os.getenv("ROUNDS", "2"))
TIMEOUT = float(os.getenv("TIMEOUT", "10"))
CSV_URL = os.getenv("CSV_URL", "https://tradersarena.ir/data/industries-csv")
SNAPSHOT_TIMEFRAME = 12
HINT_HEADERS = ("server", "cf-ray", "cf-mitigated", "retry-after", "x-ratelimit-remaining")

if not BASE:
    sys.exit("API_BASE_URL تنظیم نشده")


# ----------------------------------------------------------------------
# targets
# ----------------------------------------------------------------------
def build_targets():
    t = []
    for code in INDUSTRY_CODES:
        t.append((f"industry {code}", f"{BASE}/data/industries/{code}/snapshot", "api"))
    for cfg in FUND_TYPES.values():
        if cfg.get("enabled", True):
            t.append((f"fund {cfg['slug']}", f"{BASE}/data/industries/{cfg['slug']}/snapshot", "api"))
    t.append(("industries-csv", CSV_URL, "csv"))
    return t


# ----------------------------------------------------------------------
# clients
# ----------------------------------------------------------------------
class RequestsClient:
    name = "requests"

    def __init__(self):
        # مثل پروژه: یک session مشترک بین threadها با pool به اندازه‌ی WORKERS
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"})
        ad = requests.adapters.HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS)
        self.s.mount("https://", ad)
        self.s.mount("http://", ad)

    def get(self, url, params):
        return self.s.get(url, params=params, timeout=TIMEOUT)


class CurlCffiClient:
    name = "curl_cffi"

    def __init__(self):
        from curl_cffi import requests as cr  # noqa
        self.cr = cr
        self.local = threading.local()  # به‌جای فرض thread-safe بودن، هر thread سشن خودش

    def _session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            try:
                s = self.cr.Session(impersonate="chrome")
            except Exception:
                s = self.cr.Session(impersonate="chrome120")
            s.headers.update({"Accept": "application/json, text/plain, */*"})
            self.local.s = s
        return s

    def get(self, url, params):
        return self._session().get(url, params=params, timeout=TIMEOUT)


# ----------------------------------------------------------------------
# یک درخواست
# ----------------------------------------------------------------------
def one(client, target):
    label, url, kind = target
    params = {"_": int(time.time() * 1000)}
    if kind == "api":
        params["timeframe"] = SNAPSHOT_TIMEFRAME
    t0 = time.monotonic()
    r = {"label": label, "client": client.name, "status": None, "secs": None,
         "ok": False, "err": None, "hints": {}, "head": ""}
    try:
        resp = client.get(url, params)
        r["secs"] = time.monotonic() - t0
        r["status"] = resp.status_code
        for h in HINT_HEADERS:
            v = resp.headers.get(h)
            if v:
                r["hints"][h] = v
        text = resp.text or ""
        if resp.status_code == 200:
            if kind == "api":
                try:
                    rows = resp.json().get("rows")
                    r["ok"] = isinstance(rows, list) and len(rows) > 0
                    if not r["ok"]:
                        r["err"] = "empty_rows"
                except Exception:
                    r["err"] = "not_json"
            else:
                r["ok"] = len(text) > 0 and not text.lstrip().startswith("<")
                if not r["ok"]:
                    r["err"] = "not_csv"
        else:
            r["err"] = f"http_{resp.status_code}"
        if not r["ok"]:
            r["head"] = " ".join(text[:150].split())
    except Exception as e:  # نوع خطا فقط؛ متن خطا ممکنه URL داشته باشه
        r["secs"] = time.monotonic() - t0
        name = type(e).__name__
        r["err"] = "timeout" if "Timeout" in name else name
    return r


# ----------------------------------------------------------------------
# فازها
# ----------------------------------------------------------------------
def probe(clients, targets):
    """چند درخواست پشت‌سرهم و متناوب بین کلاینت‌ها (بدون فشار همزمانی)."""
    sample = targets[:4] + targets[-3:]
    print("\n=== PROBE (sequential, interleaved) ===")
    out = []
    for tg in sample:
        for c in clients:
            r = one(c, tg)
            out.append(r)
            print(f"{c.name:10s} {r['label']:28s} status={r['status']} "
                  f"{(r['secs'] or 0):5.2f}s ok={r['ok']} err={r['err']} {r['hints'] or ''}")
            if r["head"]:
                print(f"           body: {r['head']}")
            time.sleep(0.3)
    return out


def load(client, targets):
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        res = list(ex.map(lambda tg: one(client, tg), targets))
    return res, time.monotonic() - t0


def summarize(name, rounds):
    allr = [r for res, _ in rounds for r in res]
    ok = [r for r in allr if r["ok"]]
    lat = sorted(r["secs"] for r in ok if r["secs"] is not None)
    errs = {}
    for r in allr:
        if not r["ok"]:
            errs[r["err"]] = errs.get(r["err"], 0) + 1
    hints = {}
    for r in allr:
        for k, v in r["hints"].items():
            hints.setdefault(k, set()).add(v)
    def pct(p):
        return lat[min(len(lat) - 1, int(len(lat) * p))] if lat else float("nan")
    return {
        "client": name, "total": len(allr), "ok": len(ok),
        "rate": 100 * len(ok) / len(allr) if allr else 0,
        "p50": statistics.median(lat) if lat else float("nan"),
        "p95": pct(0.95), "max": lat[-1] if lat else float("nan"),
        "wall": [round(w, 1) for _, w in rounds], "errs": errs,
        "hints": {k: sorted(v)[:3] for k, v in hints.items()},
    }


def main():
    targets = build_targets()
    clients = [RequestsClient()]
    try:
        clients.append(CurlCffiClient())
    except Exception as e:
        print(f"curl_cffi در دسترس نیست ({type(e).__name__}) - فقط requests تست می‌شه")

    print(f"targets={len(targets)} workers={WORKERS} rounds={ROUNDS} timeout={TIMEOUT}s")
    probe(clients, targets)

    per_client = {c.name: [] for c in clients}
    for rnd in range(ROUNDS):
        order = clients if rnd % 2 == 0 else list(reversed(clients))
        for c in order:
            print(f"\n=== LOAD round {rnd + 1}/{ROUNDS}: {c.name} ({WORKERS} workers) ===")
            res, wall = load(c, targets)
            per_client[c.name].append((res, wall))
            n_ok = sum(1 for r in res if r["ok"])
            print(f"ok={n_ok}/{len(res)} wall={wall:.1f}s")
            time.sleep(5)

    summaries = [summarize(n, rr) for n, rr in per_client.items()]
    lines = ["| client | ok | success% | p50 | p95 | max | wall per round (s) | errors |",
             "|---|---|---|---|---|---|---|---|"]
    for s in summaries:
        lines.append(f"| {s['client']} | {s['ok']}/{s['total']} | {s['rate']:.0f} | {s['p50']:.2f} | "
                     f"{s['p95']:.2f} | {s['max']:.2f} | {s['wall']} | {s['errs'] or '-'} |")
    table = "\n".join(lines)
    print("\n=== SUMMARY ===\n" + table)
    for s in summaries:
        print(f"{s['client']} headers: {s['hints'] or '-'}")

    path = os.getenv("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"### requests vs curl_cffi (workers={WORKERS}, rounds={ROUNDS})\n\n{table}\n")


if __name__ == "__main__":
    main()
