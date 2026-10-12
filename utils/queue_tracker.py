"""
ردیاب صف‌های سنگین (فیلتر ۱۷: عرضه صف خرید، فیلتر ۱۸: جمع‌آوری صف فروش)

این دو هشدار برخلاف بقیه‌ی فیلترها «حافظه» لازم دارن (مقایسه‌ی وضعیت فعلی با
اجرای قبلی)، پس بیرون از BourseDataProcessor و روی اسنپ‌شات ذخیره‌شده تو Gist
(فایل جدای queue_snapshot.json) کار می‌کنن.

ساختار اسنپ‌شات:
{
  "date": "1405-07-18",
  "updated": "10:31",
  "buy":  {symbol: {"v": ارزش_فعلی, "ref": مرجع, "heavy": bool, "t": "HH:MM", "exited": bool?}},
  "sell": {symbol: {...}},
}
همه‌ی ارزش‌ها به میلیارد تومان. v == 0 یعنی نماد از سقف/کف خارج شده.
"""

import logging
import math
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

BUY_SUPPLY_FILTER = "filter_17_buy_queue_supply"
SELL_COLLECT_FILTER = "filter_18_sell_queue_collect"


def empty_snapshot(today: str) -> dict:
    return {"date": today, "updated": None, "buy": {}, "sell": {}}


def _num(value) -> Optional[float]:
    """تبدیل ایمن به float؛ نال/نامعتبر -> None"""
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    return None if math.isnan(f) else f


def _step(
    prev: Optional[dict], cur: Optional[float], heavy_now: bool, cfg: dict, now_str: str
) -> Tuple[Optional[dict], Optional[dict]]:
    """
    یک قدم از ماشین حالت یک نماد.

    هشدار فقط یک‌جا صادر می‌شه: صفِ سنگینی که تا الان بوده، کامل از بین رفته
    (cur == 0: قیمت از سقف/کف خارج شده یا ارزش صف صفر شده).

    cur: ارزش فعلی صف (میلیارد تومان)؛ 0 = الان تو صف نیست؛ None = داده نامعلوم
         (حالت قبلی بدون تغییر حفظ می‌شه و هشداری داده نمی‌شه).
    برمی‌گردونه: (entry جدید یا None، event هشدار یا None)
    """
    min_value = cfg.get("min_queue_value", 0)

    if cur is None:
        return prev, None

    was_in_queue = bool(prev) and prev.get("v", 0) > 0

    # --- الان تو صف نیست ---
    if cur <= 0:
        if was_in_queue:
            event = None
            if prev.get("heavy") and prev.get("ref", 0) >= min_value:
                event = {"ref": prev["ref"]}
            return {"v": 0.0, "ref": 0.0, "heavy": False, "t": now_str, "exited": True}, event
        return prev, None  # هیچ‌وقت تو صف نبوده یا قبلاً خارج شده

    # --- الان تو صف هست ---
    if not was_in_queue:
        # صف تازه (یا ورود دوباره بعد از خروج): فقط شروع ردیابی، بدون هشدار
        return {"v": round(cur, 3), "ref": round(cur, 3), "heavy": bool(heavy_now), "t": now_str}, None

    # هنوز تو صف هست: فقط مرجع (بیشترین ارزش صف) رو به‌روز می‌کنیم؛ کم شدن
    # صف هشدار نمی‌ده - هشدار فقط وقتی میاد که صف کامل عرضه/جمع بشه.
    ref = max(prev.get("ref", cur), cur)
    heavy = bool(prev.get("heavy")) or bool(heavy_now)
    return {"v": round(cur, 3), "ref": round(ref, 3), "heavy": heavy, "t": now_str}, None


def _buy_current(rec: dict) -> Optional[float]:
    last = _num(rec.get("last_price"))
    ceiling = _num(rec.get("ceiling_price"))
    if last is None or ceiling is None:
        return None  # صندوق‌ها سقف ندارن؛ یا داده‌ی ناقص
    if last != ceiling:
        return 0.0
    return _num(rec.get("buy_queue_value"))


def _sell_current(rec: dict, cfg: dict) -> Optional[float]:
    last = _num(rec.get("last_price"))
    if last is None:
        return None

    floor = _num(rec.get("floor_price"))
    if floor is not None:
        at_floor = last == floor
    else:
        # کف قیمت از API نیومده: بهترین قیمت فروش سطح ۱ با آخرین قیمت برابر باشه
        # و افت قیمت از حد fallback_max_change_percent بیشتر باشه.
        # (صندوق‌ها floor_price ندارن و کلاً از ردیاب صف فروش کنار گذاشته می‌شن.)
        if rec.get("is_fund") in (True, 1):
            return None
        ask_price = _num(rec.get("ask_price"))
        change = _num(rec.get("last_price_change_percent"))
        if ask_price is None or change is None:
            return None
        at_floor = ask_price == last and change <= cfg.get("fallback_max_change_percent", -3.0)

    if not at_floor:
        return 0.0
    return _num(rec.get("sell_queue_value"))


def _events_df(df: pd.DataFrame, events: List[Tuple[str, dict]]) -> pd.DataFrame:
    if not events:
        return pd.DataFrame()
    info = {symbol: ev for symbol, ev in events}
    out = df[df["symbol"].isin(info)].copy()
    if out.empty:
        return pd.DataFrame()
    out["queue_ref"] = out["symbol"].map(lambda s: info[s]["ref"])
    return out.sort_values("queue_ref", ascending=False)


def evaluate_queues(
    df: pd.DataFrame,
    snapshot: Optional[dict],
    config: dict,
    now_str: str,
    today: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    df رو با اسنپ‌شات قبلی مقایسه می‌کنه.

    Returns: (df_عرضه_صف_خرید, df_جمع‌آوری_صف_فروش, اسنپ‌شات_جدید)
    """
    if not snapshot or snapshot.get("date") != today:
        snapshot = empty_snapshot(today)

    new_snapshot = {
        "date": today,
        "updated": now_str,
        "buy": dict(snapshot.get("buy", {})),
        "sell": dict(snapshot.get("sell", {})),
    }

    if df is None or df.empty or "symbol" not in df.columns:
        return pd.DataFrame(), pd.DataFrame(), new_snapshot

    # بعضی نمادها دو ردیف دارن (مثلاً سهم و صندوق هم‌نام)؛ حالت و هشدار هر نماد
    # باید یکتا باشه، پس ردیف سهام اول می‌مونه.
    if "is_fund" in df.columns:
        df = df.sort_values("is_fund", kind="stable")
    df = df.drop_duplicates(subset="symbol", keep="first")

    buy_cfg = config.get("buy_supply", {})
    sell_cfg = config.get("sell_collect", {})

    buy_on = buy_cfg.get("enabled", True) and {
        "last_price", "ceiling_price", "buy_queue_value"
    }.issubset(df.columns)
    sell_on = sell_cfg.get("enabled", True) and {"last_price", "sell_queue_value"}.issubset(df.columns)

    if buy_cfg.get("enabled", True) and not buy_on:
        logger.warning("⚠️ ردیاب صف خرید: ستون‌های لازم در داده نیست - این دور رد شد")
    if sell_cfg.get("enabled", True) and not sell_on:
        logger.warning("⚠️ ردیاب صف فروش: ستون sell_queue_value در داده نیست - این دور رد شد")

    buy_events: List[Tuple[str, dict]] = []
    sell_events: List[Tuple[str, dict]] = []

    min_buy_value = buy_cfg.get("min_queue_value", 0)
    min_buy_order = buy_cfg.get("min_buy_order", 0)
    min_buy_power = buy_cfg.get("min_godrat_kharid")  # None/غیرفعال = بدون شرط قدرت خرید
    min_sarane = buy_cfg.get("min_sarane_kharid")      # میلیون تومان؛ None = بدون شرط سرانه
    min_sell_value = sell_cfg.get("min_queue_value", 0)

    for rec in df.to_dict("records"):
        symbol = rec.get("symbol")
        if not symbol or (isinstance(symbol, float) and math.isnan(symbol)):
            continue

        if buy_on:
            cur = _buy_current(rec)
            order = _num(rec.get("buy_order")) or 0.0
            heavy_now = cur is not None and cur >= min_buy_value and order >= min_buy_order
            entry, event = _step(new_snapshot["buy"].get(symbol), cur, heavy_now, buy_cfg, now_str)
            if event and (min_buy_power is not None or min_sarane is not None):
                # فقط وقتی هشدار بده که با وجود عرضه، قدرت خرید و سرانه‌ی خرید
                # هنوز بالان (هر شرطی که تو کانفیگ None نباشه).
                power = _num(rec.get("godrat_kharid"))
                sarane = _num(rec.get("sarane_kharid"))
                power_ok = min_buy_power is None or (power is not None and power > min_buy_power)
                sarane_ok = min_sarane is None or (sarane is not None and sarane > min_sarane)
                if not (power_ok and sarane_ok):
                    event = None
            if entry is not None:
                new_snapshot["buy"][symbol] = entry
            if event:
                buy_events.append((symbol, event))

        if sell_on:
            cur = _sell_current(rec, sell_cfg)
            heavy_now = cur is not None and cur >= min_sell_value
            entry, event = _step(new_snapshot["sell"].get(symbol), cur, heavy_now, sell_cfg, now_str)
            if entry is not None:
                new_snapshot["sell"][symbol] = entry
            if event:
                sell_events.append((symbol, event))

    logger.info(
        f"📊 ردیاب صف: عرضه صف خرید={len(buy_events)} | جمع‌آوری صف فروش={len(sell_events)} | "
        f"در حال ردیابی: خرید={sum(1 for e in new_snapshot['buy'].values() if e.get('v', 0) > 0)}، "
        f"فروش={sum(1 for e in new_snapshot['sell'].values() if e.get('v', 0) > 0)}"
    )
    return _events_df(df, buy_events), _events_df(df, sell_events), new_snapshot


def snapshot_needs_save(
    old: Optional[dict], new: dict, refresh_minutes: float, now_ts: float
) -> bool:
    """
    نوشتن اسنپ‌شات تو Gist فقط وقتی لازمه که چیز مهمی عوض شده باشه، تا هر اجرا
    یک نوشتن اضافه به Gist نخوره (GitHub برای نوشتن‌های مکرر 403 می‌ده):
      - نماد جدید وارد/خارج صف شده یا برچسب «سنگین» عوض شده
      - مرجع (ref) یک نماد بیشتر از ۱۵٪ بالا رفته
      - از آخرین ذخیره بیشتر از refresh_minutes گذشته (برای تازه‌بودن مقادیر خلاصه‌ی بازار)
    """
    if not old or old.get("date") != new.get("date"):
        return True

    for side in ("buy", "sell"):
        old_side = old.get(side, {}) or {}
        new_side = new.get(side, {}) or {}
        if set(old_side) != set(new_side):
            return True
        for sym, e in new_side.items():
            o = old_side[sym]
            if bool(o.get("heavy")) != bool(e.get("heavy")):
                return True
            if (o.get("v", 0) > 0) != (e.get("v", 0) > 0):
                return True
            if e.get("ref", 0) > o.get("ref", 0) * 1.15 and e.get("ref", 0) > 0:
                return True

    saved_at = old.get("saved_at") or 0
    return (now_ts - saved_at) >= refresh_minutes * 60
