"""
فیلتر ۱۹: سرانه خرید سنگین

شرط‌ها (همه با هم):
  ۱) سرانه خرید لحظه‌ای  >=  ۲ برابر میانگین سرانه خرید ۱۰ روز قبل
  ۲) حجم نسبی (value_to_avg_monthly_value)  >=  ۱
  ۳) قدرت خرید  >  ۱
  ۴) سرانه خرید  >  ۱۰۰ میلیون تومان

API فقط سرانه «لحظه‌ای» رو می‌ده و میانگینش رو نمی‌ده، پس مقادیر پایانیِ هر روز تو Gist
صف‌ها (فایل جدای sarane_history.json) ذخیره می‌شه. برای هر نماد دو عدد نگه‌داری می‌شه:
سرانه خرید و اختلاف سرانه (سرانه خرید - سرانه فروش). فقط history_days روز آخر (الان ۲۰) نگه
داشته می‌شه؛ بیشتر از اون پاک می‌شه. پنجره‌ی میانگینِ فیلتر ۱۹ مستقل از اینه
(avg_window_days، الان ۱۰ روزِ آخر).

ساختار فایل sarane_history.json:
{"days": {"1405-07-14": {"فملی": [312.4, 38.1], ...}, ... حداکثر ۲۰ روز}}
هر مقدار = [سرانه_خرید, اختلاف_سرانه] (اختلاف ممکنه null باشه اگه سرانه فروش نبوده).
فرمت قدیمی (فقط یک عدد برای هر نماد) هم خونده می‌شه: همون عدد سرانه خرید حساب می‌شه و
اختلافش نامشخصه.
واحد: میلیون تومان (همون واحد df بعد از پردازش).

نوشتن فقط یک‌بار در روز و بعد از بسته‌شدن بازار انجام می‌شه (توی daily_summary_main،
ساعت ۱۲:۳۱ به بعد) تا مقادیر پایانیِ واقعی ثبت بشه. حین بازار فقط خونده می‌شه. هر
نوشتن، روز جدید رو اضافه می‌کنه و فقط history_days روز آخر رو نگه می‌داره. امروز تو
میانگینِ هشدارهای حین بازار نیست (قبل از ۱۲:۳۱ هنوز ذخیره نشده).
"""

import logging
import math
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

HEAVY_SARANE_FILTER = "filter_19_heavy_sarane_buy"


def empty_history() -> dict:
    return {"days": {}, "today": None}


def _num(value) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (ValueError, TypeError):
        return None
    return None if math.isnan(f) or math.isinf(f) else f


def _dedupe_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """بعضی نمادها دو ردیف دارن (سهم و صندوق هم‌نام)؛ ردیف سهام می‌مونه."""
    if "is_fund" in df.columns:
        df = df.sort_values("is_fund", kind="stable")
    return df.drop_duplicates(subset="symbol", keep="first")


def normalize_history(raw: Optional[dict], keep_days: int = 20) -> dict:
    """ساختار ناقص رو کامل می‌کنه و فقط keep_days روز آخر رو نگه می‌داره."""
    raw = raw if isinstance(raw, dict) else {}
    days = raw.get("days")
    days = dict(days) if isinstance(days, dict) else {}
    kept = sorted(days)[-keep_days:] if keep_days > 0 else []
    return {"days": {d: days[d] for d in kept}}


def days_before(history: dict, today: str) -> Dict[str, dict]:
    """روزهای تاریخچه به‌جز امروز (برای میانگین؛ امروز هیچ‌وقت تو میانگین نیست)."""
    return {d: v for d, v in history.get("days", {}).items() if d != today}


def has_day(history: dict, today: str) -> bool:
    return today in history.get("days", {})


def split_value(value) -> Tuple[Optional[float], Optional[float]]:
    """(سرانه_خرید, اختلاف_سرانه) از یک مقدار ذخیره‌شده؛ فرمت قدیمی (یک عدد) هم پشتیبانی می‌شه."""
    if isinstance(value, (list, tuple)):
        kharid = _num(value[0]) if len(value) > 0 else None
        diff = _num(value[1]) if len(value) > 1 else None
        return kharid, diff
    return _num(value), None


def compute_averages(
    days: Dict[str, dict], min_days: int, window_days: Optional[int] = None
) -> Dict[str, tuple]:
    """
    میانگین سرانه خرید هر نماد روی window_days روزِ آخر (اگه None باشه، همه‌ی روزها).
    برمی‌گردونه: {symbol: (avg, تعداد_روز)} فقط برای نمادهایی که حداقل
    min_days روز داده دارن.
    """
    ordered = sorted(days)
    if window_days and window_days > 0:
        ordered = ordered[-window_days:]
    sums: Dict[str, float] = {}
    counts: Dict[str, int] = {}
    for day in ordered:
        day_data = days[day]
        if not isinstance(day_data, dict):
            continue
        for symbol, value in day_data.items():
            v, _ = split_value(value)
            if v is None or v <= 0:
                continue
            sums[symbol] = sums.get(symbol, 0.0) + v
            counts[symbol] = counts.get(symbol, 0) + 1
    return {
        s: (sums[s] / counts[s], counts[s])
        for s in sums
        if counts[s] >= max(1, min_days)
    }


def evaluate_heavy_sarane(df: pd.DataFrame, days: Dict[str, dict], config: dict) -> pd.DataFrame:
    """نمادهای رد‌شده از فیلتر ۱۹ رو با ستون‌های sarane_avg_10d و sarane_to_avg_ratio برمی‌گردونه."""
    required = {"symbol", "sarane_kharid", "value_to_avg_monthly_value", "godrat_kharid"}
    if df is None or df.empty or not required.issubset(df.columns):
        logger.warning("⚠️ فیلتر ۱۹: ستون‌های لازم در داده نیست - این دور رد شد")
        return pd.DataFrame()

    averages = compute_averages(
        days, config.get("min_history_days", 10), config.get("avg_window_days", 10)
    )
    if not averages:
        logger.info(
            f"فیلتر ۱۹: تاریخچه کافی نیست (روزهای ذخیره‌شده: {len(days)}، "
            f"حداقل لازم: {config.get('min_history_days', 10)}) - هشداری بررسی نشد"
        )
        return pd.DataFrame()

    df = _dedupe_symbols(df).copy()
    df["sarane_avg_10d"] = df["symbol"].map(lambda s: averages.get(s, (None, 0))[0])
    df["sarane_history_days"] = df["symbol"].map(lambda s: averages.get(s, (None, 0))[1])

    mask = (
        df["sarane_avg_10d"].notna()
        & (df["sarane_kharid"] >= config.get("min_ratio_to_avg", 2) * df["sarane_avg_10d"])
        & (df["value_to_avg_monthly_value"] >= config.get("min_value_to_avg_monthly", 1))
        & (df["godrat_kharid"] > config.get("min_godrat_kharid", 1))
        & (df["sarane_kharid"] > config.get("min_sarane_kharid", 100))
    )

    out = df[mask].copy()
    out["sarane_to_avg_ratio"] = out["sarane_kharid"] / out["sarane_avg_10d"]
    out = out.sort_values("sarane_to_avg_ratio", ascending=False)
    logger.info(f"✅ فیلتر ۱۹ (سرانه خرید سنگین): {len(out)} سهم یافت شد")
    return out


def build_today_values(df: pd.DataFrame) -> Dict[str, List[Optional[float]]]:
    """
    مقادیر فعلی هر نماد برای ذخیره تو تاریخچه: [سرانه_خرید, اختلاف_سرانه].
    سرانه خرید باید معتبر و مثبت باشه وگرنه نماد ذخیره نمی‌شه؛ اختلاف (خرید - فروش)
    اگه سرانه فروش نباشه null ذخیره می‌شه.
    """
    if df is None or df.empty or not {"symbol", "sarane_kharid"}.issubset(df.columns):
        return {}
    df = _dedupe_symbols(df)
    forosh = df["sarane_forosh"] if "sarane_forosh" in df.columns else pd.Series(None, index=df.index)
    values: Dict[str, List[Optional[float]]] = {}
    for symbol, kharid, sell in zip(df["symbol"], df["sarane_kharid"], forosh):
        k = _num(kharid)
        if not symbol or k is None or k <= 0:
            continue
        sell_v = _num(sell)
        diff = round(k - sell_v, 1) if sell_v is not None else None
        values[str(symbol)] = [round(k, 1), diff]
    return values


def add_day(history: dict, today: str, values: Dict[str, list], keep_days: int = 20) -> dict:
    """نسخه‌ی جدید تاریخچه با روز امروز؛ فقط keep_days روز آخر (با احتساب امروز) می‌مونه."""
    days = dict(history.get("days", {}))
    days[today] = values
    return normalize_history({"days": days}, keep_days)
