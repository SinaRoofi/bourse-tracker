"""
نمای کلی بازار برای خلاصه‌ی روزانه؛ مقدارها مستقیم از API خونده می‌شن (همه‌ی واحدها ریال):

  ۱. شاخص‌ها و معاملات کل بازار (بورس و فرابورس): api.tablokhani.com/public/market-indices
  ۲. پول حقیقی و سرانه‌ی کل بازار (آخرین ردیف):    tradersarena.ir/data/market/chart/totals0
  ۳. بازار آپشن + ارزش صف خرید/فروش کل بازار:       api.optionbaaz.ir/market-overview

هر منبع مستقل از بقیه fetch می‌شه و خرابی یکی، بقیه رو خراب نمی‌کنه؛ منبعی که نیومد None می‌شه و
بخش مربوطه نمایش داده نمی‌شه. جدول‌ها یک‌بار اینجا به‌صورت «بخش» (سرتیتر + ردیف) ساخته می‌شن و هم
مسیر ریچ (جدول) هم مسیر قدیمی (متن HTML) از همون استفاده می‌کنن.

fetch_market_state(): فقط برای چک باز/بسته‌بودن بازار تو main.py (هیچ ربطی به متن خلاصه نداره).
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests

from utils.industry_market_fetcher import RIAL_TO_MILLION_TOMAN, RIAL_TO_TRILLION_TOMAN

logger = logging.getLogger(__name__)

INDICES_URL = "https://api.tablokhani.com/public/market-indices"
TOTALS_URL = "https://tradersarena.ir/data/market/chart/totals0"
OPTIONS_URL = "https://api.optionbaaz.ir/market-overview"

FETCH_TIMEOUT = 20  # ثانیه
FETCH_RETRIES = 1   # یک‌بار retry

# چک وضعیت بازار تو هر اجرای main.py: کوتاه و بدون retry، تا اجرا رو کند نکنه
STATE_TIMEOUT = 10

HEADERS = {"User-Agent": "Mozilla/5.0"}

# پسوند مقدارها تو tablokhani (مثلاً '329,559.890B')
_SUFFIX_MULTIPLIER = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}


def _num(value: Any) -> Optional[float]:
    """عدد یا رشته‌ی عددی ('8,342,551.97'، '-1.01'، '46.707B') -> float؛ نامعتبر -> None"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    multiplier = 1.0
    if text[-1].upper() in _SUFFIX_MULTIPLIER:
        multiplier = _SUFFIX_MULTIPLIER[text[-1].upper()]
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _get_json(url: str, timeout: float = FETCH_TIMEOUT, retries: int = FETCH_RETRIES) -> Optional[dict]:
    host = url.split("/")[2]
    for attempt in range(1, retries + 2):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict):
                    return data
                logger.warning(f"⚠️ پاسخ نامعتبر از {host}")
                return None
            logger.warning(f"⚠️ {host}: HTTP {resp.status_code} (تلاش {attempt})")
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ {host}: Timeout (تلاش {attempt})")
        except Exception as e:  # متن خطا ممکنه URL داشته باشه؛ فقط نوعش
            logger.warning(f"⚠️ {host}: {type(e).__name__} (تلاش {attempt})")
        if attempt <= retries:
            time.sleep(1)
    return None


# ----------------------------------------------------------------------
# وضعیت باز/بسته‌ی بازار (برای main.py)
# ----------------------------------------------------------------------
def fetch_market_state() -> Optional[str]:
    """
    وضعیت بازار از tablokhani (فیلد state بورس و فرابورس؛ مقدارها 'open' یا 'closed'):
      • هر کدوم open باشن            -> 'open'
      • همه‌ی مقدارهای دیده‌شده closed -> 'closed'
      • مقدار ناشناخته               -> همون مقدار خام (صدازننده فقط لاگ می‌کنه)
      • نیومد/نامعتبر                -> None
    فقط یک درخواست کوتاه (STATE_TIMEOUT) بدون retry.
    """
    payload = _get_json(INDICES_URL, timeout=STATE_TIMEOUT, retries=0)
    if not payload or not payload.get("success"):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    states = []
    for key in ("bourse", "fara_bourse"):
        value = (data.get(key) or {}).get("state")
        if value:
            states.append(str(value).strip().lower())
    if not states:
        return None
    if "open" in states:
        return "open"
    unique = sorted(set(states))
    return unique[0] if len(unique) == 1 else "|".join(unique)


# ----------------------------------------------------------------------
# tablokhani: شاخص‌ها + معاملات کل بازار
# ----------------------------------------------------------------------
def parse_indices_payload(payload: dict) -> Optional[Dict[str, Any]]:
    """
    خروجی: {"indices": [ {key, name, value, change, percent}, ... ],
            "trade": {"bourse": {...}, "fara": {...}}}
    ارزش‌ها (value_rial / market_value_rial) ریالن (مقدار API با پسوند B = میلیارد).
    """
    if not payload.get("success"):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    bourse = data.get("bourse") or {}
    fara = data.get("fara_bourse") or {}

    specs = (
        ("main", "شاخص کل", bourse, "index"),
        ("equal", "شاخص کل هم‌وزن", bourse, "index_h"),
        ("fara", "شاخص کل فرابورس", fara, "index"),
    )
    indices: List[dict] = []
    for key, name, src, field in specs:
        value = _num(src.get(field))
        percent = _num(src.get(f"{field}_change_percent"))
        if value is None or percent is None:
            continue
        indices.append({
            "key": key,
            "name": name,
            "value": value,
            "change": _num(src.get(f"{field}_change")),
            "percent": percent,
        })

    def trade(src: dict) -> Optional[dict]:
        value = _num(src.get("trade_value"))
        if value is None:
            return None
        return {
            "value_rial": value,
            "count": _num(src.get("trade_number")),
            "market_value_rial": _num(src.get("market_value")),
        }

    result = {
        "indices": indices,
        "trade": {"bourse": trade(bourse), "fara": trade(fara)},
    }
    if not indices and not (result["trade"]["bourse"] or result["trade"]["fara"]):
        return None
    return result


# ----------------------------------------------------------------------
# tradersarena: پول حقیقی و سرانه‌ی کل بازار (آخرین ردیف سری زمانی)
# ----------------------------------------------------------------------
_TOTALS_FIELDS = {
    "transfer": "real_flow",                      # خالص ورود/خروج پول حقیقی (ریال)
    "stockTransfer": "stock_flow",                # ... در سهام
    "fixedIncomesTransfer": "fixed_income_flow",  # ... در صندوق‌های درآمد ثابت
    "plus": "positive",                           # تعداد نمادهای مثبت
    "minus": "negative",                          # تعداد نمادهای منفی
    "bpc": "buy_per_capita",                      # سرانه خرید حقیقی کل (ریال)
    "spc": "sell_per_capita",                     # سرانه فروش حقیقی کل (ریال)
}


def parse_totals_payload(payload: dict, today: Optional[date] = None) -> Optional[Dict[str, Any]]:
    """
    آخرین ردیف سری زمانی. فقط کلیدهایی که طول لیستشون با time برابره (مثل خود API)
    خونده می‌شن. اگه today داده بشه و تاریخ آخرین ردیف با اون نخونه، داده کهنه حساب می‌شه.
    """
    times = payload.get("time")
    if not isinstance(times, list) or not times:
        return None
    n = len(times)
    last = {k: v[-1] for k, v in payload.items() if isinstance(v, list) and len(v) == n}

    result: Dict[str, Any] = {}
    for api_key, name in _TOTALS_FIELDS.items():
        result[name] = _num(last.get(api_key))
    stamp = _num(last.get("time"))
    if stamp is None:
        return None
    if result["real_flow"] is None and result["buy_per_capita"] is None:
        return None

    row_date = datetime.utcfromtimestamp(stamp).date()
    if today is not None and row_date != today:
        logger.warning(f"⚠️ داده‌ی totals0 مال {row_date} است نه امروز ({today}) - نادیده گرفته شد")
        return None
    result["time"] = int(stamp)
    return result


# ----------------------------------------------------------------------
# optionbaaz: بازار آپشن + ارزش صف خرید/فروش کل بازار
# ----------------------------------------------------------------------
def _option_side(src: Any) -> Optional[Dict[str, Optional[float]]]:
    if not isinstance(src, dict):
        return None
    side = {
        "turnover": _num(src.get("turnover")),
        "oi": _num(src.get("oi")),
        "oi_prev": _num(src.get("oiPrev")),
        # خالص ورود پول حقیقی به‌صورت نوشنال؛ فقط تو بخش latest پاسخ هست
        "net_notional": _num(src.get("realNetNotional")),
    }
    return side if side["turnover"] is not None else None


def parse_options_payload(payload: dict, today: Optional[date] = None) -> Optional[Dict[str, Any]]:
    """
    خروجی: {"call": {...}|None, "put": {...}|None, "queue": {"buy_value","sell_value"}|None}
    queue = ارزش صف خرید/فروش «کل بازار» (segments.total؛ برابر جمع همه‌ی کلاس‌ها).
    اگه today داده بشه و tradingDate با اون نخونه، داده کهنه حساب می‌شه.
    """
    if payload.get("status") != "success":
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    if today is not None:
        trading_date = str(data.get("tradingDate") or "")
        if trading_date != today.isoformat():
            logger.warning(f"⚠️ داده‌ی optionbaaz مال {trading_date or '؟'} است نه امروز ({today}) - نادیده گرفته شد")
            return None
    latest = ((data.get("markets") or {}).get("equity") or {}).get("latest") or {}
    classes = latest.get("classes") or {}
    call = _option_side(classes.get("equity_option_call"))
    put = _option_side(classes.get("equity_option_put"))

    seg = (latest.get("segments") or {}).get("total") or {}
    buy_q, sell_q = _num(seg.get("buyQueue")), _num(seg.get("sellQueue"))
    queue = {"buy_value": buy_q, "sell_value": sell_q} if buy_q is not None and sell_q is not None else None

    if not (call or put or queue):
        return None
    return {"call": call, "put": put, "queue": queue}


# ----------------------------------------------------------------------
# ورودی اصلی
# ----------------------------------------------------------------------
def fetch_market_overview(today: Optional[date] = None) -> Dict[str, Any]:
    """
    هر سه منبع رو هم‌زمان می‌گیره. خروجی همیشه دیکشنری با این کلیدهاست (None = نیومد):
      {"indices": [...] | None, "trade": {...} | None, "totals": {...} | None, "options": {...} | None}
    """
    def fetch_indices():
        payload = _get_json(INDICES_URL)
        return parse_indices_payload(payload) if payload else None

    def fetch_totals():
        payload = _get_json(TOTALS_URL)
        return parse_totals_payload(payload, today) if payload else None

    def fetch_options():
        payload = _get_json(OPTIONS_URL)
        return parse_options_payload(payload, today) if payload else None

    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {
            "indices": ex.submit(fetch_indices),
            "totals": ex.submit(fetch_totals),
            "options": ex.submit(fetch_options),
        }
        for name, fut in futures.items():
            try:
                results[name] = fut.result()
            except Exception as e:
                logger.error(f"❌ خطا در پردازش {name}: {type(e).__name__}")
                results[name] = None

    idx = results["indices"]
    overview = {
        "indices": idx["indices"] if idx and idx["indices"] else None,
        "trade": idx["trade"] if idx and (idx["trade"]["bourse"] or idx["trade"]["fara"]) else None,
        "totals": results["totals"],
        "options": results["options"],
    }
    logger.info(
        "📈 نمای کلی بازار: شاخص‌ها=%s، معاملات=%s، پول/سرانه=%s، آپشن=%s",
        *("✅" if overview[k] else "❌" for k in ("indices", "trade", "totals", "options")),
    )
    return overview


# ----------------------------------------------------------------------
# «بخش»‌های نمایشی (مشترک بین مسیر ریچ و مسیر قدیمی)
#   هر بخش: {"headers": [...], "units": [...], "rows": [[...], ...], "note": str|None}
#   مقدارها رشته‌ی ساده‌ان (escape نشده)؛ units[i] کنار ستون i (خالی = بدون واحد).
# ----------------------------------------------------------------------
def _hemat(rial: float) -> str:
    return f"{rial / RIAL_TO_TRILLION_TOMAN:,.2f}"


def _hemat0(rial: float) -> str:
    """ارزش بازار: بدون اعشار (عدد خیلی بزرگه؛ اعشار فقط نویزه و جمع ردیف‌ها هم دقیق می‌مونه)."""
    return f"{rial / RIAL_TO_TRILLION_TOMAN:,.0f}"


def _signed_pct(percent: float) -> str:
    arrow = "▲" if percent >= 0 else "▼"
    return f"{arrow} {abs(percent):.2f}٪"


def trade_section(trade: Optional[dict]) -> Optional[dict]:
    """معاملات کل بازار: ردیف بورس، فرابورس و «کل بازار» (جمع هر دو؛ فقط اگه هر دو اومده باشن)."""
    if not trade:
        return None
    bourse, fara = trade.get("bourse"), trade.get("fara")
    rows = []

    def count(x):
        return f"{x:,.0f}" if x is not None else "—"

    def row(name, src):
        mv = src.get("market_value_rial")
        return [name, _hemat(src["value_rial"]), count(src.get("count")), _hemat0(mv) if mv is not None else "—"]

    if bourse:
        rows.append(row("بورس", bourse))
    if fara:
        rows.append(row("فرابورس", fara))
    if bourse and fara:
        def both(key):
            a, b = bourse.get(key), fara.get(key)
            return a + b if a is not None and b is not None else None
        total_mv = both("market_value_rial")
        total_count = both("count")
        rows.append([
            "کل بازار",
            _hemat(bourse["value_rial"] + fara["value_rial"]),
            count(total_count),
            _hemat0(total_mv) if total_mv is not None else "—",
        ])
    if not rows:
        return None
    return {
        "headers": ["بازار", "ارزش معاملات", "تعداد معاملات", "ارزش بازار"],
        "units": ["", "همت", "", "همت"],
        "rows": rows,
        "note": None,
    }


def money_section(totals: Optional[dict], options: Optional[dict]) -> Optional[dict]:
    """پول حقیقی + سرانه + (از optionbaaz) ارزش صف خرید/فروش کل بازار."""
    rows = []
    t = totals or {}
    for label, key in (("خالص پول حقیقی (کل)", "real_flow"), ("پول حقیقی سهام", "stock_flow"),
                       ("پول حقیقی درآمد ثابت", "fixed_income_flow")):
        v = t.get(key)
        if v is not None:
            emoji, word = ("🟢", "ورود") if v >= 0 else ("🔴", "خروج")
            rows.append([label, f"{emoji} {word} {abs(v) / RIAL_TO_TRILLION_TOMAN:,.2f} همت"])
    if t.get("buy_per_capita") is not None:
        rows.append(["سرانه خرید حقیقی", f"{t['buy_per_capita'] / RIAL_TO_MILLION_TOMAN:,.0f} میلیون تومان"])
    if t.get("sell_per_capita") is not None:
        rows.append(["سرانه فروش حقیقی", f"{t['sell_per_capita'] / RIAL_TO_MILLION_TOMAN:,.0f} میلیون تومان"])
    if t.get("positive") is not None and t.get("negative") is not None:
        rows.append(["نماد مثبت / منفی", f"{t['positive']:,.0f} / {t['negative']:,.0f}"])
    queue = (options or {}).get("queue")
    if queue:
        rows.append(["ارزش صف خرید", f"{_hemat(queue['buy_value'])} همت"])
        rows.append(["ارزش صف فروش", f"{_hemat(queue['sell_value'])} همت"])
    if not rows:
        return None
    return {"headers": ["شاخص", "مقدار"], "units": ["", ""], "rows": rows, "note": None}


def options_section(options: Optional[dict]) -> Optional[dict]:
    """
    بازار آپشن: ستون‌های کال/پوت/کل. «کل» = جمع کال و پوت؛ تغییر موقعیت باز و نسبت پوت به کال
    تنها مقدارهای محاسبه‌شده‌ان (API مستقیم نمی‌ده).
    """
    if not options:
        return None
    call, put = options.get("call"), options.get("put")
    if not (call and put):
        return None

    def both(key):
        a, b = call.get(key), put.get(key)
        return a + b if a is not None and b is not None else None

    def pct_change(oi, prev):
        if oi is None or not prev or prev <= 0:
            return "—"
        return _signed_pct((oi / prev - 1) * 100)

    def fmt(value, fn):
        return fn(value) if value is not None else "—"

    def flow(v):
        return f"{'🟢' if v >= 0 else '🔴'} {v / RIAL_TO_TRILLION_TOMAN:+,.2f}"

    total = {k: both(k) for k in ("turnover", "oi", "oi_prev", "net_notional")}
    rows = [
        ["ارزش معاملات (همت)"] + [fmt(s["turnover"], _hemat) for s in (call, put, total)],
        ["موقعیت‌های باز (میلیون)"] + [fmt(s["oi"], lambda v: f"{v / 1e6:,.2f}") for s in (call, put, total)],
        ["تغییر موقعیت باز"] + [pct_change(s["oi"], s["oi_prev"]) for s in (call, put, total)],
    ]
    if any(s.get("net_notional") is not None for s in (call, put)):
        rows.insert(1, ["ورود پول نوشنال (همت)"] + [fmt(s.get("net_notional"), flow) for s in (call, put, total)])
    notes = []
    if call["turnover"] and put["turnover"] is not None:
        notes.append(f"ارزش معاملات {put['turnover'] / call['turnover']:.2f}")
    if call["oi"] and put["oi"] is not None:
        notes.append(f"موقعیت‌های باز {put['oi'] / call['oi']:.2f}")
    return {
        "headers": ["شاخص", "کال", "پوت", "کل"],
        "units": ["", "", "", ""],
        "rows": rows,
        "note": ("نسبت پوت به کال — " + " | ".join(notes)) if notes else None,
    }


# عنوان هر بخش (ایموجی + متن)
SECTION_TITLES = {
    "trade": "💹 معاملات کل بازار",
    "money": "💰 پول، سرانه و صف کل بازار",
    "options": "🎯 بازار آپشن",
}


def overview_sections(overview: Optional[dict]) -> List[tuple]:
    """[(key, section)] به ترتیب نمایش، فقط بخش‌هایی که داده دارن."""
    ov = overview or {}
    out = []
    for key, section in (
        ("trade", trade_section(ov.get("trade"))),
        ("money", money_section(ov.get("totals"), ov.get("options"))),
        ("options", options_section(ov.get("options"))),
    ):
        if section:
            out.append((key, section))
    return out
