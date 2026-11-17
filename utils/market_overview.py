"""
نمای کلی بازار برای خلاصه‌ی روزانه؛ مقدارها مستقیم از API خونده می‌شن (همه‌ی واحدها ریال):

  ۱. شاخص‌ها و معاملات کل بازار (بورس و فرابورس): api.tablokhani.com/public/market-indices
  ۲. پول حقیقی و سرانه‌ی کل بازار (آخرین ردیف):    tradersarena.ir/data/market/chart/totals0
  ۳. بازار آپشن + ارزش صف خرید/فروش کل بازار:       api.optionbaaz.ir/market-overview

هر منبع مستقل از بقیه fetch می‌شه و خرابی یکی، بقیه رو خراب نمی‌کنه؛ منبعی که نیومد None می‌شه و
بخش مربوطه نمایش داده نمی‌شه. جدول‌ها یک‌بار اینجا به‌صورت «بخش» (سرتیتر + ردیف) ساخته می‌شن و هم
مسیر ریچ (جدول) هم مسیر قدیمی (متن HTML) از همون استفاده می‌کنن.

start_market_state_fetch(): فقط برای چک باز/بسته‌بودن بازار تو main.py (هیچ ربطی به متن خلاصه نداره).
فقط از optionbaaz، موازی با fetch اصلی، با سقف سخت STATE_TIMEOUT ثانیه؛ بدون هیچ fallback.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import date, datetime
from typing import Any, Dict, List, Optional

try:
    from curl_cffi import requests as http_client  # libcurl: روی Actions برای tablokhani (کاربر)
except ImportError:  # نصب نبود => main.py نباید از کار بیفته؛ با requests ادامه می‌ده
    import requests as http_client

from config import MARKET_RETAIL_TURNOVER_CLASSES
from utils.industry_market_fetcher import RIAL_TO_MILLION_TOMAN, RIAL_TO_TRILLION_TOMAN

logger = logging.getLogger(__name__)

HTTP_CLIENT_NAME = "curl_cffi" if http_client.__name__.startswith("curl_cffi") else "requests"

INDICES_URL = "https://api.tablokhani.com/public/market-indices"
TOTALS_URL = "https://tradersarena.ir/data/market/chart/totals0"
OPTIONS_URL = "https://api.optionbaaz.ir/market-overview"

FETCH_TIMEOUT = 25  # ثانیه (برای داده‌های اصلی بعد بازار)
FETCH_RETRIES = 1   # یک‌بار retry

# چک وضعیت بازار تو هر اجرای main.py: سقف سخت از لحظه‌ی شروع (هم درخواست HTTP هم انتظار نتیجه)
STATE_TIMEOUT = 3.0

# endpoint وضعیت بازار optionbaaz (ساختار پاسخ: data.groups[].children[] با isOpen/session).
# ⚠️ آدرس تأییدنشده: ساختار پاسخ رو از نمونه‌ای که فرستادی نوشتم ولی آدرسش رو ندیدم؛ اگه فرق داره فقط همین خط.
# آدرس اشتباه/خطا => None => فقط ساعت/تقویم (ایمن).
MARKET_STATE_URL = "https://api.optionbaaz.ir/market/status/overview"

# فقط زیرگروه‌هایی که ربات روشون کار می‌کنه (سهام + صندوق بورس و فرابورس)؛ درآمد ثابت و بورس کالا نه.
STATE_CHILD_KEYS = ("TSE_STOCK", "IFB_STOCK", "TSE_ETF", "IFB_ETF")

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


def _get_json(
    url: str,
    timeout: float = FETCH_TIMEOUT,
    retries: int = FETCH_RETRIES
) -> Optional[dict]:

    host = url.split("/")[2]

    for attempt in range(1, retries + 2):
        try:
            resp = http_client.get(
                url,
                # impersonate="chrome",  # غیرفعال - روی Actions گاهی تایم‌اوت می‌داد
                timeout=timeout,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8",
                }
            )

            if resp.status_code == 200:
                data = resp.json()

                if isinstance(data, dict):
                    return data

                logger.warning(f"⚠️ پاسخ نامعتبر از {host}")
                return None

            logger.warning(
                f"⚠️ {host}: HTTP {resp.status_code} "
                f"(تلاش {attempt})"
            )

        except Exception as e:
            logger.warning(
                f"⚠️ {host}: {type(e).__name__}: {e} "
                f"(تلاش {attempt})"
            )

        if attempt <= retries:
            time.sleep(2)

    return None


# ----------------------------------------------------------------------
# وضعیت باز/بسته‌ی بازار (برای main.py)
# ----------------------------------------------------------------------
def parse_market_state(payload: Optional[dict]) -> Optional[str]:
    """
    وضعیت بازار از data.groups[].children[] (کلیدهای STATE_CHILD_KEYS، فیلد isOpen):
      • هرکدوم باز => 'open'
      • هر چهار تا دیده شدن و همه بسته => 'closed'
      • پاسخ نامعتبر / هر زیرگروه نبود / isOpen بولین نبود => None (نیمه‌کاره حدس نمی‌زنیم)
    """
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return None
    data = payload.get("data")
    groups = data.get("groups") if isinstance(data, dict) else None
    if not isinstance(groups, list):
        return None

    found: Dict[str, bool] = {}
    for group in groups:
        for child in (group.get("children") or []) if isinstance(group, dict) else []:
            if isinstance(child, dict) and child.get("key") in STATE_CHILD_KEYS and isinstance(child.get("isOpen"), bool):
                found[child["key"]] = child["isOpen"]

    if any(found.values()):
        return "open"
    if len(found) == len(STATE_CHILD_KEYS):
        return "closed"
    logger.info(f"ℹ️ زیرگروه‌های وضعیت بازار ناقص بود (دیده‌شده: {sorted(found)}) - فقط ساعت/تقویم")
    return None


def _fetch_market_state_blocking() -> Optional[str]:
    payload = _get_json(MARKET_STATE_URL, timeout=STATE_TIMEOUT, retries=0)
    return parse_market_state(payload)


class MarketStateFetch:
    """
    fetch وضعیت بازار تو پس‌زمینه. start_market_state_fetch() بلافاصله برمی‌گرده؛
    result() حداکثر تا STATE_TIMEOUT ثانیه *از لحظه‌ی شروع* صبر می‌کنه (نه از لحظه‌ی صدا زدن)،
    پس اگه fetch اصلی بیشتر از ۳ ثانیه طول کشیده، result() فوری جواب می‌ده. هر خطا/تایم‌اوت => None.
    """

    def __init__(self) -> None:
        self._t0 = time.monotonic()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="market-state")
        self._future = self._pool.submit(_fetch_market_state_blocking)

    def result(self) -> Optional[str]:
        remaining = max(0.0, STATE_TIMEOUT - (time.monotonic() - self._t0))
        try:
            return self._future.result(timeout=remaining)
        except FuturesTimeout:
            logger.warning(f"⏱️ وضعیت بازار از optionbaaz تو {STATE_TIMEOUT:g}s نیومد")
            return None
        except Exception as e:
            logger.warning(f"⚠️ وضعیت بازار: {type(e).__name__}")
            return None
        finally:
            self._pool.shutdown(wait=False)  # thread معلق جلوی خروج برنامه رو نگیره


def start_market_state_fetch() -> MarketStateFetch:
    return MarketStateFetch()


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
    "stockTransfer": "stock_flow",                # ورود پول سهام (ریال؛ منفی = خروج)
    "fixedIncomesTransfer": "fixed_income_flow",  # ورود پول صندوق‌های درآمد ثابت
    "bpcx": "buy_per_capita",                     # سرانه خرید معاملات خرد (ریال)
    "spcx": "sell_per_capita",                    # سرانه فروش معاملات خرد (ریال)
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
    if result["stock_flow"] is None and result["buy_per_capita"] is None:
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
    خروجی: {"call": {...}|None, "put": {...}|None, "queue": {"buy_value","sell_value"}|None,
            "breadth": {"total","pos","neg","zero","buy_queue_count","sell_queue_count"}|None,
            "retail_turnover": ارزش معاملات خرد به ریال|None}
    breadth = وضعیت نمادهای سهام (کلاس stock). queue = ارزش صف خرید/فروش «کل بازار» (segments.total؛ برابر جمع همه‌ی کلاس‌ها).
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

    stock = classes.get("stock") or {}
    total = _num(stock.get("total"))
    breadth = None
    if total and total > 0:
        breadth = {
            "total": total,
            "pos": _num(stock.get("pos")),
            "neg": _num(stock.get("neg")),
            "zero": _num(stock.get("zero")),
            "buy_queue_count": _num(stock.get("buyQueueCount")),
            "sell_queue_count": _num(stock.get("sellQueueCount")),
        }

    seg = (latest.get("segments") or {}).get("total") or {}
    buy_q, sell_q = _num(seg.get("buyQueue")), _num(seg.get("sellQueue"))
    queue = {"buy_value": buy_q, "sell_value": sell_q} if buy_q is not None and sell_q is not None else None

    # ارزش معاملات خرد (ریال) = جمع turnover کلاس‌های MARKET_RETAIL_TURNOVER_CLASSES؛
    # اگه حتی یکی نبود/نامعتبر بود None (جمع ناقص، عدد غلط می‌ده)
    parts = [_num((classes.get(name) or {}).get("turnover")) for name in MARKET_RETAIL_TURNOVER_CLASSES]
    retail_turnover = sum(parts) if parts and all(v is not None for v in parts) else None
    if parts and retail_turnover is None:
        logger.warning("⚠️ turnover بعضی کلاس‌های ارزش معاملات خرد تو optionbaaz نیومد - این ردیف نمایش داده نمی‌شه")

    if not (call or put or queue or breadth or retail_turnover is not None):
        return None
    return {"call": call, "put": put, "queue": queue, "breadth": breadth, "retail_turnover": retail_turnover}


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


def money_section(totals: Optional[dict], options: Optional[dict], trade: Optional[dict] = None) -> Optional[dict]:
    """
    ارزش بازار (بورس + فرابورس) + ارزش معاملات خرد (optionbaaz) + ورود پول + سرانه‌ی معاملات خرد
    + ارزش صف‌های خرید و فروش. ردیفی که داده‌اش نیومده نمایش داده نمی‌شه.
    """
    rows = []
    bourse, fara = (trade or {}).get("bourse"), (trade or {}).get("fara")
    if bourse and fara:
        a, b = bourse.get("market_value_rial"), fara.get("market_value_rial")
        if a is not None and b is not None:
            rows.append(["ارزش بازار", f"{_hemat0(a + b)} همت"])
    retail = (options or {}).get("retail_turnover")
    if retail is not None:
        rows.append(["ارزش معاملات خرد", f"{_hemat(retail)} همت"])
    t = totals or {}
    for label, key in (("ورود پول سهام", "stock_flow"), ("ورود پول درآمد ثابت", "fixed_income_flow")):
        v = t.get(key)
        if v is not None:
            emoji = "🟢" if v >= 0 else "🔴"
            rows.append([label, f"{emoji} {v / RIAL_TO_TRILLION_TOMAN:+,.2f} همت"])
    if t.get("buy_per_capita") is not None:
        rows.append(["سرانه خرید", f"{t['buy_per_capita'] / RIAL_TO_MILLION_TOMAN:,.0f} میلیون تومان"])
    if t.get("sell_per_capita") is not None:
        rows.append(["سرانه فروش", f"{t['sell_per_capita'] / RIAL_TO_MILLION_TOMAN:,.0f} میلیون تومان"])
    queue = (options or {}).get("queue")
    if queue:
        rows.append(["ارزش صف‌های خرید", f"{_hemat(queue['buy_value'])} همت"])
        rows.append(["ارزش صف‌های فروش", f"{_hemat(queue['sell_value'])} همت"])
    if not rows:
        return None
    return {"headers": ["شاخص", "مقدار"], "units": ["", ""], "rows": rows, "note": None}


def breadth_section(options: Optional[dict]) -> Optional[dict]:
    """
    مثبت بودن بازار (نمادهای سهام): تعداد و درصد مثبت، منفی، بدون تغییر، و تعداد در صف خرید/فروش
    (درصد از کل نمادها؛ نوار ده‌تایی برای نگاه سریع). صف‌ها زیرمجموعه‌ی مثبت/منفی‌ان.
    """
    b = (options or {}).get("breadth")
    if not b or not b.get("total"):
        return None
    total = b["total"]

    def row(emoji, label, n):
        if n is None:
            return None
        pct = n / total * 100
        filled = max(0, min(10, round(pct / 10)))
        return [f"{emoji} {label}", f"{n:,.0f}", f"{pct:.0f}٪ {'█' * filled}{'░' * (10 - filled)}"]

    rows = [r for r in (
        row("🟢", "مثبت", b.get("pos")),
        row("🔴", "منفی", b.get("neg")),
        row("⚪", "بدون تغییر", b.get("zero")),
        row("🟩", "صف خرید", b.get("buy_queue_count")),
        row("🟥", "صف فروش", b.get("sell_queue_count")),
    ) if r]
    if not rows:
        return None
    return {
        "headers": ["وضعیت", "تعداد", "درصد"],
        "units": ["", "", ""],
        "rows": rows,
        "note": f"از {total:,.0f} نماد سهام؛ صف‌ها زیرمجموعه‌ی مثبت و منفی‌اند",
    }


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
    "breadth": "🧭 مثبت بودن بازار (سهام)",
    "money": "💰 پول، سرانه و صف کل بازار",
    "options": "🎯 بازار آپشن",
}


def overview_sections(overview: Optional[dict]) -> List[tuple]:
    """[(key, section)] به ترتیب نمایش، فقط بخش‌هایی که داده دارن."""
    ov = overview or {}
    out = []
    for key, section in (
        ("breadth", breadth_section(ov.get("options"))),
        ("money", money_section(ov.get("totals"), ov.get("options"), ov.get("trade"))),
        ("options", options_section(ov.get("options"))),
    ):
        if section:
            out.append((key, section))
    return out
