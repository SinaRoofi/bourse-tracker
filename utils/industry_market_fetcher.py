"""
ماژول دریافت و تحلیل خلاصه‌ی صنایع/صندوق‌ها از endpoint جدول صنایع
tradersarena (data/industries-csv).

این کاملاً جدا از per-symbol snapshot (utils/data_fetcher.py) هست: هر
ردیف اینجا یک صنعت یا یک نوع صندوقه (نه یک نماد)، با معیارهای تجمیعی
مثل ارزش کل معاملات، ورود پول، سرانه خرید/فروش و قدرت خرید، به همراه
مقایسه‌ی هرکدوم با میانگین ۵ و ۲۰ روزه‌ی خودشون.

طبق درخواست کاربر، صندوق‌های طلا/نقره/درآمد ثابت همیشه از خروجی حذف
می‌شن (ارزش و ورود پولشون آنقدر بزرگه که میانگین‌های کل بازار رو
منحرف می‌کنه، و اصلاً "صنعت" واقعی هم نیستن).
"""

import csv
import io
import logging
import time
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

INDUSTRIES_CSV_URL = "https://tradersarena.ir/data/industries-csv"

FETCH_TIMEOUT = 10      # ثانیه
FETCH_RETRIES = 1       # یک‌بار retry
FETCH_RETRY_DELAY = 1   # ثانیه بین retry‌ها

# ریال -> میلیارد/میلیون تومان (هم‌رسم با data_processor._clean_and_prepare_api1)
RIAL_TO_BILLION_TOMAN = 10_000_000_000
RIAL_TO_MILLION_TOMAN = 10_000_000

# صندوق‌هایی که طبق درخواست کاربر همیشه از خلاصه‌ی بازار کنار گذاشته می‌شن
EXCLUDED_CODES = {"gold-funds", "silver-funds", "fixed-income-funds"}

# ترتیب ستون‌ها دقیقاً مطابق خروجی endpoint
COLUMNS = [
    "code", "name", "volume", "value",
    "value_buy_real", "value_sell_real",
    "value_buy_orders", "value_sell_orders", "net_order_value",
    "group_return_equal_weight",
    "sarane_kharid", "sarane_forosh", "godrat_kharid",
    "buy_real_percent", "sell_real_percent",
    "pol_hagigi",
    "value_percent_of_retail_trades", "share_index_of_trades",
    "value_vs_avg5_pct", "value_vs_avg20_pct",
    "value_avg5", "value_avg20",
    "sarane_kharid_5d", "sarane_forosh_5d",
    "sarane_kharid_20d", "sarane_forosh_20d",
    "godrat_kharid_5d", "godrat_kharid_20d",
    "pol_hagigi_5d", "pol_hagigi_20d",
]

TEXT_COLUMNS = {"code", "name"}


def _to_number(raw: str) -> float:
    raw = (raw or "").strip()
    if raw == "":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _looks_like_header(first_row: List[str]) -> bool:
    """اگه ستون سوم (حجم) عدد نباشه، یعنی ردیف اول هدره نه داده."""
    if len(first_row) < 3:
        return True
    try:
        float(first_row[2])
        return False
    except ValueError:
        return True


class IndustryMarketFetcher:
    """دریافت، پارس و تحلیل خلاصه‌ی صنایع/صندوق‌ها از tradersarena."""

    def __init__(self, url: str = INDUSTRIES_CSV_URL):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv, text/plain, */*",
        })

    # ------------------------------------------------------------------
    # دریافت
    # ------------------------------------------------------------------
    def _get_with_retry(self) -> Optional[requests.Response]:
        params = {"_": int(time.time() * 1000)}
        for attempt in range(FETCH_RETRIES + 1):
            try:
                resp = self.session.get(self.url, params=params, timeout=FETCH_TIMEOUT)
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"⚠️ تلاش {attempt + 1} برای خلاصه‌ی صنایع ناموفق: {e}")
                if attempt < FETCH_RETRIES:
                    time.sleep(FETCH_RETRY_DELAY)
        logger.error("❌ دریافت خلاصه‌ی صنایع/صندوق‌ها ناموفق بود")
        return None

    def fetch(self) -> List[Dict]:
        """
        Returns:
            list[dict]: هر دیکشنری یک صنعت/صندوق با کلیدهای COLUMNS،
            به‌جز سه صندوق مستثنا (طلا/نقره/درآمد ثابت).
        """
        response = self._get_with_retry()
        if response is None:
            return []

        response.encoding = response.encoding or "utf-8"

        try:
            rows = list(csv.reader(io.StringIO(response.text)))
        except Exception as e:
            logger.error(f"❌ خطا در parse CSV صنایع: {e}")
            return []

        if not rows:
            return []

        if _looks_like_header(rows[0]):
            rows = rows[1:]

        records: List[Dict] = []
        excluded_found = 0

        for row in rows:
            if len(row) < len(COLUMNS):
                continue

            record = {
                col: (row[i].strip() if col in TEXT_COLUMNS else _to_number(row[i]))
                for i, col in enumerate(COLUMNS)
            }

            if record["code"] in EXCLUDED_CODES:
                excluded_found += 1
                continue

            records.append(record)

        logger.info(
            f"✅ {len(records)} صنعت/صندوق دریافت شد "
            f"(بعد از حذف {excluded_found} صندوق مستثنا)"
        )
        return records

    # ------------------------------------------------------------------
    # تحلیل
    # ------------------------------------------------------------------
    @staticmethod
    def analyze(records: List[Dict]) -> Dict:
        """
        محاسبه‌ی خلاصه‌ی بازار روی رکوردهای برگشتی از fetch().

        خروجی:
          - value_above_avg: صنایعی که ارزش امروزشون از میانگین ۵ روزه
            *و* ۲۰ روزه بیشتره (مرتب‌شده بر اساس درصد فاصله از میانگین
            ۲۰ روزه، هر رکورد یک کلید اضافه‌ی pct5/pct20 داره)
          - sarane_above_20d: صنایعی که سرانه خرید امروزشون از سرانه
            خرید ۲۰ روزه‌شون بیشتره (هر رکورد یک کلید اضافه‌ی
            sarane_ratio داره - نسبت امروز به ۲۰ روزه)
          - pol_to_avg20: برای هر صنعت با میانگین ۲۰ روزه‌ی معتبر،
            ورود پول امروز به‌عنوان درصدی از اون میانگین (هر رکورد یک
            کلید اضافه‌ی pol_to_avg20_pct داره)، مرتب از مثبت به منفی
          - totals: جمع کل بازار روی همه‌ی رکوردهای ورودی
        """
        value_above_avg: List[Dict] = []
        sarane_above_20d: List[Dict] = []
        pol_to_avg20: List[Dict] = []

        total_value = 0.0
        total_pol_hagigi = 0.0
        total_value_avg20 = 0.0
        total_value_buy_real = 0.0
        total_buyer_count = 0.0  # مخرج سرانه‌ی خرید وزن‌دار کل بازار

        for r in records:
            total_value += r["value"]
            total_pol_hagigi += r["pol_hagigi"]
            total_value_avg20 += r["value_avg20"]
            total_value_buy_real += r["value_buy_real"]

            # سرانه‌ی خرید هر صنعت یعنی ارزش‌خرید‌حقیقی/تعداد کد خریدار؛
            # از همین رابطه تعداد کد خریدار رو استخراج می‌کنیم تا بشه
            # سرانه‌ی وزن‌دار کل بازار رو درست حساب کرد (نه میانگین ساده).
            if r["sarane_kharid"] > 0:
                total_buyer_count += r["value_buy_real"] / r["sarane_kharid"]

            if r["value_vs_avg5_pct"] > 0 and r["value_vs_avg20_pct"] > 0:
                value_above_avg.append({
                    **r,
                    "pct5": r["value_vs_avg5_pct"],
                    "pct20": r["value_vs_avg20_pct"],
                })

            if r["sarane_kharid_20d"] > 0 and r["sarane_kharid"] > r["sarane_kharid_20d"]:
                sarane_above_20d.append({
                    **r,
                    "sarane_ratio": r["sarane_kharid"] / r["sarane_kharid_20d"],
                })

            if r["value_avg20"] > 0:
                pol_to_avg20.append({
                    **r,
                    "pol_to_avg20_pct": (r["pol_hagigi"] / r["value_avg20"]) * 100,
                })

        value_above_avg.sort(key=lambda r: r["pct20"], reverse=True)
        sarane_above_20d.sort(key=lambda r: r["sarane_ratio"], reverse=True)
        pol_to_avg20.sort(key=lambda r: r["pol_to_avg20_pct"], reverse=True)

        market_sarane_kharid = (
            total_value_buy_real / total_buyer_count if total_buyer_count > 0 else 0.0
        )
        market_pol_to_avg20_pct = (
            total_pol_hagigi / total_value_avg20 * 100 if total_value_avg20 > 0 else 0.0
        )

        return {
            "value_above_avg": value_above_avg,
            "sarane_above_20d": sarane_above_20d,
            "pol_to_avg20": pol_to_avg20,
            "totals": {
                "total_value": total_value,
                "total_pol_hagigi": total_pol_hagigi,
                "market_sarane_kharid": market_sarane_kharid,
                "market_pol_to_avg20_pct": market_pol_to_avg20_pct,
            },
        }

    def fetch_and_analyze(self) -> Optional[Dict]:
        """میان‌بر: fetch() + analyze() با هم؛ اگه fetch شکست بخوره None برمی‌گردونه."""
        records = self.fetch()
        if not records:
            return None
        return self.analyze(records)
