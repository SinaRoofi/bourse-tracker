
import ast
import logging
import time
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

INDUSTRIES_CSV_URL = "https://tradersarena.ir/data/industries-csv"

FETCH_TIMEOUT = 10      # ثانیه
FETCH_RETRIES = 1       # یک‌بار retry
FETCH_RETRY_DELAY = 1   # ثانیه بین retry‌ها

# ریال -> میلیارد/میلیون/(هزار میلیارد=تریلیون) تومان
# (هم‌رسم با data_processor._clean_and_prepare_api1)
RIAL_TO_BILLION_TOMAN = 10_000_000_000
RIAL_TO_MILLION_TOMAN = 10_000_000
RIAL_TO_TRILLION_TOMAN = RIAL_TO_BILLION_TOMAN * 1000  # هزار میلیارد تومان

# صندوق‌هایی که طبق درخواست کاربر همیشه از خلاصه‌ی بازار کنار گذاشته می‌شن
EXCLUDED_CODES = {
    "gold-funds", "silver-funds", "fixed-income-funds",
    "saffron-funds", "energy-funds", "real-state-funds",
}

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


def _to_number(raw) -> float:
    if isinstance(raw, (int, float)):
        return float(raw)
    raw = (str(raw) or "").strip()
    if raw == "":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def parse_industries_response(text: str) -> List[Dict]:
    """
    پارس متن خام endpoint. تأیید شده که خروجی واقعی یه رشته‌ی تک‌خطی
    شبیه JSON (کوتیشن دوتایی، اعداد به فرمت علمی) هست که ast.literal_eval
    مستقیم پارسش می‌کنه - چون از نظر syntax با لیست پایتون یکیه.

    Returns:
        list[dict]: هر دیکشنری یک صنعت/صندوق با کلیدهای COLUMNS، به‌جز
        شش صندوق مستثنا. اگه پارس شکست بخوره لیست خالی برمی‌گردونه.
    """
    stripped = text.strip().lstrip("\ufeff")

    try:
        data = ast.literal_eval(stripped)
    except (ValueError, SyntaxError) as e:
        logger.error(f"❌ پارس خروجی صنایع شکست خورد - فرمت endpoint عوض شده: {e}")
        return []

    if not isinstance(data, (list, tuple)) or not data or not isinstance(data[0], (list, tuple)):
        logger.error("❌ خروجی صنایع لیست/تاپل نبود - فرمت endpoint عوض شده")
        return []

    records: List[Dict] = []
    excluded_found = 0
    skipped_short = 0

    for row in data:
        if len(row) < len(COLUMNS):
            skipped_short += 1
            continue

        record = {
            col: (str(row[i]).strip() if col in TEXT_COLUMNS else _to_number(row[i]))
            for i, col in enumerate(COLUMNS)
        }

        if record["code"] in EXCLUDED_CODES:
            excluded_found += 1
            continue

        records.append(record)

    if skipped_short:
        logger.warning(f"⚠️ {skipped_short} ردیف به‌خاطر تعداد ستون کم رد شدن")
    logger.info(
        f"✅ {len(records)} صنعت/صندوق دریافت شد "
        f"(بعد از حذف {excluded_found} صندوق مستثنا)"
    )
    return records


class IndustryMarketFetcher:
    """دریافت، پارس و تحلیل خلاصه‌ی صنایع/صندوق‌ها از tradersarena."""

    def __init__(self, url: str = INDUSTRIES_CSV_URL):
        self.url = url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv, text/plain, */*",
        })

    def close(self) -> None:
        """session رو صریحاً می‌بنده - این fetcher عمرش کوتاهه (یک fetch_and_analyze
        در هر اجرا)، پس بستنش تمیزتره و از warning‌های unclosed connection جلوگیری می‌کنه."""
        try:
            self.session.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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
            به‌جز شش صندوق مستثنا (طلا/نقره/درآمد ثابت/زعفران/انرژی/املاک).
        """
        response = self._get_with_retry()
        if response is None:
            return []

        response.encoding = response.encoding or "utf-8"
        records = parse_industries_response(response.text)

        # چک سلامت: اگه به‌طرز مشکوکی کم صنعت برگشت، احتمالاً فرمت
        # endpoint دوباره عوض شده - متن خام رو ذخیره می‌کنیم تا بشه بعداً
        # دقیقاً دید چی برگشته (به‌جای اینکه دوباره حدس بزنیم).
        if 0 < len(records) < 10:
            logger.warning(
                f"⚠️ فقط {len(records)} صنعت/صندوق پارس شد (انتظار ~۵۰-۶۰ تا می‌رفت) - "
                f"احتمالاً فرمت خروجی endpoint عوض شده. صنایع برگشته: "
                f"{[r['name'] for r in records]}"
            )
            self._dump_raw_response_for_debug(response.text)

        return records

    @staticmethod
    def _dump_raw_response_for_debug(text: str) -> None:
        """
        متن خام response رو تو یه فایل لوکال ذخیره می‌کنه تا اگه workflow
        این فایل رو به‌عنوان artifact آپلود کنه، دفعه‌ی بعد که چک سلامت
        بالا فعال شد، بتونیم دقیقاً ببینیم endpoint چه فرمتی برگردونده -
        به‌جای اینکه دوباره حدس بزنیم.
        """
        try:
            with open("industries_csv_raw_debug.txt", "w", encoding="utf-8") as f:
                f.write(text)
            logger.warning(
                "📝 متن خام در industries_csv_raw_debug.txt ذخیره شد "
                "(اگه workflow این فایل رو آپلود کنه، از artifact قابل دانلوده)"
            )
        except Exception as e:
            logger.error(f"❌ نتونستم متن خام رو برای دیباگ ذخیره کنم: {e}")

    # ------------------------------------------------------------------
    # تحلیل
    # ------------------------------------------------------------------
    @staticmethod
    def analyze(records: List[Dict]) -> Dict:
        """
        محاسبه‌ی خلاصه‌ی بازار روی رکوردهای برگشتی از fetch().
        اصطلاح «هفتگی» = ۵ روزه و «ماهانه» = ۲۰ روزه (تقریب رایج بازه‌ی
        معاملاتی هفته/ماه در بورس ایران).

        خروجی:
          - value_above_avg: صنایعی که ارزش امروزشون از میانگین هفتگی
            *و* ماهانه بیشتره (مرتب‌شده بر اساس pct_month، هر رکورد
            کلیدهای اضافه‌ی pct_week/pct_month داره)
          - sarane_above_month: صنایعی که سرانه خرید امروزشون از سرانه
            خرید ماهانه‌شون بیشتره (کلید اضافه‌ی sarane_ratio - نسبت
            امروز به ماهانه)
          - pol_to_avg_month: برای هر صنعت با میانگین ماهانه‌ی معتبر،
            ورود پول امروز به‌عنوان درصدی از اون میانگین (کلید اضافه‌ی
            pol_to_avg_month_pct)، مرتب نزولی (بیشترین ورود پول نسبی
            بالای لیست) - همین لیست به‌عنوان «قدرت پول صنایع» هم استفاده
            می‌شه
          - return_ranked: صنایع فعال (حجم>۰) مرتب‌شده نزولی بر اساس
            بازدهی هم‌وزن گروه
          - breadth: تعداد/سهم صنایع فعالی که هرکدوم از شرط‌های بالا رو
            داشتن (نبض بازار) - از روی همون صنایع فعال (حجم>۰) حساب می‌شه
          - totals: جمع کل بازار - ارزش و ورود پول کل، و سرانه خرید/فروش
            کل بازار (میانگین ساده‌ی سرانه‌ی صنایع فعال - نه وزن‌دار) به
            همراه نسبتشون (ratio) به میانگین ماهانه‌ی خودشون
        """
        value_above_avg: List[Dict] = []
        sarane_above_month: List[Dict] = []
        pol_to_avg_month: List[Dict] = []

        total_value = 0.0
        total_pol_hagigi = 0.0
        total_value_avg_month = 0.0

        # میانگین ساده‌ی سرانه‌ی خرید/فروش کل بازار - فقط روی صنایعی که
        # سرانه‌ی معتبر (>۰) دارن حساب می‌شه (صنایع بدون معامله وارد
        # میانگین نمی‌شن)
        sarane_kharid_sum = 0.0
        sarane_kharid_count = 0
        sarane_forosh_sum = 0.0
        sarane_forosh_count = 0
        sarane_kharid_month_sum = 0.0
        sarane_kharid_month_count = 0
        sarane_forosh_month_sum = 0.0
        sarane_forosh_month_count = 0

        for r in records:
            total_value += r["value"]
            total_pol_hagigi += r["pol_hagigi"]
            total_value_avg_month += r["value_avg20"]

            if r["sarane_kharid"] > 0:
                sarane_kharid_sum += r["sarane_kharid"]
                sarane_kharid_count += 1
            if r["sarane_forosh"] > 0:
                sarane_forosh_sum += r["sarane_forosh"]
                sarane_forosh_count += 1
            if r["sarane_kharid_20d"] > 0:
                sarane_kharid_month_sum += r["sarane_kharid_20d"]
                sarane_kharid_month_count += 1
            if r["sarane_forosh_20d"] > 0:
                sarane_forosh_month_sum += r["sarane_forosh_20d"]
                sarane_forosh_month_count += 1

            if r["value_vs_avg5_pct"] > 0 and r["value_vs_avg20_pct"] > 0:
                value_above_avg.append({
                    **r,
                    "pct_week": r["value_vs_avg5_pct"],
                    "pct_month": r["value_vs_avg20_pct"],
                })

            if r["sarane_kharid_20d"] > 0 and r["sarane_kharid"] > r["sarane_kharid_20d"]:
                sarane_above_month.append({
                    **r,
                    "sarane_ratio": r["sarane_kharid"] / r["sarane_kharid_20d"],
                })

            if r["value_avg20"] > 0:
                pol_to_avg_month.append({
                    **r,
                    "pol_to_avg_month_pct": (r["pol_hagigi"] / r["value_avg20"]) * 100,
                })

        value_above_avg.sort(key=lambda r: r["pct_month"], reverse=True)
        sarane_above_month.sort(key=lambda r: r["sarane_ratio"], reverse=True)
        pol_to_avg_month.sort(key=lambda r: r["pol_to_avg_month_pct"], reverse=True)

        # صنایع بدون معامله (حجم صفر) رو از رتبه‌بندی بازدهی کنار می‌ذاریم -
        # وگرنه یه صنعت راکد با مقدار صفر می‌تونه به‌اشتباه بالای لیست
        # بیفته (چون صفر از خیلی از مقادیر منفی روزهای ضعیف بزرگ‌تره)
        active_records = [r for r in records if r["volume"] > 0]
        return_ranked = sorted(active_records, key=lambda r: r["group_return_equal_weight"], reverse=True)

        total_active = len(active_records)
        breadth = {
            "total_active": total_active,
            "value_above_count": sum(
                1 for r in active_records
                if r["value_vs_avg5_pct"] > 0 and r["value_vs_avg20_pct"] > 0
            ),
            "pol_positive_count": sum(
                1 for r in active_records
                if r["value_avg20"] > 0 and r["pol_hagigi"] > 0
            ),
            "sarane_above_count": sum(
                1 for r in active_records
                if r["sarane_kharid_20d"] > 0 and r["sarane_kharid"] > r["sarane_kharid_20d"]
            ),
            "return_positive_count": sum(
                1 for r in active_records if r["group_return_equal_weight"] > 0
            ),
        }

        market_sarane_kharid = (
            sarane_kharid_sum / sarane_kharid_count if sarane_kharid_count > 0 else 0.0
        )
        market_sarane_kharid_month = (
            sarane_kharid_month_sum / sarane_kharid_month_count
            if sarane_kharid_month_count > 0 else 0.0
        )
        market_sarane_kharid_ratio = (
            market_sarane_kharid / market_sarane_kharid_month
            if market_sarane_kharid_month > 0 else 0.0
        )

        market_sarane_forosh = (
            sarane_forosh_sum / sarane_forosh_count if sarane_forosh_count > 0 else 0.0
        )
        market_sarane_forosh_month = (
            sarane_forosh_month_sum / sarane_forosh_month_count
            if sarane_forosh_month_count > 0 else 0.0
        )
        market_sarane_forosh_ratio = (
            market_sarane_forosh / market_sarane_forosh_month
            if market_sarane_forosh_month > 0 else 0.0
        )

        market_pol_to_avg_month_pct = (
            total_pol_hagigi / total_value_avg_month * 100 if total_value_avg_month > 0 else 0.0
        )

        return {
            "value_above_avg": value_above_avg,
            "sarane_above_month": sarane_above_month,
            "pol_to_avg_month": pol_to_avg_month,
            "return_ranked": return_ranked,
            "breadth": breadth,
            "totals": {
                "total_value": total_value,
                "total_pol_hagigi": total_pol_hagigi,
                "market_sarane_kharid": market_sarane_kharid,
                "market_sarane_kharid_month": market_sarane_kharid_month,
                "market_sarane_kharid_ratio": market_sarane_kharid_ratio,
                "market_sarane_forosh": market_sarane_forosh,
                "market_sarane_forosh_month": market_sarane_forosh_month,
                "market_sarane_forosh_ratio": market_sarane_forosh_ratio,
                "market_pol_to_avg_month_pct": market_pol_to_avg_month_pct,
            },
        }

    def fetch_and_analyze(self) -> Optional[Dict]:
        """میان‌بر: fetch() + analyze() با هم؛ اگه fetch شکست بخوره None برمی‌گردونه."""
        records = self.fetch()
        if not records:
            return None
        return self.analyze(records)
