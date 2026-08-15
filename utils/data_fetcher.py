import copy
import time
import requests
import pandas as pd
import logging
from typing import Optional, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ========================================
# تنظیمات fetch
# ========================================
FETCH_TIMEOUT = 10          # ثانیه
FETCH_RETRIES = 1           # یک‌بار retry
FETCH_RETRY_DELAY = 1       # ثانیه بین retry‌ها
FETCH_MAX_WORKERS = 12      # تعداد thread‌های موازی (صنایع + صندوق‌ها با هم، ~48 درخواست)

# بازه‌ی زمانی پیش‌فرض برای اندپوینت snapshot (روز معاملاتی)
SNAPSHOT_TIMEFRAME = 12


# ========================================
# helper سراسری: خوندن مقدار از دیکشنری تودرتو با دات-پث
# مثال: get_path(row, "live.market.prices.close")
# ========================================
def get_path(d: dict, path: str, default=None):
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


# ========================================
# mapping: یک ردیف از JSON جدید (endpoint /snapshot) -> همون شکل ستون‌های
# قدیمی (api1_columns سابق) که data_processor.py انتظارشون رو داره.
#
# نکته‌ی مهم: همه‌ی مقادیر پولی اینجا خام (ریال) برگردونده می‌شن - دقیقاً
# مثل رفتار CSV قدیمی. تقسیم به تومان/میلیون/میلیارد همونطور که قبلاً هم
# بود توی data_processor._clean_and_prepare_api1 انجام می‌شه، اینجا لازم
# نیست تکرار بشه.
#
# فیلدهای "buy_order" / "buy_queue_value" / "ceiling_price" جایگزین API
# دوم (BrsApi) هستن؛ از سطح اول صف سفارش (levels[0]) و سقف قیمت مجاز
# ساخته می‌شن (فقط برای سهام - صندوق‌ها ceiling_price ندارن).
# ========================================
def map_snapshot_row_to_old_schema(row: dict) -> dict:
    kind = row.get("kind")
    is_fund = kind != "STOCK"

    if is_fund:
        marketcap = get_path(row, "live.market.valuation.netAsset")
        value_to_marketcap = get_path(row, "live.market.valuation.valueToNetAssetPercent")
    else:
        marketcap = get_path(row, "live.market.valuation.marketValue")
        value_to_marketcap = get_path(row, "live.market.valuation.valueToMarketValuePercent")

    levels = get_path(row, "live.market.orders.levels", []) or []
    lvl1 = levels[0] if levels else {}
    bid_price = lvl1.get("bidPrice")
    bid_volume = lvl1.get("bidVolume")
    bid_count = lvl1.get("bidCount")

    if bid_price and bid_volume:
        buy_queue_value = bid_price * bid_volume  # خام (ریال) - ارزش کل صف خرید سطح ۱
    else:
        buy_queue_value = 0

    if bid_price and bid_volume and bid_count:
        buy_order = (bid_price * bid_volume) / bid_count  # خام (ریال) - سرانه‌ی هر سفارش
    else:
        buy_order = 0

    return {
        "id": row.get("id"),
        "symbol": row.get("symbol"),

        "volume": get_path(row, "live.market.trading.volume"),
        "value": get_path(row, "live.market.trading.value"),

        "first_price": get_path(row, "live.market.prices.open"),
        "first_price_change_percent": get_path(row, "live.market.changes.openPercent"),
        "high_price": get_path(row, "live.market.prices.high"),
        "high_price_change_percent": get_path(row, "live.market.changes.highPercent"),
        "low_price": get_path(row, "live.market.prices.low"),
        "low_price_change_percent": get_path(row, "live.market.changes.lowPercent"),
        "last_price": get_path(row, "live.market.prices.close"),
        "last_price_change_percent": get_path(row, "live.market.changes.closePercent"),
        "final_price": get_path(row, "live.market.prices.closing"),
        "final_price_change_percent": get_path(row, "live.market.changes.closingPercent"),

        # قبلاً مستقیم توی CSV بود؛ معادلش درصد فاصله‌ی آخرین قیمت تا پایانی‌ست
        "diff_last_final": get_path(row, "live.market.changes.closeToClosingPercent"),

        "volatility": None,  # هیچ‌جای فیلترها/آلارم‌ها استفاده نمی‌شه

        "sarane_kharid": get_path(row, "live.market.clientType.realBuyPerCapitaValue"),
        "sarane_forosh": get_path(row, "live.market.clientType.realSellPerCapitaValue"),
        "godrat_kharid": get_path(row, "live.market.clientType.buyPower"),
        "pol_hagigi": get_path(row, "live.market.clientType.moneyFlowValue"),

        "buy_order_value": get_path(row, "live.market.orders.buyValue"),
        "sell_order_value": get_path(row, "live.market.orders.sellValue"),
        "diff_buy_sell_order": get_path(row, "live.market.orders.netValue"),

        "avg_5_day_pol_hagigi": get_path(row, "static.marketHistory.averageMoneyFlow.5"),
        "avg_20_day_pol_hagigi": get_path(row, "static.marketHistory.averageMoneyFlow.20"),
        "avg_60_day_pol_hagigi": get_path(row, "static.marketHistory.averageMoneyFlow.60"),

        "5_day_pol_hagigi": get_path(row, "static.marketHistory.cumulativeMoneyFlow.5"),
        "20_day_pol_hagigi": get_path(row, "static.marketHistory.cumulativeMoneyFlow.20"),
        "60_day_pol_hagigi": get_path(row, "static.marketHistory.cumulativeMoneyFlow.60"),

        "5_day_godrat_kharid": get_path(row, "static.marketHistory.buyPower.5"),
        "20_day_godrat_kharid": get_path(row, "static.marketHistory.buyPower.20"),

        "avg_monthly_value": get_path(row, "static.marketHistory.averageValue.20"),
        "value_to_avg_monthly_value": get_path(row, "live.market.historyDerived.valueToAverage.20"),
        "avg_3_month_value": get_path(row, "static.marketHistory.averageValue.60"),
        "value_to_avg_3_month_value": get_path(row, "live.market.historyDerived.valueToAverage.60"),
        "avg_5_day_value": get_path(row, "static.marketHistory.averageValue.5"),

        "5_day_return": get_path(row, "live.market.historyDerived.priceReturns.5"),
        "20_day_return": get_path(row, "live.market.historyDerived.priceReturns.20"),
        "60_day_return": get_path(row, "live.market.historyDerived.priceReturns.60"),

        "marketcap": marketcap,
        "value_to_marketcap": value_to_marketcap,

        "col51": None,  # هیچ‌جا استفاده نمی‌شه

        # --- فقط صندوق‌ها: NAV / حباب ---
        "bubble_percent": get_path(row, "live.fund.bubblePercent") if is_fund else None,
        "avg_1_month_bubble": get_path(row, "static.fund.bubbleHistory.average.20") if is_fund else None,

        # --- جایگزین API دوم (BrsApi) برای فیلتر ۱۰ ---
        "buy_order": buy_order,              # خام (ریال) - سرانه‌ی هر سفارش صف خرید سطح ۱
        "buy_queue_value": buy_queue_value,   # خام (ریال) - ارزش کل صف خرید سطح ۱
        "ceiling_price": get_path(row, "static.fundamentals.highThreshold"),  # فقط سهام
    }


class UnifiedDataFetcher:
    """کلاس دریافت داده از endpoint جدید tradersarena (data/industries/{slug}/snapshot)"""

    def __init__(self, api1_base_url: str = None, snapshot_timeframe: int = SNAPSHOT_TIMEFRAME):
        self.api1_base_url = api1_base_url
        self.snapshot_timeframe = snapshot_timeframe

        self.session_api1 = requests.Session()
        self.session_api1.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
        })

        try:
            from config import FUND_TYPES
            # deepcopy تا هر instance نسخه‌ی مستقل خودش رو داشته باشه؛
            # وگرنه mutate کردن fund_types روی یک instance (مثلا غیرفعال کردن یک صندوق)
            # روی dict مشترک ماژول config و در نتیجه روی همه‌ی instance‌های دیگه هم اثر می‌ذاشت.
            self.fund_types: Dict[str, Dict] = copy.deepcopy(FUND_TYPES)
        except ImportError:
            logger.warning("⚠️ FUND_TYPES در config یافت نشد - از لیست پیش‌فرض صندوق‌ها استفاده می‌شود")
            self.fund_types = {
                "index": {"slug": "index-funds", "name": "صندوق‌های شاخصی", "enabled": True},
                "real_state": {"slug": "real-state-funds", "name": "صندوق‌های املاک", "enabled": True},
                "fund_in_fund": {"slug": "fund-in-funds", "name": "صندوق‌های فراصندوق", "enabled": True},
                "classic_stock": {"slug": "classic-stock-funds", "name": "صندوق‌های سهامی کلاسیک", "enabled": True},
                "mixed": {"slug": "mixed-funds", "name": "صندوق‌های مختلط", "enabled": True},
                "energy": {"slug": "energy-funds", "name": "صندوق‌های انرژی", "enabled": True},
                "leveraged": {"slug": "leveraged-funds", "name": "صندوق‌های اهرمی", "enabled": True},
                "sector": {"slug": "sector-funds", "name": "صندوق‌های بخشی", "enabled": True},
            }

    # ========================================
    # هلپر: GET با retry
    # ========================================

    def _get_with_retry(self, url: str, label: str = "", params: dict = None) -> Optional[requests.Response]:
        """
        GET با timeout کوتاه و یک retry خودکار.
        برای 504 و Timeout سریع fail می‌کنه به جای انتظار طولانی.
        """
        for attempt in range(FETCH_RETRIES + 1):
            try:
                response = self.session_api1.get(url, params=params, timeout=FETCH_TIMEOUT)

                if response.status_code == 200:
                    return response

                if response.status_code in (504, 502, 503):
                    logger.warning(
                        f"⚠️ {label}: HTTP {response.status_code}"
                        f" (attempt {attempt + 1}/{FETCH_RETRIES + 1})"
                    )
                else:
                    logger.warning(f"⚠️ {label}: HTTP {response.status_code}")
                    return None  # خطاهای غیر-5xx قابل retry نیستن

            except requests.exceptions.Timeout:
                logger.warning(
                    f"⏱️ {label}: Timeout"
                    f" (attempt {attempt + 1}/{FETCH_RETRIES + 1})"
                )
            except requests.exceptions.ConnectionError as e:
                logger.error(f"❌ {label}: Connection error: {e}")
                return None

            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)

        logger.error(f"❌ {label}: همه {FETCH_RETRIES + 1} تلاش ناموفق بود")
        return None

    # ========================================
    # پارس کردن response جدید (dict با کلید "rows") به list of dicts خام
    # ========================================

    def _parse_response(self, response: requests.Response) -> List[Dict]:
        try:
            json_data = response.json()
            rows = json_data.get("rows", []) if isinstance(json_data, dict) else []
            return rows if isinstance(rows, list) else []
        except Exception as e:
            logger.error(f"❌ خطا در parse JSON: {e}")
            return []

    # ========================================
    # fetch یک صنعت/صندوق از endpoint جدید (thread-safe)
    # ========================================

    def _fetch_slug_data(self, slug: str, label: str) -> List[Dict]:
        """
        fetch از /data/industries/{slug}/snapshot?timeframe=..&_=<cache-buster>
        هم برای کد صنعت (مثلا '01') و هم برای slug صندوق (مثلا 'leveraged-funds') کار می‌کنه.
        """
        url = f"{self.api1_base_url}/data/industries/{slug}/snapshot"
        params = {"timeframe": self.snapshot_timeframe, "_": int(time.time() * 1000)}
        response = self._get_with_retry(url, label=label, params=params)
        if response is None:
            return []
        return self._parse_response(response)

    # ========================================
    # API اول - موازی
    # ========================================

    def fetch_from_api1(self, industry_codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        دریافت موازی داده از endpoint جدید.
        صنایع و همه‌ی انواع صندوق (FUND_TYPES) در یک batch واحد،
        با همون ThreadPoolExecutor و همزمان با هم fetch می‌شن.
        """
        try:
            if industry_codes is None:
                from config import INDUSTRY_CODES, INDUSTRY_NAMES
                industry_codes = INDUSTRY_CODES
            else:
                from config import INDUSTRY_NAMES

            enabled_funds = {
                key: cfg for key, cfg in self.fund_types.items()
                if cfg.get("enabled", True)
            }

            logger.info(
                f"📥 دریافت موازی از endpoint جدید "
                f"({len(industry_codes)} صنعت + {len(enabled_funds)} نوع صندوق، "
                f"max_workers={FETCH_MAX_WORKERS})..."
            )

            all_rows: List[Dict] = []
            success_count = 0
            fail_count = 0

            # ----------------------------------------
            # fetch موازی صنایع + صندوق‌ها در یک batch
            # ----------------------------------------
            with ThreadPoolExecutor(max_workers=FETCH_MAX_WORKERS) as executor:
                future_to_task: Dict = {}

                for code in industry_codes:
                    future = executor.submit(self._fetch_slug_data, code, f"صنعت {code}")
                    future_to_task[future] = ("industry", code)

                for key, cfg in enabled_funds.items():
                    future = executor.submit(self._fetch_slug_data, cfg["slug"], cfg["name"])
                    future_to_task[future] = ("fund", key)

                for future in as_completed(future_to_task):
                    task_type, key = future_to_task[future]
                    label = f"صنعت {key}" if task_type == "industry" else self.fund_types[key]["name"]

                    try:
                        raw_rows = future.result()
                    except Exception as e:
                        logger.error(f"❌ {label}: خطای غیرمنتظره: {e}")
                        fail_count += 1
                        continue

                    if not raw_rows:
                        fail_count += 1
                        continue

                    rows = [map_snapshot_row_to_old_schema(r) for r in raw_rows]

                    if task_type == "industry":
                        for row_dict in rows:
                            row_dict["industry_code"] = key
                            row_dict["industry_name"] = INDUSTRY_NAMES.get(key, "نامشخص")
                            row_dict["is_fund"] = False
                            row_dict["fund_type"] = None
                    else:
                        cfg = self.fund_types[key]
                        for row_dict in rows:
                            row_dict["industry_code"] = cfg["slug"]
                            row_dict["industry_name"] = cfg["name"]
                            row_dict["is_fund"] = True
                            row_dict["fund_type"] = key

                    all_rows.extend(rows)
                    success_count += 1

            logger.info(
                f"  ✅ {success_count} موفق، {fail_count} ناموفق "
                f"(از {len(future_to_task)} درخواست، {len(all_rows)} رکورد)"
            )

            # ----------------------------------------
            # DataFrame نهایی
            # ----------------------------------------
            if not all_rows:
                logger.warning("⚠️ هیچ داده‌ای دریافت نشد")
                return None

            df = pd.DataFrame(all_rows)

            total_stocks = len(df[df["is_fund"] == False])
            logger.info(f"✅ {len(df)} رکورد")
            logger.info(f"    • سهام صنایع: {total_stocks}")
            for key, cfg in enabled_funds.items():
                count = len(df[df["fund_type"] == key])
                logger.info(f"    • {cfg['name']}: {count}")

            return df

        except ImportError:
            logger.error("❌ خطا در import INDUSTRY_CODES از config")
            return None
        except Exception as e:
            logger.error(f"❌ خطا در fetch_from_api1: {e}")
            return None

    # ========================================
    # fetch همه‌ی داده
    # ========================================

    def fetch_all_data(self, industry_codes: List[str] = None) -> Optional[pd.DataFrame]:
        logger.info("=" * 80)
        logger.info("🚀 شروع دریافت داده")
        logger.info("=" * 80)

        t0 = time.time()
        df = self.fetch_from_api1(industry_codes)
        t1 = time.time()

        logger.info("\n" + "=" * 80)
        logger.info("📊 خلاصه دریافت داده:")
        logger.info(
            f"  • کل: {len(df) if df is not None else 0} رکورد"
            f"  ⏱️ {t1 - t0:.1f}s"
        )
        if df is not None and not df.empty:
            logger.info(f"    - سهام: {len(df[df['is_fund'] == False])}")
            logger.info(f"    - صندوق‌ها: {len(df[df['is_fund'] == True])}")
        logger.info("=" * 80)

        return df

    # ========================================
    # اعتبارسنجی
    # ========================================

    def validate_api1_data(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False
        required = [
            "symbol", "last_price", "final_price",
            "value_to_avg_monthly_value", "sarane_kharid",
            "godrat_kharid", "pol_hagigi",
        ]
        missing = [c for c in required if c not in df.columns]
        if missing:
            logger.warning(f"⚠️ ستون‌های گمشده: {missing}")
            return False
        return True
