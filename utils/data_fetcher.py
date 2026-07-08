import copy
import requests
import pandas as pd
import logging
from typing import Optional, Dict, List, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ========================================
# تنظیمات fetch
# ========================================
FETCH_TIMEOUT = 10          # ثانیه (به جای 30)
FETCH_RETRIES = 1           # یک‌بار retry
FETCH_RETRY_DELAY = 1       # ثانیه بین retry‌ها
FETCH_MAX_WORKERS = 12      # تعداد thread‌های موازی (صنایع + صندوق‌ها با هم، ~48 درخواست)


class UnifiedDataFetcher:
    """کلاس یکپارچه برای دریافت داده از هر دو API"""

    def __init__(self, api1_base_url: str = None, api2_key: str = None):
        self.api1_base_url = api1_base_url

        self.api2_key = api2_key
        self.api2_base_url = "https://Api.BrsApi.ir/Tsetmc"

        self.api1_columns = [
            "id", "symbol", "volume", "value",
            "first_price", "first_price_change_percent",
            "high_price", "high_price_change_percent",
            "low_price", "low_price_change_percent",
            "last_price", "last_price_change_percent",
            "final_price", "final_price_change_percent",
            "diff_last_final", "volatility",
            "sarane_kharid", "sarane_forosh", "godrat_kharid",
            "pol_hagigi",
            "buy_order_value", "sell_order_value", "diff_buy_sell_order",
            "avg_5_day_pol_hagigi", "avg_20_day_pol_hagigi", "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi", "20_day_pol_hagigi", "60_day_pol_hagigi",
            "5_day_godrat_kharid", "20_day_godrat_kharid",
            "avg_monthly_value", "value_to_avg_monthly_value",
            "avg_3_month_value", "value_to_avg_3_month_value",
            "5_day_return", "20_day_return", "60_day_return",
            "marketcap", "value_to_marketcap", "col51",
        ]

        self.session_api1 = requests.Session()
        self.session_api1.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
        })

        self.headers_api2 = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

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

    def _get_with_retry(self, url: str, label: str = "") -> Optional[requests.Response]:
        """
        GET با timeout کوتاه و یک retry خودکار.
        برای 504 و Timeout سریع fail می‌کنه به جای انتظار طولانی.
        """
        for attempt in range(FETCH_RETRIES + 1):
            try:
                response = self.session_api1.get(url, timeout=FETCH_TIMEOUT)

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

            # اگر retry باقی مانده
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)

        logger.error(f"❌ {label}: همه {FETCH_RETRIES + 1} تلاش ناموفق بود")
        return None

    # ========================================
    # پارس کردن response به list of dicts
    # ========================================

    def _parse_response(self, response: requests.Response) -> List[Dict]:
        try:
            json_data = response.json()
            data = (
                json_data["data"]
                if isinstance(json_data, dict) and "data" in json_data
                else json_data
            )
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"❌ خطا در parse JSON: {e}")
            return []

    def _rows_to_dicts(self, data: List) -> List[Dict]:
        result = []
        for row in data:
            if isinstance(row, list):
                row_dict = dict(zip(self.api1_columns, row[: len(self.api1_columns)]))
            else:
                row_dict = row.copy()
            result.append(row_dict)
        return result

    # ========================================
    # fetch یک صنعت (thread-safe)
    # ========================================

    def _fetch_slug_data(self, slug: str, label: str) -> List[Dict]:
        """
        fetch عمومی برای هر endpoint زیر industries-stocks-csv/{slug}.
        هم برای کد صنعت (مثلا '01') و هم برای slug صندوق (مثلا 'index-funds') کار می‌کنه.
        """
        url = f"{self.api1_base_url}/data/industries-stocks-csv/{slug}"
        response = self._get_with_retry(url, label=label)
        if response is None:
            return []
        return self._parse_response(response)

    # ========================================
    # API اول - موازی
    # ========================================

    def fetch_from_api1(self, industry_codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        دریافت موازی داده از API اول.
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
                f"📥 دریافت موازی API اول "
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
                        data = future.result()
                    except Exception as e:
                        logger.error(f"❌ {label}: خطای غیرمنتظره: {e}")
                        fail_count += 1
                        continue

                    if not data:
                        fail_count += 1
                        continue

                    rows = self._rows_to_dicts(data)
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
                logger.warning("⚠️ API اول: هیچ داده‌ای دریافت نشد")
                return None

            df = pd.DataFrame(all_rows)

            total_stocks = len(df[df["is_fund"] == False])
            logger.info(f"✅ API اول: {len(df)} رکورد")
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
    # API دوم - لحظه‌ای (بدون تغییر منطقی)
    # ========================================

    def fetch_from_api2(self) -> Optional[pd.DataFrame]:
        if not self.api2_key:
            logger.warning("⚠️ کلید API دوم تنظیم نشده")
            return None

        url = f"{self.api2_base_url}/AllSymbols.php?key={self.api2_key}"

        try:
            logger.info("📥 دریافت داده از API دوم (لحظه‌ای)...")
            response = requests.get(
                url,
                headers=self.headers_api2,
                timeout=FETCH_TIMEOUT * 3,   # API دوم یک request بزرگه، timeout بیشتر
            )

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    logger.info(f"✅ API دوم: {len(df)} نماد")
                    return df
                else:
                    logger.error("❌ API دوم: داده خالی یا فرمت نامعتبر")
                    return None
            else:
                logger.error(f"❌ API دوم: HTTP {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            logger.error("❌ API دوم: Timeout")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("❌ API دوم: خطای اتصال")
            return None
        except Exception as e:
            logger.error(f"❌ API دوم: {e}")
            return None

    def fetch_symbol_details_api2(self, symbol: str) -> Optional[Dict]:
        if not self.api2_key:
            return None
        url = f"{self.api2_base_url}/SymbolDetails.php?key={self.api2_key}&symbol={symbol}"
        try:
            response = requests.get(url, headers=self.headers_api2, timeout=FETCH_TIMEOUT)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"❌ {symbol}: {e}")
            return None

    # ========================================
    # fetch هر دو API
    # ========================================

    def fetch_all_data(
        self, industry_codes: List[str] = None
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        logger.info("=" * 80)
        logger.info("🚀 شروع دریافت داده از هر دو API")
        logger.info("=" * 80)

        t0 = time.time()
        df_api1 = self.fetch_from_api1(industry_codes)
        t1 = time.time()
        df_api2 = self.fetch_from_api2()
        t2 = time.time()

        logger.info("\n" + "=" * 80)
        logger.info("📊 خلاصه دریافت داده:")
        logger.info(
            f"  • API اول (فیلتر 1-9): "
            f"{len(df_api1) if df_api1 is not None else 0} رکورد"
            f"  ⏱️ {t1 - t0:.1f}s"
        )
        if df_api1 is not None and not df_api1.empty:
            logger.info(f"    - سهام: {len(df_api1[df_api1['is_fund'] == False])}")
            logger.info(f"    - صندوق‌ها: {len(df_api1[df_api1['is_fund'] == True])}")
        logger.info(
            f"  • API دوم (فیلتر 10): "
            f"{len(df_api2) if df_api2 is not None else 0} نماد"
            f"  ⏱️ {t2 - t1:.1f}s"
        )
        logger.info(f"  • ⏱️ کل زمان fetch: {t2 - t0:.1f}s")
        logger.info("=" * 80)

        return df_api1, df_api2

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
            logger.warning(f"⚠️ ستون‌های گمشده API اول: {missing}")
            return False
        return True

    def validate_api2_data(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False
        if "symbol" not in df.columns and "l18" not in df.columns:
            logger.warning("⚠️ ستون symbol/l18 در API دوم یافت نشد")
            return False
        return True
