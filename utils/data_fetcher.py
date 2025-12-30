import requests
import pandas as pd
import logging
from typing import Optional, Dict, List, Tuple
import time

logger = logging.getLogger(__name__)


class UnifiedDataFetcher:
    """کلاس یکپارچه برای دریافت داده از هر دو API"""

    def __init__(self, api1_base_url: str = None, api2_key: str = None):
        """
        Args:
            api1_base_url: آدرس پایه API اول
            api2_key: کلید API دوم (BrsApi.ir)
        """
        # API اول
        self.api1_base_url = api1_base_url 

        # API دوم (BrsApi)
        self.api2_key = api2_key
        self.api2_base_url = "https://BrsApi.ir/Api/Tsetmc"

        # ستون‌های API اول (40 ستون اصلی - مشترک بین صنایع و صندوق‌ها)
        self.api1_columns = [
            "id",
            "symbol",
            "volume",
            "value",
            "first_price",
            "first_price_change_percent",
            "high_price",
            "high_price_change_percent",
            "low_price",
            "low_price_change_percent",
            "last_price",
            "last_price_change_percent",
            "final_price",
            "final_price_change_percent",
            "diff_last_final",
            "volatility",
            "sarane_kharid",
            "sarane_forosh",
            "godrat_kharid",
            "pol_hagigi",
            "buy_order_value",
            "sell_order_value",
            "diff_buy_sell_order",
            "avg_5_day_pol_hagigi",
            "avg_20_day_pol_hagigi",
            "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi",
            "20_day_pol_hagigi",
            "60_day_pol_hagigi",
            "5_day_godrat_kharid",
            "20_day_godrat_kharid",
            "avg_monthly_value",
            "value_to_avg_monthly_value",
            "avg_3_month_value",
            "value_to_avg_3_month_value",
            "5_day_return",
            "20_day_return",
            "60_day_return",
            "marketcap",
            "value_to_marketcap",
            "col51",
        ]

        # Session برای API اول
        self.session_api1 = requests.Session()
        self.session_api1.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
        })

        # هدرها برای API دوم
        self.headers_api2 = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/106.0.0.0",
            "Accept": "application/json, text/plain, */*"
        }

        # تنظیمات صندوق‌ها (خوانده می‌شود از config)
        try:
            from config import INCLUDE_LEVERAGED_FUNDS, INCLUDE_SECTOR_FUNDS
            self.include_leveraged_funds = INCLUDE_LEVERAGED_FUNDS
            self.include_sector_funds = INCLUDE_SECTOR_FUNDS
        except ImportError:
            # اگر در config نبود، به صورت پیش‌فرض فعال است
            logger.warning("⚠️ INCLUDE_LEVERAGED_FUNDS و INCLUDE_SECTOR_FUNDS در config یافت نشد - از مقادیر پیش‌فرض استفاده می‌شود")
            self.include_leveraged_funds = True
            self.include_sector_funds = True

    # ========================================
    # API اول - داده‌های تاریخی
    # ========================================

    def _fetch_industry_data(self, industry_code: str) -> List[Dict]:
        """
        دریافت داده یک صنعت از API اول
        
        Args:
            industry_code: کد صنعت (مثلاً "27")
            
        Returns:
            لیست دیکشنری اطلاعات سهام
        """
        url = f"{self.api1_base_url}/data/industries-stocks-csv/{industry_code}"

        try:
            response = self.session_api1.get(url, timeout=30)

            if response.status_code != 200:
                logger.warning(f"⚠️ خطا در دریافت صنعت {industry_code}: {response.status_code}")
                return []

            json_data = response.json()
            data = json_data["data"] if isinstance(json_data, dict) and "data" in json_data else json_data

            return data

        except Exception as e:
            logger.error(f"❌ خطا در دریافت صنعت {industry_code}: {e}")
            return []

    def _fetch_leveraged_funds_data(self) -> List[Dict]:
        """
        دریافت داده صندوق‌های اهرمی از API اول
        
        Returns:
            لیست دیکشنری اطلاعات صندوق‌های اهرمی
        """
        url = f"{self.api1_base_url}/data/industries-stocks-csv/leveraged-funds"

        try:
            response = self.session_api1.get(url, timeout=30)

            if response.status_code != 200:
                logger.warning(f"⚠️ خطا در دریافت صندوق‌های اهرمی: {response.status_code}")
                return []

            json_data = response.json()
            data = json_data["data"] if isinstance(json_data, dict) and "data" in json_data else json_data

            return data

        except Exception as e:
            logger.error(f"❌ خطا در دریافت صندوق‌های اهرمی: {e}")
            return []

    def _fetch_sector_funds_data(self) -> List[Dict]:
        """
        دریافت داده صندوق‌های بخشی از API اول
        
        Returns:
            لیست دیکشنری اطلاعات صندوق‌های بخشی
        """
        url = f"{self.api1_base_url}/data/industries-stocks-csv/sector-funds"

        try:
            response = self.session_api1.get(url, timeout=30)

            if response.status_code != 200:
                logger.warning(f"⚠️ خطا در دریافت صندوق‌های بخشی: {response.status_code}")
                return []

            json_data = response.json()
            data = json_data["data"] if isinstance(json_data, dict) and "data" in json_data else json_data

            return data

        except Exception as e:
            logger.error(f"❌ خطا در دریافت صندوق‌های بخشی: {e}")
            return []

    def fetch_from_api1(self, industry_codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        دریافت داده از API اول
        
        این API شامل:
        - صنایع: میانگین‌های تاریخی، بازده‌ها، قدرت خرید، سرانه، پول حقیقی
        - صندوق‌های اهرمی (leveraged-funds)
        - صندوق‌های بخشی (sector-funds)
        - استفاده برای فیلترهای 1 تا 9
        
        Args:
            industry_codes: لیست کدهای صنعت (اگر None باشد از config استفاده می‌شود)
            
        Returns:
            DataFrame یکپارچه شامل صنایع + صندوق‌های اهرمی + صندوق‌های بخشی
        """
        try:
            logger.info("📥 دریافت داده از API اول (فیلترهای 1-9)...")

            # اگر کدهای صنعت داده نشده، از config استفاده کن
            if industry_codes is None:
                from config import INDUSTRY_CODES, INDUSTRY_NAMES
                industry_codes = INDUSTRY_CODES
            else:
                from config import INDUSTRY_NAMES

            all_rows = []
            total_sources = len(industry_codes) + (1 if self.include_leveraged_funds else 0) + (1 if self.include_sector_funds else 0)
            current_source = 0

            # ========================================
            # 1. دریافت داده صنایع
            # ========================================
            total_industries = len(industry_codes)
            for idx, code in enumerate(industry_codes, 1):
                current_source += 1
                logger.info(f"  📊 [{current_source}/{total_sources}] دریافت صنعت {code} ({idx}/{total_industries})...")

                data = self._fetch_industry_data(code)

                if not data:
                    continue

                # پردازش هر سهم
                for row in data:
                    # اگر row یک list بود → تبدیل به dict
                    if isinstance(row, list):
                        # فقط 40 ستون اول را بگیر
                        row_data = row[:len(self.api1_columns)]
                        row_dict = dict(zip(self.api1_columns, row_data))
                    else:
                        row_dict = row.copy()

                    # اضافه کردن اطلاعات شناسایی
                    row_dict["industry_code"] = code
                    row_dict["industry_name"] = INDUSTRY_NAMES.get(code, "نامشخص")
                    row_dict["is_fund"] = False
                    row_dict["fund_type"] = None
                    
                    all_rows.append(row_dict)

                # تاخیر کوچک برای جلوگیری از rate limit
                time.sleep(0.1)

            # ========================================
            # 2. دریافت صندوق‌های اهرمی
            # ========================================
            if self.include_leveraged_funds:
                current_source += 1
                logger.info(f"  📊 [{current_source}/{total_sources}] دریافت صندوق‌های اهرمی...")
                
                leveraged_data = self._fetch_leveraged_funds_data()
                
                if leveraged_data:
                    for row in leveraged_data:
                        # اگر row یک list بود → تبدیل به dict
                        if isinstance(row, list):
                            # فقط 40 ستون اول را بگیر (ستون‌های اضافی نادیده گرفته می‌شوند)
                            row_data = row[:len(self.api1_columns)]
                            row_dict = dict(zip(self.api1_columns, row_data))
                        else:
                            row_dict = row.copy()

                        # اضافه کردن اطلاعات شناسایی
                        row_dict["industry_code"] = "leveraged-funds"
                        row_dict["industry_name"] = "صندوق‌های اهرمی"
                        row_dict["is_fund"] = True
                        row_dict["fund_type"] = "leveraged"
                        
                        all_rows.append(row_dict)
                    
                    logger.info(f"    ✅ {len(leveraged_data)} صندوق اهرمی دریافت شد")
                else:
                    logger.warning("    ⚠️ هیچ صندوق اهرمی یافت نشد")

                time.sleep(0.1)

            # ========================================
            # 3. دریافت صندوق‌های بخشی
            # ========================================
            if self.include_sector_funds:
                current_source += 1
                logger.info(f"  📊 [{current_source}/{total_sources}] دریافت صندوق‌های بخشی...")
                
                sector_data = self._fetch_sector_funds_data()
                
                if sector_data:
                    for row in sector_data:
                        # اگر row یک list بود → تبدیل به dict
                        if isinstance(row, list):
                            # فقط 40 ستون اول را بگیر (ستون‌های اضافی نادیده گرفته می‌شوند)
                            row_data = row[:len(self.api1_columns)]
                            row_dict = dict(zip(self.api1_columns, row_data))
                        else:
                            row_dict = row.copy()

                        # اضافه کردن اطلاعات شناسایی
                        row_dict["industry_code"] = "sector-funds"
                        row_dict["industry_name"] = "صندوق‌های بخشی"
                        row_dict["is_fund"] = True
                        row_dict["fund_type"] = "sector"
                        
                        all_rows.append(row_dict)
                    
                    logger.info(f"    ✅ {len(sector_data)} صندوق بخشی دریافت شد")
                else:
                    logger.warning("    ⚠️ هیچ صندوق بخشی یافت نشد")

                time.sleep(0.1)

            # ========================================
            # 4. ساخت DataFrame نهایی
            # ========================================
            if not all_rows:
                logger.warning("⚠️ API اول: هیچ داده‌ای دریافت نشد")
                return None

            # ساخت DataFrame یکپارچه
            df = pd.DataFrame(all_rows)

            # شمارش نوع داده‌ها
            total_stocks = len(df[df["is_fund"] == False]) if "is_fund" in df.columns else 0
            total_leveraged = len(df[df["fund_type"] == "leveraged"]) if "fund_type" in df.columns else 0
            total_sector = len(df[df["fund_type"] == "sector"]) if "fund_type" in df.columns else 0

            logger.info(f"✅ API اول: {len(df)} رکورد دریافت شد")
            logger.info(f"    • سهام صنایع: {total_stocks}")
            logger.info(f"    • صندوق‌های اهرمی: {total_leveraged}")
            logger.info(f"    • صندوق‌های بخشی: {total_sector}")

            return df

        except ImportError:
            logger.error("❌ خطا در import کردن INDUSTRY_CODES از config")
            return None
        except Exception as e:
            logger.error(f"❌ خطا در دریافت از API اول: {e}")
            return None

    # ========================================
    # API دوم - داده‌های لحظه‌ای
    # ========================================

    def fetch_from_api2(self) -> Optional[pd.DataFrame]:
        """
        دریافت داده از API دوم
        
        این API شامل:
        - اطلاعات لحظه‌ای قیمت
        - صف‌های خرید و فروش (5 سطح)
        - حجم و ارزش حقیقی/حقوقی
        - اطلاعات معاملات امروز
        - استفاده برای فیلتر 10
        
        Returns:
            DataFrame یا None
        """
        if not self.api2_key:
            logger.warning("⚠️ کلید API دوم تنظیم نشده است")
            return None

        url = f"{self.api2_base_url}/AllSymbols.php?key={self.api2_key}"

        try:
            logger.info("📥 دریافت داده از API دوم (لحظه‌ای - فیلتر 10)...")
            response = requests.get(url, headers=self.headers_api2, timeout=30)

            if response.status_code == 200:
                data = response.json()

                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data)
                    logger.info(f"✅ API دوم: {len(df)} نماد دریافت شد")
                    return df
                else:
                    logger.error("❌ API دوم: داده خالی یا فرمت نامعتبر")
                    return None
            else:
                logger.error(f"❌ API دوم: خطای {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            logger.error("❌ API دوم: خطای Timeout")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("❌ API دوم: خطای اتصال")
            return None
        except Exception as e:
            logger.error(f"❌ خطا در دریافت از API دوم: {e}")
            return None

    def fetch_symbol_details_api2(self, symbol: str) -> Optional[Dict]:
        """
        دریافت جزئیات یک نماد از API دوم
        
        Args:
            symbol: نام نماد
            
        Returns:
            دیکشنری اطلاعات یا None
        """
        if not self.api2_key:
            return None

        url = f"{self.api2_base_url}/SymbolDetails.php?key={self.api2_key}&symbol={symbol}"

        try:
            response = requests.get(url, headers=self.headers_api2, timeout=30)

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"❌ خطا در دریافت {symbol}: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ خطا در دریافت {symbol}: {e}")
            return None

    # ========================================
    # دریافت از هر دو API همزمان
    # ========================================

    def fetch_all_data(self, industry_codes: List[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """
        دریافت داده از هر دو API
        
        Args:
            industry_codes: لیست کدهای صنعت (فقط برای API اول)
            
        Returns:
            Tuple[df_api1, df_api2]
            - df_api1: DataFrame از API اول (صنایع + صندوق‌ها - فیلترهای 1-9)
            - df_api2: DataFrame از API دوم (داده‌های لحظه‌ای - فیلتر 10)
        """
        logger.info("=" * 80)
        logger.info("🚀 شروع دریافت داده از هر دو API")
        logger.info("=" * 80)

        # دریافت از API اول (تاریخی - برای فیلترهای 1 تا 9)
        df_api1 = self.fetch_from_api1(industry_codes)

        # دریافت از API دوم (لحظه‌ای - برای فیلتر 10)
        df_api2 = self.fetch_from_api2()

        # گزارش نهایی
        logger.info("\n" + "=" * 80)
        logger.info("📊 خلاصه دریافت داده:")
        logger.info(f"  • API اول (فیلتر 1-9): {len(df_api1) if df_api1 is not None and not df_api1.empty else 0} رکورد")
        if df_api1 is not None and not df_api1.empty:
            total_stocks = len(df_api1[df_api1["is_fund"] == False])
            total_funds = len(df_api1[df_api1["is_fund"] == True])
            logger.info(f"    - سهام: {total_stocks}")
            logger.info(f"    - صندوق‌ها: {total_funds}")
        logger.info(f"  • API دوم (فیلتر 10): {len(df_api2) if df_api2 is not None and not df_api2.empty else 0} نماد")
        logger.info("=" * 80)

        return df_api1, df_api2

    # ========================================
    # توابع کمکی
    # ========================================

    def validate_api1_data(self, df: pd.DataFrame) -> bool:
        """
        اعتبارسنجی داده‌های API اول
        بررسی اینکه تمام ستون‌های لازم موجود باشند
        
        Args:
            df: DataFrame دریافت شده از API اول
            
        Returns:
            bool: True اگر داده معتبر باشد
        """
        if df is None or df.empty:
            return False

        required_columns = [
            'symbol',
            'last_price',
            'final_price',
            'value_to_avg_monthly_value',
            'sarane_kharid',
            'godrat_kharid',
            'pol_hagigi'
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logger.warning(f"⚠️ ستون‌های گمشده در API اول: {missing_columns}")
            return False

        return True

    def validate_api2_data(self, df: pd.DataFrame) -> bool:
        """
        اعتبارسنجی داده‌های API دوم
        بررسی اینکه تمام ستون‌های لازم موجود باشند
        
        Args:
            df: DataFrame دریافت شده از API دوم
            
        Returns:
            bool: True اگر داده معتبر باشد
        """
        if df is None or df.empty:
            return False

        # چک کردن وجود ستون symbol (یا l18)
        if 'symbol' not in df.columns and 'l18' not in df.columns:
            logger.warning("⚠️ ستون symbol یا l18 در API دوم یافت نشد")
            return False

        return True


# ========================================
# مثال استفاده (برای تست)
# ========================================
if __name__ == "__main__":
    # تنظیم لاگ
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # ایجاد نمونه
    fetcher = UnifiedDataFetcher(
        api1_base_url="API_BASE_URL",  # پیش‌فرض
        api2_key="YOUR_BRSAPI_KEY"  # کلید BrsApi
    )

    # دریافت از هر دو API
    df_api1, df_api2 = fetcher.fetch_all_data()

    # نمایش نمونه API اول
    if df_api1 is not None and not df_api1.empty:
        print("\n📊 نمونه داده API اول:")
        print(df_api1.head(2))
        print(f"\n📈 تعداد ستون‌های API اول: {len(df_api1.columns)}")
        print(f"✅ اعتبارسنجی API اول: {fetcher.validate_api1_data(df_api1)}")
        
        # نمایش توزیع داده‌ها
        print("\n📊 توزیع داده‌ها:")
        print(df_api1.groupby(['is_fund', 'fund_type']).size())
        
        # مثال فیلتر کردن
        print("\n🔍 مثال‌های فیلتر:")
        print(f"  • سهام عادی: {len(df_api1[df_api1['is_fund'] == False])}")
        print(f"  • صندوق‌های اهرمی: {len(df_api1[df_api1['fund_type'] == 'leveraged'])}")
        print(f"  • صندوق‌های بخشی: {len(df_api1[df_api1['fund_type'] == 'sector'])}")

    # نمایش نمونه API دوم
    if df_api2 is not None and not df_api2.empty:
        print("\n📊 نمونه داده API دوم:")
        print(df_api2.head(2))
        print(f"\n📈 تعداد ستون‌های API دوم: {len(df_api2.columns)}")
        print(f"✅ اعتبارسنجی API دوم: {fetcher.validate_api2_data(df_api2)}")
