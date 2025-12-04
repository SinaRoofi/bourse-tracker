"""
ماژول دریافت داده از هر دو API
- API اول (قدیمی): داده‌های تاریخی و میانگین‌ها
- API دوم (BrsApi): داده‌های لحظه‌ای و صف خرید/فروش
"""

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
            api1_base_url: آدرس پایه API اول (قدیمی)
            api2_key: کلید API دوم (BrsApi.ir)
        """
        # API اول (قدیمی)
        self.api1_base_url = api1_base_url
        
        # API دوم (BrsApi)
        self.api2_key = api2_key
        self.api2_base_url = "https://BrsApi.ir/Api/Tsetmc"
        
        # هدرها برای API دوم
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 OPR/106.0.0.0",
            "Accept": "application/json, text/plain, */*"
        }

    # ========================================
    # API اول (قدیمی) - داده‌های تاریخی
    # ========================================
    
    def fetch_from_api1(self, industry_codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        دریافت داده از API اول (قدیمی)
        
        این API شامل:
        - میانگین‌های تاریخی (5 روز، 20 روز، 60 روز، ماهانه، 3 ماهه)
        - بازده‌های تاریخی
        - اطلاعات تکمیلی
        
        Args:
            industry_codes: لیست کدهای صنعت (اگر None باشد همه صنایع)
            
        Returns:
            DataFrame یا None
        """
        if not self.api1_base_url:
            logger.warning("آدرس API اول تنظیم نشده است")
            return None
        
        try:
            logger.info("📥 دریافت داده از API اول (تاریخی)...")
            
            # اینجا کد دریافت از API قدیمی شما قرار می‌گیره
            # مثال:
            # response = requests.get(f"{self.api1_base_url}/endpoint", timeout=30)
            # data = response.json()
            # df = pd.DataFrame(data)
            
            # فعلاً برای نمونه:
            df = pd.DataFrame()  # جایگزین با کد واقعی
            
            logger.info(f"✅ API اول: {len(df)} سهم دریافت شد")
            return df
            
        except Exception as e:
            logger.error(f"❌ خطا در دریافت از API اول: {e}")
            return None

    # ========================================
    # API دوم (BrsApi) - داده‌های لحظه‌ای
    # ========================================
    
    def fetch_from_api2(self) -> Optional[pd.DataFrame]:
        """
        دریافت داده از API دوم (BrsApi)
        
        این API شامل:
        - اطلاعات لحظه‌ای قیمت
        - صف‌های خرید و فروش (5 سطح)
        - حجم و ارزش حقیقی/حقوقی
        - اطلاعات معاملات امروز
        
        Returns:
            DataFrame یا None
        """
        if not self.api2_key:
            logger.warning("کلید API دوم تنظیم نشده است")
            return None
        
        url = f"{self.api2_base_url}/AllSymbols.php?key={self.api2_key}"
        
        try:
            logger.info("📥 دریافت داده از API دوم (BrsApi - لحظه‌ای)...")
            response = requests.get(url, headers=self.headers, timeout=30)
            
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
            response = requests.get(url, headers=self.headers, timeout=30)
            
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
            - df_api1: DataFrame از API اول (داده‌های تاریخی)
            - df_api2: DataFrame از API دوم (داده‌های لحظه‌ای)
        """
        logger.info("=" * 80)
        logger.info("🚀 شروع دریافت داده از هر دو API")
        logger.info("=" * 80)
        
        # دریافت از API اول (تاریخی)
        df_api1 = self.fetch_from_api1(industry_codes)
        
        # دریافت از API دوم (لحظه‌ای)
        df_api2 = self.fetch_from_api2()
        
        # گزارش نهایی
        logger.info("\n" + "=" * 80)
        logger.info("📊 خلاصه دریافت داده:")
        logger.info(f"  • API اول (تاریخی): {len(df_api1) if df_api1 is not None else 0} سهم")
        logger.info(f"  • API دوم (لحظه‌ای): {len(df_api2) if df_api2 is not None else 0} نماد")
        logger.info("=" * 80)
        
        return df_api1, df_api2

    def merge_data(self, df_api1: pd.DataFrame, df_api2: pd.DataFrame, on: str = 'symbol') -> pd.DataFrame:
        """
        ترکیب داده‌های دو API بر اساس نماد
        
        Args:
            df_api1: DataFrame از API اول
            df_api2: DataFrame از API دوم
            on: ستون کلید برای join (معمولاً 'symbol')
            
        Returns:
            DataFrame ترکیب شده
        """
        if df_api1 is None or df_api1.empty:
            logger.warning("API اول خالی است، فقط داده API دوم برگردانده می‌شود")
            return df_api2 if df_api2 is not None else pd.DataFrame()
        
        if df_api2 is None or df_api2.empty:
            logger.warning("API دوم خالی است، فقط داده API اول برگردانده می‌شود")
            return df_api1
        
        try:
            # اگه API دوم ستون l18 داره، اونو به symbol تبدیل کن
            if 'l18' in df_api2.columns and 'symbol' not in df_api2.columns:
                df_api2 = df_api2.rename(columns={'l18': 'symbol'})
            
            # Merge با outer join تا همه داده‌ها حفظ بشن
            merged = pd.merge(
                df_api1, 
                df_api2, 
                on=on, 
                how='outer',
                suffixes=('_api1', '_api2')
            )
            
            logger.info(f"✅ {len(merged)} سهم از ترکیب دو API")
            return merged
            
        except Exception as e:
            logger.error(f"❌ خطا در ترکیب داده‌ها: {e}")
            return pd.DataFrame()


# ========================================
# مثال استفاده
# ========================================
if __name__ == "__main__":
    # تنظیم لاگ
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # ایجاد نمونه
    fetcher = UnifiedDataFetcher(
        api1_base_url="http://your-api1-url.com",  # آدرس API قدیمی
        api2_key="YourApiKey"  # کلید BrsApi
    )
    
    # دریافت از هر دو API
    df_api1, df_api2 = fetcher.fetch_all_data()
    
    # نمایش نمونه
    if df_api2 is not None and not df_api2.empty:
        print("\n📊 نمونه داده API دوم:")
        print(df_api2.head(2))
        print(f"\n📈 ستون‌های API دوم: {df_api2.columns.tolist()}")
