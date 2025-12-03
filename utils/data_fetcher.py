"""
ماژول دریافت داده‌های بورس از API
"""
import requests
import pandas as pd
import time
from typing import Optional, List
import logging

from config import API_BASE_URL, INDUSTRY_CODES, INDUSTRY_NAMES, CSV_COLUMNS

# تنظیم لاگ
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BourseDataFetcher:
    """کلاس دریافت داده‌های بورس از API"""
    
    def __init__(self):
        self.base_url = API_BASE_URL
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.industry_codes = INDUSTRY_CODES
        self.industry_names = INDUSTRY_NAMES
        self.columns = CSV_COLUMNS
    
    def fetch_industry_data(self, industry_code: str) -> Optional[pd.DataFrame]:
        """
        دریافت داده یک صنعت خاص
        
        Args:
            industry_code: کد صنعت (مثلاً '43')
            
        Returns:
            DataFrame حاوی داده‌های سهام آن صنعت یا None در صورت خطا
        """
        try:
            # تولید timestamp برای جلوگیری از cache
            timestamp = int(time.time() * 1000)
            url = f"{self.base_url}{industry_code}?_={timestamp}"
            
            logger.info(f"در حال دریافت داده صنعت {industry_code} ({self.industry_names.get(industry_code, 'نامشخص')})")
            
            # ارسال درخواست
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            # تبدیل JSON به DataFrame
            data = response.json()
            
            if not data:
                logger.warning(f"داده‌ای برای صنعت {industry_code} دریافت نشد")
                return None
            
            df = pd.DataFrame(data, columns=self.columns)
            
            # اضافه کردن اطلاعات صنعت
            df['industry_code'] = industry_code
            df['industry_name'] = self.industry_names.get(industry_code, 'نامشخص')
            
            # تبدیل نوع داده‌ها برای عملکرد بهتر
            df = self._convert_dtypes(df)
            
            logger.info(f"✅ {len(df)} سهم از صنعت {self.industry_names.get(industry_code)} دریافت شد")
            
            return df
            
        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout در دریافت داده صنعت {industry_code}")
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ خطا در دریافت داده صنعت {industry_code}: {e}")
            return None
            
        except Exception as e:
            logger.error(f"❌ خطای غیرمنتظره در پردازش داده صنعت {industry_code}: {e}")
            return None
    
    def fetch_all_industries(self, industry_codes: Optional[List[str]] = None, batch_size: int = 5) -> pd.DataFrame:
        """
        دریافت داده همه صنایع به صورت batch
        
        Args:
            industry_codes: لیست کدهای صنعت (اگر None باشد، همه صنایع دریافت می‌شود)
            batch_size: تعداد صنایع در هر batch (پیش‌فرض: 5)
            
        Returns:
            DataFrame حاوی داده‌های تمام صنایع
        """
        if industry_codes is None:
            industry_codes = self.industry_codes
        
        all_data = []
        failed_industries = []
        
        total_industries = len(industry_codes)
        logger.info(f"شروع دریافت داده از {total_industries} صنعت (هر batch: {batch_size} صنعت)...")
        
        # تقسیم به batch
        for batch_num, i in enumerate(range(0, total_industries, batch_size), 1):
            batch = industry_codes[i:i + batch_size]
            
            logger.info(f"📦 Batch {batch_num}/{(total_industries + batch_size - 1) // batch_size}: "
                       f"دریافت {len(batch)} صنعت...")
            
            for code in batch:
                df = self.fetch_industry_data(code)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                else:
                    failed_industries.append(code)
                
                # تاخیر کوتاه برای جلوگیری از فشار به سرور
                time.sleep(0.3)
            
            # تاخیر بین batch‌ها
            if i + batch_size < total_industries:
                logger.info(f"⏸️  توقف 2 ثانیه بین batch‌ها...")
                time.sleep(2)
        
        # گزارش نتیجه
        if failed_industries:
            logger.warning(f"⚠️  صنایع با خطا ({len(failed_industries)}): {', '.join(failed_industries)}")
        
        if not all_data:
            logger.error("❌ هیچ داده‌ای دریافت نشد!")
            return pd.DataFrame()
        
        # ادغام همه دیتافریم‌ها
        final_df = pd.concat(all_data, ignore_index=True)
        
        logger.info(f"✅ جمع {len(final_df)} سهم از {len(all_data)}/{total_industries} صنعت دریافت شد")
        
        return final_df
    
    def get_available_industries(self) -> dict:
        """
        دریافت لیست صنایع موجود
        
        Returns:
            دیکشنری کدها و نام‌های صنایع
        """
        return self.industry_names
    
    def _convert_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        تبدیل نوع داده ستون‌ها برای عملکرد بهتر
        
        Args:
            df: DataFrame ورودی
            
        Returns:
            DataFrame با نوع داده‌های صحیح و واحدهای تبدیل شده
        """
        # ستون‌های عددی (integer)
        int_columns = [
            'id', 'volume', 'value', 'first_price', 'high_price', 'low_price',
            'last_price', 'final_price', 'diff_last_final',
            'buy_order_value', 'sell_order_value', 'diff_buy_sell_order',
            'avg_monthly_value', 'avg_3_month_value', 'marketcap'
        ]
        
        # ستون‌های عددی (float)
        float_columns = [
            'first_price_change_percent', 'high_price_change_percent',
            'low_price_change_percent', 'last_price_change_percent',
            'final_price_change_percent', 'volatility',
            'sarane_kharid', 'sarane_forosh', 'godrat_kharid', 'pol_hagigi',
            'avg_5_day_pol_hagigi', 'avg_20_day_pol_hagigi', 'avg_60_day_pol_hagigi',
            '5_day_pol_hagigi', '20_day_pol_hagigi', '60_day_pol_hagigi',
            '5_day_godrat_kharid', '20_day_godrat_kharid',
            'value_to_avg_monthly_value', 'value_to_avg_3_month_value',
            '5_day_return', '20_day_return', '60_day_return', 'value_to_marketcap'
        ]
        
        # تبدیل به int
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        
        # تبدیل به float
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')
        
        # symbol باید string بمونه
        if 'symbol' in df.columns:
            df['symbol'] = df['symbol'].astype(str)
        
        # ========================================
        # تبدیل واحدها برای محاسبات راحت‌تر
        # ========================================
        
        # ستون‌های مالی بزرگ: تقسیم بر 10 میلیارد (به میلیارد تومان)
        billion_columns = [
            'pol_hagigi', 'value', 'marketcap',
            'buy_order_value', 'sell_order_value', 'diff_buy_sell_order',
            'avg_5_day_pol_hagigi', 'avg_20_day_pol_hagigi', 'avg_60_day_pol_hagigi',
            '5_day_pol_hagigi', '20_day_pol_hagigi', '60_day_pol_hagigi',
            'avg_monthly_value', 'avg_3_month_value'
        ]
        
        for col in billion_columns:
            if col in df.columns:
                df[col] = df[col] / 10_000_000_000  # تبدیل به میلیارد تومان
        
        # سرانه خرید و فروش: تقسیم بر 10 میلیون (به میلیون تومان)
        million_columns = [
            'sarane_kharid', 'sarane_forosh'
        ]
        
        for col in million_columns:
            if col in df.columns:
                df[col] = df[col] / 10_000_000  # تبدیل به میلیون تومان
        
        # ========================================
        # محاسبه ستون‌های جدید
        # ========================================
        
        # نسبت ورود پول حقیقی به میانگین ارزش معاملات ماهانه
        if 'pol_hagigi' in df.columns and 'avg_monthly_value' in df.columns:
            df['pol_hagigi_to_avg_monthly_value'] = df.apply(
                lambda row: row['pol_hagigi'] / row['avg_monthly_value'] 
                if row['avg_monthly_value'] != 0 else 0, 
                axis=1
            )
        
        return df