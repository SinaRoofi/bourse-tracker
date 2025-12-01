# data_fetcher.py

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
    
    def fetch_all_industries(self, industry_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        دریافت داده همه صنایع
        
        Args:
            industry_codes: لیست کدهای صنعت (اگر None باشد، همه صنایع دریافت می‌شود)
            
        Returns:
            DataFrame حاوی داده‌های تمام صنایع
        """
        if industry_codes is None:
            industry_codes = self.industry_codes
        
        all_data = []
        failed_industries = []
        
        logger.info(f"شروع دریافت داده از {len(industry_codes)} صنعت...")
        
        for code in industry_codes:
            df = self.fetch_industry_data(code)
            
            if df is not None and not df.empty:
                all_data.append(df)
            else:
                failed_industries.append(code)
            
            # تاخیر کوتاه برای جلوگیری از فشار به سرور
            time.sleep(0.3)
        
        # گزارش نتیجه
        if failed_industries:
            logger.warning(f"⚠️  صنایع با خطا: {', '.join(failed_industries)}")
        
        if not all_data:
            logger.error("❌ هیچ داده‌ای دریافت نشد!")
            return pd.DataFrame()
        
        # ادغام همه دیتافریم‌ها
        final_df = pd.concat(all_data, ignore_index=True)
        
        logger.info(f"✅ جمع {len(final_df)} سهم از {len(all_data)} صنعت دریافت شد")
        
        return final_df
    
    def get_available_industries(self) -> dict:
        """
        دریافت لیست صنایع موجود
        
        Returns:
            دیکشنری کدها و نام‌های صنایع
        """
        return self.industry_names