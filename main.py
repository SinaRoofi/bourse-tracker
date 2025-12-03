"""
Main script برای Bourse Tracker
اجرای فیلترها و ارسال هشدارها به تلگرام
"""
import sys
import logging
from datetime import datetime
import jdatetime
import pytz

from config import (
    MARKET_START_TIME, MARKET_END_TIME, WORKING_DAYS, HOLIDAYS_1404,
    validate_config
)
from utils.data_fetcher import BourseDataFetcher
from utils.data_processor import BourseDataProcessor
from utils.alerts import TelegramAlert

# تنظیم timezone تهران
TEHRAN_TZ = pytz.timezone('Asia/Tehran')

# ========================================
# تنظیمات لاگ
# ========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bourse_tracker.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


# ========================================
# توابع کمکی
# ========================================

def is_market_open() -> bool:
    """
    بررسی اینکه آیا بازار باز است یا نه
    
    Returns:
        True اگر بازار باز باشد
    """
    # دریافت زمان UTC و تبدیل به تهران
    utc_now = datetime.now(pytz.UTC)
    now = utc_now.astimezone(TEHRAN_TZ)
    
    logger.info(f"🕐 زمان UTC: {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"🕐 زمان تهران: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # بررسی روز هفته (0=شنبه تا 6=جمعه)
    weekday = (now.weekday() + 2) % 7  # تبدیل به تقویم ایرانی
    
    if weekday not in WORKING_DAYS:
        logger.info(f"امروز روز کاری نیست (روز هفته: {weekday})")
        return False
    
    # بررسی تعطیلات رسمی
    jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
    today_str = jnow.strftime('%Y-%m-%d')
    
    if today_str in HOLIDAYS_1404:
        logger.info(f"امروز تعطیل رسمی است: {today_str}")
        return False
    
    # بررسی ساعت کاری
    current_time = now.strftime('%H:%M')
    
    if not (MARKET_START_TIME <= current_time <= MARKET_END_TIME):
        logger.info(f"خارج از ساعات کاری بازار (ساعت تهران: {current_time})")
        return False
    
    logger.info(f"✅ بازار باز است - {today_str} {current_time}")
    return True


def send_alert_safely(alert: TelegramAlert, df, filter_name: str) -> bool:
    """
    ارسال ایمن هشدار با مدیریت خطا
    
    Args:
        alert: شی TelegramAlert
        df: DataFrame فیلتر شده
        filter_name: نام فیلتر
        
    Returns:
        True در صورت موفقیت
    """
    try:
        if df.empty:
            logger.info(f"فیلتر {filter_name}: داده خالی، ارسال نمی‌شود")
            return False
        
        logger.info(f"ارسال هشدار {filter_name} با {len(df)} سهم...")
        success = alert.send_filter_alert_sync(df, filter_name)
        
        if success:
            logger.info(f"✅ هشدار {filter_name} با موفقیت ارسال شد")
        else:
            logger.error(f"❌ خطا در ارسال هشدار {filter_name}")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ خطا در ارسال هشدار {filter_name}: {e}")
        return False


# ========================================
# تابع اصلی
# ========================================

def main():
    """تابع اصلی اجرای برنامه"""
    
    logger.info("=" * 80)
    logger.info("🚀 شروع Bourse Tracker")
    logger.info("=" * 80)
    
    try:
        # 1. اعتبارسنجی تنظیمات
        logger.info("بررسی تنظیمات...")
        validate_config()
        logger.info("✅ تنظیمات معتبر است")
        
        # 2. بررسی وضعیت بازار
        if not is_market_open():
            logger.info("⏸️  بازار بسته است. خروج از برنامه.")
            return
        
        # 3. دریافت داده
        logger.info("\n📥 شروع دریافت داده از API...")
        fetcher = BourseDataFetcher()
        all_stocks = fetcher.fetch_all_industries(batch_size=5)
        
        if all_stocks.empty:
            logger.error("❌ هیچ داده‌ای دریافت نشد!")
            return
        
        logger.info(f"✅ {len(all_stocks)} سهم از {all_stocks['industry_name'].nunique()} صنعت دریافت شد")
        
        # 4. اعمال فیلترها
        logger.info("\n🔍 شروع اعمال فیلترها...")
        processor = BourseDataProcessor()
        filters_results = processor.apply_all_filters(all_stocks)
        
        # 5. ارسال هشدارها
        logger.info("\n📤 شروع ارسال هشدارها به تلگرام...")
        alert = TelegramAlert()
        
        sent_count = 0
        failed_count = 0
        
        for filter_name, filtered_df in filters_results.items():
            if not filtered_df.empty:
                success = send_alert_safely(alert, filtered_df, filter_name)
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            else:
                logger.info(f"فیلتر {filter_name}: نتیجه‌ای یافت نشد")
        
        # 6. گزارش نهایی
        logger.info("\n" + "=" * 80)
        logger.info("📊 گزارش نهایی:")
        logger.info(f"  • تعداد فیلترها: {len(filters_results)}")
        logger.info(f"  • هشدارهای ارسال شده: {sent_count}")
        logger.info(f"  • هشدارهای ناموفق: {failed_count}")
        logger.info("=" * 80)
        logger.info("✅ اجرا با موفقیت به پایان رسید")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  اجرا توسط کاربر متوقف شد")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"\n❌ خطای غیرمنتظره: {e}", exc_info=True)
        sys.exit(1)


# ========================================
# نقطه ورود
# ========================================

if __name__ == "__main__":
    main()