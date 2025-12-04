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
    MARKET_START_TIME,
    MARKET_END_TIME,
    WORKING_DAYS,
    API_BASE_URL,
    BRSAPI_KEY,
    validate_config,
)
from utils.holidays import is_holiday, is_working_day
from utils.data_fetcher import UnifiedDataFetcher
from utils.data_processor import BourseDataProcessor
from utils.filters import BourseFilters
from utils.alerts import TelegramAlert
from utils.alert_manager import AlertManager

# تنظیم timezone تهران
TEHRAN_TZ = pytz.timezone("Asia/Tehran")

# ========================================
# تنظیمات لاگ
# ========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bourse_tracker.log", encoding="utf-8"),
    ],
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

    # بررسی روز هفته
    weekday = (now.weekday() + 2) % 7

    if weekday not in WORKING_DAYS:
        logger.info(f"امروز روز کاری نیست (روز هفته: {weekday})")
        return False

    # بررسی تعطیلات رسمی
    jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
    today_str = jnow.strftime("%Y-%m-%d")

    if is_holiday(today_str):
        logger.info(f"امروز تعطیل رسمی است: {today_str}")
        return False

    # بررسی ساعت کاری
    current_time = now.strftime("%H:%M")

    if not (MARKET_START_TIME <= current_time <= MARKET_END_TIME):
        logger.info(f"خارج از ساعات کاری بازار (ساعت تهران: {current_time})")
        return False

    logger.info(f"✅ بازار باز است - {today_str} {current_time}")
    return True


def send_alerts_for_api(
    alert: TelegramAlert,
    alert_manager: AlertManager,
    filters_results: dict,
    api_name: str
) -> tuple:
    """
    ارسال هشدارها برای یک API
    
    Args:
        alert: شی TelegramAlert
        alert_manager: مدیر هشدارها
        filters_results: نتایج فیلترها
        api_name: نام API
        
    Returns:
        (sent_count, skipped_count)
    """
    sent_count = 0
    skipped_count = 0
    
    logger.info(f"\n{'='*60}")
    logger.info(f"📤 ارسال هشدارهای {api_name}")
    logger.info(f"{'='*60}")
    
    for filter_name, filtered_df in filters_results.items():
        if filtered_df.empty:
            logger.info(f"فیلتر {filter_name}: نتیجه‌ای یافت نشد")
            continue
        
        logger.info(f"\n🔍 پردازش فیلتر {filter_name}: {len(filtered_df)} سهم")
        
        # ارسال هر نماد به صورت جداگانه
        for idx, row in filtered_df.iterrows():
            symbol = row['symbol']
            
            # بررسی اسپم
            if not alert_manager.should_send_alert(symbol, filter_name):
                logger.info(f"⏭️  {symbol}: قبلاً امروز ارسال شده")
                skipped_count += 1
                continue
            
            # تبدیل سطر به DataFrame تک‌سطری
            single_row_df = row.to_frame().T
            
            # ارسال هشدار
            success = alert.send_filter_alert_sync(single_row_df, filter_name)
            
            if success:
                # ثبت به عنوان ارسال شده
                alert_manager.mark_as_sent(symbol, filter_name)
                sent_count += 1
                logger.info(f"✅ {symbol} - {filter_name}: ارسال شد")
            else:
                logger.error(f"❌ {symbol} - {filter_name}: خطا در ارسال")
    
    return sent_count, skipped_count


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

        # 3. دریافت داده از هر دو API
        logger.info("\n📥 شروع دریافت داده از APIها...")
        fetcher = UnifiedDataFetcher(
            api1_base_url=API_BASE_URL,
            api2_key=BRSAPI_KEY
        )
        df_api1_raw, df_api2_raw = fetcher.fetch_all_data()

        # 4. پردازش داده‌ها
        logger.info("\n🔄 شروع پردازش داده‌ها...")
        processor = BourseDataProcessor()
        df_api1, df_api2 = processor.process_all_data(df_api1_raw, df_api2_raw)

        # 5. اعمال فیلترها
        logger.info("\n🔍 شروع اعمال فیلترها...")
        filters = BourseFilters()
        all_results = filters.apply_all_filters(df_api1, df_api2)

        # 6. ارسال هشدارها (با مدیریت اسپم)
        logger.info("\n📤 شروع ارسال هشدارها به تلگرام...")
        alert = TelegramAlert()
        alert_manager = AlertManager()

        total_sent = 0
        total_skipped = 0

        # ارسال هشدارهای API اول
        if 'api1' in all_results:
            sent, skipped = send_alerts_for_api(
                alert, alert_manager, all_results['api1'], "API اول"
            )
            total_sent += sent
            total_skipped += skipped

        # ارسال هشدارهای API دوم
        if 'api2' in all_results:
            sent, skipped = send_alerts_for_api(
                alert, alert_manager, all_results['api2'], "API دوم"
            )
            total_sent += sent
            total_skipped += skipped

        # 7. آمار نهایی
        stats = alert_manager.get_today_stats()
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 گزارش نهایی:")
        logger.info(f"  • تاریخ: {stats['date']}")
        logger.info(f"  • هشدارهای ارسال شده (این اجرا): {total_sent}")
        logger.info(f"  • هشدارهای رد شده (اسپم): {total_skipped}")
        logger.info(f"  • مجموع هشدارهای امروز: {stats['total_alerts']}")
        logger.info("\n  • آمار بر اساس فیلتر:")
        for filter_name, count in stats['filters'].items():
            logger.info(f"    - {filter_name}: {count}")
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
