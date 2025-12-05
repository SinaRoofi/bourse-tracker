"""
Main script برای Bourse Tracker
اجرای فیلترها و ارسال هشدارها به تلگرام
با مدیریت هشدارها از طریق GitHub Gist

فیلترهای 1-9: روی API اول (داده‌های تاریخی)
فیلتر 10: روی API دوم (داده‌های لحظه‌ای)
"""

import sys
import logging
from datetime import datetime
import jdatetime
import pytz
import os
import asyncio

from config import (
    MARKET_START_TIME,
    MARKET_END_TIME,
    WORKING_DAYS,
    API_BASE_URL,
    BRSAPI_KEY,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    GIST_ID,
    GIST_TOKEN,
    validate_config,
)
from utils.holidays import is_holiday, is_working_day
from utils.data_fetcher import UnifiedDataFetcher
from utils.data_processor import BourseDataProcessor
from utils.alerts import TelegramAlert
from utils.gist_alert_manager import GistAlertManager

# تنظیم timezone تهران
TEHRAN_TZ = pytz.timezone("Asia/Tehran")

# تنظیمات لاگ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bourse_tracker.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# تنظیمات گروه‌بندی پیام‌ها
STOCKS_PER_MESSAGE = 10  # تعداد سهم در هر پیام

# ========================================
# توابع کمکی
# ========================================

def is_market_open() -> bool:
    """بررسی اینکه آیا بازار باز است یا نه"""
    utc_now = datetime.now(pytz.UTC)
    now = utc_now.astimezone(TEHRAN_TZ)

    logger.info(f"🕐 زمان UTC: {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"🕐 زمان تهران: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    weekday = (now.weekday() + 2) % 7
    if weekday not in WORKING_DAYS:
        logger.info(f"امروز روز کاری نیست (روز هفته: {weekday})")
        return False

    jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
    today_str = jnow.strftime("%Y-%m-%d")
    if is_holiday(today_str):
        logger.info(f"امروز تعطیل رسمی است: {today_str}")
        return False

    current_time = now.strftime("%H:%M")
    if not (MARKET_START_TIME <= current_time <= MARKET_END_TIME):
        logger.info(f"خارج از ساعات کاری بازار (ساعت تهران: {current_time})")
        return False

    logger.info(f"✅ بازار باز است - {today_str} {current_time}")
    return True

def chunk_dataframe(df, chunk_size):
    """تقسیم DataFrame به چانک‌های کوچکتر"""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

async def send_alerts_for_filters_async(alert: TelegramAlert, alert_manager: GistAlertManager, 
                                         filters_results: dict, api_name: str) -> tuple:
    """
    ارسال هشدارها برای فیلترهای یک API (نسخه async با بهبود ذخیره‌سازی)
    
    Args:
        alert: شیء TelegramAlert
        alert_manager: شیء GistAlertManager
        filters_results: نتایج فیلترها
        api_name: نام API (برای لاگ)
        
    Returns:
        tuple: (تعداد ارسال شده، تعداد رد شده)
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

        # گروه‌بندی سهام - 5 سهم در هر پیام
        for chunk_idx, chunk_df in enumerate(chunk_dataframe(filtered_df, STOCKS_PER_MESSAGE), 1):
            # بررسی spam برای همه سهام در chunk
            symbols_to_send = []
            for idx, row in chunk_df.iterrows():
                symbol = row['symbol']
                if not alert_manager.should_send_alert(symbol, filter_name):
                    logger.info(f"⏭️  {symbol}: قبلاً امروز ارسال شده")
                    skipped_count += 1
                else:
                    symbols_to_send.append(symbol)

            # اگر سهمی برای ارسال باشد
            if symbols_to_send:
                # فیلتر کردن فقط سهم‌هایی که باید ارسال بشن
                chunk_to_send = chunk_df[chunk_df['symbol'].isin(symbols_to_send)]

                # ارسال یک پیام برای گروه
                success = await alert.send_filter_alert(chunk_to_send, filter_name)

                if success:
                    # ✅ بهبود: ذخیره گروهی به جای تک‌تک
                    alerts_to_save = [(symbol, filter_name) for symbol in symbols_to_send]
                    save_success = alert_manager.mark_multiple_as_sent(alerts_to_save)
                    
                    if save_success:
                        sent_count += len(symbols_to_send)
                        logger.info(f"✅ گروه {chunk_idx} از {filter_name}: {len(symbols_to_send)} سهم ارسال و ذخیره شد")
                    else:
                        logger.warning(f"⚠️ گروه {chunk_idx}: ارسال موفق اما ذخیره ناموفق")
                        sent_count += len(symbols_to_send)
                else:
                    logger.error(f"❌ گروه {chunk_idx} از {filter_name}: خطا در ارسال")
            else:
                logger.info(f"⏭️  گروه {chunk_idx}: همه قبلاً ارسال شده‌اند")

    return sent_count, skipped_count

# ========================================
# تابع اصلی
# ========================================

async def main_async():
    """تابع اصلی async"""
    logger.info("=" * 80)
    logger.info("🚀 شروع Bourse Tracker")
    logger.info("=" * 80)

    try:
        # اعتبارسنجی تنظیمات
        validate_config()
        logger.info("✅ تنظیمات معتبر است")

        # بررسی بازار
        if not is_market_open():
            logger.info("⏸️  بازار بسته است. خروج از برنامه.")
            return

        # دریافت داده از APIها
        logger.info("\n📥 شروع دریافت داده از APIها...")
        fetcher = UnifiedDataFetcher(api1_base_url=API_BASE_URL, api2_key=BRSAPI_KEY)
        df_api1_raw, df_api2_raw = fetcher.fetch_all_data()

        # پردازش داده‌ها
        logger.info("\n🔄 شروع پردازش داده‌ها...")
        processor = BourseDataProcessor()
        df_api1, df_api2 = processor.process_all_data(df_api1_raw, df_api2_raw)

        # اعمال فیلترها
        logger.info("\n🔍 اعمال فیلترها...")
        all_results = processor.apply_all_filters(df_api1, df_api2)

        # آماده‌سازی ارسال هشدارها
        logger.info("\n📤 شروع ارسال هشدارها به تلگرام...")
        alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)

        total_sent = 0
        total_skipped = 0

        # ارسال هشدارهای API اول (فیلترهای 1-9)
        if 'api1' in all_results and all_results['api1']:
            sent, skipped = await send_alerts_for_filters_async(
                alert, 
                alert_manager, 
                all_results['api1'], 
                "API اول (فیلترهای 1-9)"
            )
            total_sent += sent
            total_skipped += skipped

        # ارسال هشدارهای API دوم (فیلتر 10)
        if 'api2' in all_results and all_results['api2']:
            sent, skipped = await send_alerts_for_filters_async(
                alert, 
                alert_manager, 
                all_results['api2'], 
                "API دوم (فیلتر 10)"
            )
            total_sent += sent
            total_skipped += skipped

        # گزارش نهایی
        stats = alert_manager.get_today_stats()

        logger.info("\n" + "=" * 80)
        logger.info("📊 گزارش نهایی:")
        logger.info(f"  • تاریخ: {stats['date']}")
        logger.info(f"  • هشدارهای ارسال شده (این اجرا): {total_sent}")
        logger.info(f"  • هشدارهای رد شده (اسپم): {total_skipped}")
        logger.info(f"  • مجموع هشدارهای امروز: {stats['total_alerts']}")
        logger.info(f"  • آمار بر اساس نوع هشدار:")
        for alert_type, count in stats['alerts_by_type'].items():
            logger.info(f"    - {alert_type}: {count}")
        logger.info(f"  • Gist: {alert_manager.get_gist_url()}")
        logger.info("=" * 80)
        logger.info("✅ اجرا با موفقیت به پایان رسید")

    except KeyboardInterrupt:
        logger.info("\n⚠️  اجرا توسط کاربر متوقف شد")
        sys.exit(0)

    except Exception as e:
        logger.error(f"\n❌ خطای غیرمنتظره: {e}", exc_info=True)
        sys.exit(1)

def main():
    """wrapper برای اجرای async"""
    asyncio.run(main_async())

# ========================================
# نقطه ورود
# ========================================

if __name__ == "__main__":
    main()