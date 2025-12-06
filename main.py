"""
Main script برای Bourse Tracker
اجرای فیلترها و ارسال هشدارها به تلگرام
با مدیریت هشدارها از طریق GitHub Gist

فیلترهای 1-9: روی API اول (داده‌های تاریخی)
فیلتر 10: روی API دوم (داده‌های لحظه‌ای) + غنی‌سازی با API اول
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

# ===========================
# تنظیم timezone تهران
# ===========================
TEHRAN_TZ = pytz.timezone("Asia/Tehran")

# ===========================
# تنظیم logging به وقت تهران
# ===========================
def tehran_time(*args):
    return datetime.now(TEHRAN_TZ).timetuple()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bourse_tracker.log", encoding="utf-8"),
    ],
)
logging.Formatter.converter = tehran_time
logger = logging.getLogger(__name__)

# ===========================
# تعداد سهام در هر پیام بر اساس فیلتر
# ===========================
STOCKS_PER_MESSAGE_MAP = {
    'filter_1_strong_buying': 5,
    'filter_2_sarane_cross': 5,
    'filter_3_watchlist': 5,
    'filter_4_ceiling_queue': 4,
    'filter_5_pol_hagigi_ratio': 3,
    'filter_6_tick_time': 5,
    'filter_7_suspicious_volume': 5,
    'filter_8_swing_trade': 5,
    'filter_9_first_hour': 5,
    'filter_10_heavy_buy_queue': 5
}

# ===========================
# توابع کمکی
# ===========================
def is_market_open() -> bool:
    """بررسی اینکه آیا بازار باز است یا نه (به وقت تهران)"""
    now = datetime.now(TEHRAN_TZ)
    logger.info(f"🕐 زمان تهران: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # روز هفته به وقت تهران
    weekday = (now.weekday() + 2) % 7
    if weekday not in WORKING_DAYS:
        logger.info(f"امروز روز کاری نیست (روز هفته: {weekday})")
        return False

    # تاریخ شمسی
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

def chunk_dataframe(df, filter_name):
    """تقسیم DataFrame به چانک‌های کوچکتر بر اساس فیلتر"""
    chunk_size = STOCKS_PER_MESSAGE_MAP.get(filter_name, 5)
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

# ===========================
# ارسال هشدارها - نسخه Parallel
# ===========================
async def send_alerts_for_filters_async(alert: TelegramAlert, alert_manager: GistAlertManager, 
                                        filters_results: dict, api_name: str) -> tuple:
    """
    ارسال هشدارها برای فیلترهای یک API به صورت کاملاً موازی
    
    Args:
        alert: شیء TelegramAlert
        alert_manager: شیء GistAlertManager
        filters_results: دیکشنری نتایج فیلترها
        api_name: نام API (برای لاگ)
    
    Returns:
        tuple: (تعداد ارسال شده, تعداد رد شده)
    """
    sent_count = 0
    skipped_count = 0

    logger.info(f"\n{'='*60}")
    logger.info(f"📤 ارسال هشدارهای {api_name}")
    logger.info(f"{'='*60}")

    # لیست تمام Taskهای ارسال
    all_tasks = []
    all_symbols_to_mark = []

    for filter_name, filtered_df in filters_results.items():
        if filtered_df.empty:
            logger.info(f"فیلتر {filter_name}: نتیجه‌ای یافت نشد")
            continue

        logger.info(f"\n🔍 پردازش فیلتر {filter_name}: {len(filtered_df)} سهم")

        # گروه‌بندی بر اساس فیلتر
        for chunk_idx, chunk_df in enumerate(chunk_dataframe(filtered_df, filter_name), 1):
            symbols_to_send = []
            
            # چک کردن اینکه کدوم سهام قبلاً ارسال نشده
            for idx, row in chunk_df.iterrows():
                symbol = row['symbol']
                if not alert_manager.should_send_alert(symbol, filter_name):
                    logger.info(f"⏭️  {symbol}: قبلاً امروز ارسال شده")
                    skipped_count += 1
                else:
                    symbols_to_send.append(symbol)

            if symbols_to_send:
                # فقط سهام جدید رو ارسال می‌کنیم
                chunk_to_send = chunk_df[chunk_df['symbol'].isin(symbols_to_send)]

                # ایجاد Task برای ارسال (بدون await)
                task = alert.send_filter_alert(chunk_to_send, filter_name)
                all_tasks.append((task, symbols_to_send, filter_name, chunk_idx))
                
                logger.info(f"📋 Task ایجاد شد برای {filter_name} گروه {chunk_idx}: {len(symbols_to_send)} سهم")
            else:
                logger.info(f"⏭️  {filter_name} گروه {chunk_idx}: همه قبلاً ارسال شده‌اند")

    # اجرای همزمان تمام Taskها
    if all_tasks:
        logger.info(f"\n🚀 شروع ارسال موازی {len(all_tasks)} پیام...")
        
        # جمع‌آوری فقط taskها
        tasks_only = [task for task, _, _, _ in all_tasks]
        
        # اجرای همزمان
        results = await asyncio.gather(*tasks_only, return_exceptions=True)
        
        # جمع‌آوری موفقیت‌ها برای mark کردن
        successful_marks = []
        
        # پردازش نتایج
        for idx, (result, (_, symbols, filter_name, chunk_idx)) in enumerate(zip(results, all_tasks)):
            if isinstance(result, Exception):
                logger.error(f"❌ خطا در ارسال {filter_name} گروه {chunk_idx}: {result}")
            elif result:
                # موفق - آماده برای mark
                successful_marks.extend([(s, filter_name) for s in symbols])
                sent_count += len(symbols)
                logger.info(f"✅ {filter_name} گروه {chunk_idx}: {len(symbols)} سهم ارسال شد")
            else:
                logger.error(f"❌ {filter_name} گروه {chunk_idx}: خطا در ارسال")
        
        # Mark کردن تمام موفقیت‌ها یکجا (async)
        if successful_marks:
            logger.info(f"📝 علامت‌گذاری {len(successful_marks)} هشدار در Gist...")
            await alert_manager.mark_multiple_as_sent(successful_marks)
    
    return sent_count, skipped_count

# ===========================
# تابع اصلی
# ===========================
async def main_async():
    logger.info("=" * 80)
    logger.info("🚀 شروع Bourse Tracker")
    logger.info("=" * 80)

    try:
        # اعتبارسنجی تنظیمات
        validate_config()
        logger.info("✅ تنظیمات معتبر است")

        # بررسی وضعیت بازار
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

        # ارسال هشدارها
        logger.info("\n📤 شروع ارسال هشدارها به تلگرام...")
        alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)

        total_sent = 0
        total_skipped = 0

        # ارسال هشدارهای API اول (فیلترهای 1-9)
        if 'api1' in all_results and all_results['api1']:
            sent, skipped = await send_alerts_for_filters_async(
                alert, alert_manager, all_results['api1'], "API اول (فیلترهای 1-9)"
            )
            total_sent += sent
            total_skipped += skipped

        # ارسال هشدارهای API دوم (فیلتر 10)
        if 'api2' in all_results and all_results['api2']:
            sent, skipped = await send_alerts_for_filters_async(
                alert, alert_manager, all_results['api2'], "API دوم (فیلتر 10)"
            )
            total_sent += sent
            total_skipped += skipped

        # گزارش نهایی
        stats = await alert_manager.get_today_stats()
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
    """نقطه ورود اصلی برنامه"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()