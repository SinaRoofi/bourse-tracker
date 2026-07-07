import sys
import logging
from datetime import datetime
import jdatetime
import pytz
import asyncio

from config import (
    MARKET_START_TIME,
    MARKET_END_TIME,
    API_BASE_URL,
    BRSAPI_KEY,
    ERROR_CHAT_ID,
    GIST_ID,
    GIST_TOKEN,
    validate_config,
)
from utils.holidays import is_trading_day
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
    "filter_1_strong_buying": 5,
    "filter_2_sarane_cross": 5,
    "filter_3_watchlist": 5,
    "filter_4_range_mosbat": 5,
    "filter_5_pol_hagigi_ratio": 5,
    "filter_6_tick_time": 5,
    "filter_7_suspicious_volume": 5,
    "filter_8_swing_trade": 5,
    "filter_9_first_hour": 5,
    "filter_10_heavy_buy_queue": 5,
    "filter_11_hoghooghi_haghighi_strong_buy": 5,
}

# ===========================
# نگاشت فیلتر به ستون کلیدی برای Daily Summary
# فقط فیلترهایی که در summary نمایش داده می‌شن
# ===========================
FILTER_VALUE_COLUMN = {
    "filter_1_strong_buying":                  "godrat_kharid",
    "filter_2_sarane_cross":                   "sarane_kharid",
    "filter_5_pol_hagigi_ratio":               "pol_hagigi_to_avg_monthly_value",
    "filter_7_suspicious_volume":              "value_to_avg_monthly_value",
    "filter_10_heavy_buy_queue":               "buy_queue_value",
    "filter_11_hoghooghi_haghighi_strong_buy": "sarane_kharid",
}


# ===========================
# توابع کمکی
# ===========================
def is_market_open() -> bool:
    """بررسی اینکه آیا بازار باز است یا نه (به وقت تهران)"""
    now = datetime.now(TEHRAN_TZ)
    logger.info(f"🕐 زمان تهران: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    if not is_trading_day(now):
        logger.info("امروز روز معاملاتی نیست (آخر هفته یا تعطیل رسمی)")
        return False

    current_time = now.strftime("%H:%M")
    if not (MARKET_START_TIME <= current_time <= MARKET_END_TIME):
        logger.info(f"خارج از ساعات کاری بازار (ساعت تهران: {current_time})")
        return False

    jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
    logger.info(f"✅ بازار باز است - {jnow.strftime('%Y-%m-%d')} {current_time}")
    return True


def chunk_dataframe(df, filter_name):
    """تقسیم DataFrame به چانک‌های کوچکتر بر اساس فیلتر"""
    chunk_size = STOCKS_PER_MESSAGE_MAP.get(filter_name, 5)
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]


# ===========================
# ارسال هشدارها - نسخه Parallel
# ===========================
async def send_alerts_for_filters_async(
    alert: TelegramAlert,
    alert_manager: GistAlertManager,
    filters_results: dict,
    api_name: str,
) -> tuple:
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

    all_tasks = []

    for filter_name, filtered_df in filters_results.items():
        if filtered_df.empty:
            logger.info(f"فیلتر {filter_name}: نتیجه‌ای یافت نشد")
            continue

        logger.info(f"\n🔍 پردازش فیلتر {filter_name}: {len(filtered_df)} سهم")

        value_col = FILTER_VALUE_COLUMN.get(filter_name)

        for chunk_idx, chunk_df in enumerate(
            chunk_dataframe(filtered_df, filter_name), 1
        ):
            symbols_to_send = []

            for idx, row in chunk_df.iterrows():
                symbol = row["symbol"]
                if not await alert_manager.should_send_alert(symbol, filter_name):
                    logger.info(f"⏭️  {symbol}: قبلاً امروز ارسال شده")
                    skipped_count += 1
                else:
                    symbols_to_send.append(symbol)

            if symbols_to_send:
                chunk_to_send = chunk_df[chunk_df["symbol"].isin(symbols_to_send)]

                task = alert.send_filter_alert(chunk_to_send, filter_name)
                all_tasks.append((task, symbols_to_send, filter_name, chunk_idx, chunk_to_send, value_col))

                logger.info(
                    f"📋 Task ایجاد شد برای {filter_name} گروه {chunk_idx}: {len(symbols_to_send)} سهم"
                )
            else:
                logger.info(f"⏭️  {filter_name} گروه {chunk_idx}: همه قبلاً ارسال شده‌اند")

    if all_tasks:
        logger.info(f"\n🚀 شروع ارسال موازی {len(all_tasks)} پیام...")

        tasks_only = [task for task, _, _, _, _, _ in all_tasks]
        results = await asyncio.gather(*tasks_only, return_exceptions=True)

        successful_marks = []

        for result, (_, symbols, filter_name, chunk_idx, chunk_to_send, value_col) in zip(results, all_tasks):
            if isinstance(result, Exception):
                logger.error(
                    f"❌ خطا در ارسال {filter_name} گروه {chunk_idx}: {result}"
                )
            elif result:
                # استخراج value برای هر نماد از chunk_to_send
                for s in symbols:
                    val = None
                    if value_col and value_col in chunk_to_send.columns:
                        row = chunk_to_send[chunk_to_send["symbol"] == s]
                        if not row.empty:
                            try:
                                val = float(row.iloc[0][value_col])
                            except (ValueError, TypeError):
                                val = None
                    successful_marks.append((s, filter_name, val))

                sent_count += len(symbols)
                logger.info(
                    f"✅ {filter_name} گروه {chunk_idx}: {len(symbols)} سهم ارسال شد"
                )
            else:
                logger.error(f"❌ {filter_name} گروه {chunk_idx}: خطا در ارسال")

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
        validate_config()
        logger.info("✅ تنظیمات معتبر است")

        if not is_market_open():
            logger.info("⏸️  بازار بسته است. خروج از برنامه.")
            return

        logger.info("\n📥 شروع دریافت داده از APIها...")
        fetcher = UnifiedDataFetcher(api1_base_url=API_BASE_URL, api2_key=BRSAPI_KEY)
        df_api1_raw, df_api2_raw = fetcher.fetch_all_data()

        logger.info("\n🔄 شروع پردازش داده‌ها...")
        processor = BourseDataProcessor()
        df_api1, df_api2 = processor.process_all_data(df_api1_raw, df_api2_raw)

        logger.info("\n🔍 اعمال فیلترها...")
        all_results = processor.apply_all_filters(df_api1, df_api2)

        logger.info("\n📤 شروع ارسال هشدارها به تلگرام...")
        alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)

        # هشدار فوری اگه یکی از فیلترها امروز خطا داده باشه (کانال جدا، نه کانال اصلی)
        if processor.failed_filters:
            failed_list = "، ".join(processor.failed_filters)
            logger.error(f"⚠️ فیلترهای خطادار این اجرا: {failed_list}")
            if ERROR_CHAT_ID:
                await alert.send_message(
                    f"⚠️ <b>خطا در اجرای فیلتر</b>\n\n"
                    f"فیلترهای زیر امروز اجرا نشدن (احتمالاً به‌خاطر تغییر schema API):\n"
                    f"<code>{failed_list}</code>\n\n"
                    f"لاگ کامل رو تو GitHub Actions چک کن.",
                    chat_id=ERROR_CHAT_ID,
                )
            else:
                logger.warning(
                    "⚠️ ERROR_CHAT_ID تنظیم نشده — هشدار خطای فیلتر فقط در لاگ ثبت شد"
                )

        total_sent = 0
        total_skipped = 0

        if "api1" in all_results and all_results["api1"]:
            sent, skipped = await send_alerts_for_filters_async(
                alert, alert_manager, all_results["api1"], "API اول (فیلترهای 1-9)"
            )
            total_sent += sent
            total_skipped += skipped

        if "api2" in all_results and all_results["api2"]:
            sent, skipped = await send_alerts_for_filters_async(
                alert, alert_manager, all_results["api2"], "API دوم (فیلتر 10)"
            )
            total_sent += sent
            total_skipped += skipped

        stats = await alert_manager.get_today_stats()
        logger.info("\n" + "=" * 80)
        logger.info("📊 گزارش نهایی:")
        logger.info(f"  • تاریخ: {stats['date']}")
        logger.info(f"  • هشدارهای ارسال شده (این اجرا): {total_sent}")
        logger.info(f"  • هشدارهای رد شده (اسپم): {total_skipped}")
        logger.info(f"  • مجموع هشدارهای امروز: {stats['total_alerts']}")
        logger.info("  • آمار بر اساس نوع هشدار:")
        for alert_type, count in stats["alerts_by_type"].items():
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
