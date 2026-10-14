"""
Entry point برای Daily Summary Reporter
فقط یک‌بار در روز و فقط بعد از ساعت 12:31 تهران اجرا می‌شود

برای تست: با ست‌کردن متغیر محیطی FORCE_SUMMARY=true می‌تونید چک روز
معاملاتی، چک ساعت، و قفل «قبلاً ارسال شده» رو دور بزنید. توی حالت فورس،
خلاصه ارسال می‌شه ولی قفل روزانه ثبت نمی‌شه (تا اجرای واقعی بعدی خراب نشه).
مثال اجرا: FORCE_SUMMARY=true python daily_summary_main.py
"""

import asyncio
from datetime import datetime
import os
import pytz
import sys
import logging

from utils.daily_summary_generator import DailySummaryGenerator
from utils.alerts import TelegramAlert
from utils.gist_alert_manager import GistAlertManager
from utils.holidays import is_trading_day
from utils.data_fetcher import UnifiedDataFetcher
from utils.data_processor import BourseDataProcessor
from utils.sarane_history import (
    normalize_history,
    has_day,
    build_today_values,
    add_day,
)
from config import (
    GIST_TOKEN,
    GIST_ID,
    QUEUE_GIST_TOKEN,
    QUEUE_GIST_ID,
    API_BASE_URL,
    HEAVY_SARANE_CONFIG,
)

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
        logging.FileHandler("daily_summary.log", encoding="utf-8"),
    ],
)
logging.Formatter.converter = tehran_time
logger = logging.getLogger(__name__)

# ===========================
# حالت فورس (تست)
# ===========================
FORCE_SUMMARY = os.getenv("FORCE_SUMMARY", "false").strip().lower() in ("1", "true", "yes")


def should_send_summary_by_time() -> bool:
    """فقط بعد از 12:31 تهران"""
    now = datetime.now(TEHRAN_TZ)
    if now.hour < 12:
        return False
    if now.hour == 12 and now.minute < 31:
        return False
    return True


def is_trading_day_today() -> bool:
    now = datetime.now(TEHRAN_TZ)
    if not is_trading_day(now):
        logger.info("⏭️ امروز روز معاملاتی بورس نیست (آخر هفته یا تعطیل رسمی)")
        return False
    return True


async def save_sarane_history_step(alert_manager: GistAlertManager) -> None:
    """
    ذخیره‌ی سرانه‌ی پایانیِ امروز برای فیلتر ۱۹ - یک‌بار در روز، بعد از بسته‌شدن بازار.
    مستقل از قفل خلاصه‌ی روزانه است (اگه ذخیره شکست بخوره، اجرای بعدیِ همین روز دوباره
    تلاش می‌کنه) و هیچ‌وقت خلاصه‌ی بازار رو خراب نمی‌کنه.
    """
    cfg = HEAVY_SARANE_CONFIG
    try:
        if not cfg.get("enabled"):
            return
        if not alert_manager.queue_gist_enabled:
            logger.warning("⚠️ QUEUE_GIST_ID/QUEUE_GIST_TOKEN تنظیم نشده - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            return
        if not API_BASE_URL:
            logger.warning("⚠️ API_BASE_URL تنظیم نشده - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            return

        not_after = cfg.get("save_not_after", "12:45")
        if datetime.now(TEHRAN_TZ).strftime("%H:%M") > not_after:
            logger.info(f"⏭️ بعد از {not_after} - ذخیره‌ی تاریخچه‌ی سرانه رد شد (پنجره‌ی ذخیره تموم شده)")
            return

        raw = await alert_manager.load_sarane_history()
        if raw is None:
            logger.warning("⚠️ تاریخچه‌ی سرانه خونده نشد - ذخیره‌ی امروز رد شد")
            return
        keep_days = cfg.get("history_days", 10)
        history = normalize_history(raw, keep_days)
        today = alert_manager.today_jalali
        if has_day(history, today):
            logger.info("⏭️ سرانه‌ی امروز قبلاً ذخیره شده")
            return

        fetcher = UnifiedDataFetcher(api1_base_url=API_BASE_URL)
        try:
            df_raw = fetcher.fetch_all_data()
        finally:
            fetcher.close()
        if df_raw is None or df_raw.empty:
            logger.error("❌ دریافت داده برای ذخیره‌ی سرانه شکست خورد")
            return

        df = BourseDataProcessor().process_all_data(df_raw)
        values = build_today_values(df)
        if len(values) < cfg.get("min_symbols_to_save", 100):
            logger.warning(
                f"⚠️ فقط {len(values)} نماد معتبر - ذخیره‌ی سرانه رد شد (جلوگیری از خراب‌شدن تاریخچه)"
            )
            return

        if await alert_manager.save_sarane_history(add_day(history, today, values, keep_days)):
            logger.info(f"💾 سرانه‌ی پایانی امروز ذخیره شد ({len(values)} نماد)")
        else:
            logger.warning("⚠️ ذخیره‌ی تاریخچه‌ی سرانه شکست خورد")
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره‌ی تاریخچه‌ی سرانه (خلاصه‌ی بازار ادامه پیدا می‌کنه): {e}", exc_info=True)


async def main_async():
    logger.info("=" * 80)
    logger.info("📊 Daily Summary Reporter")
    logger.info("=" * 80)

    try:
        if FORCE_SUMMARY:
            logger.warning("🧪 FORCE_SUMMARY فعاله — چک روز معاملاتی، چک ساعت و قفل روزانه دور زده می‌شن")

        # 1) چک روز معاملاتی (روز کاری + غیرتعطیل)
        if not FORCE_SUMMARY and not is_trading_day_today():
            logger.info("⏭️ امروز روز معاملاتی بورس نیست — خروج بدون ارسال")
            return

        # 2) چک زمان
        now = datetime.now(TEHRAN_TZ)
        current_time = now.strftime("%H:%M")

        if not FORCE_SUMMARY and not should_send_summary_by_time():
            logger.info(f"⏭️ هنوز زود است. ساعت فعلی: {current_time}")
            return

        logger.info(f"✅ ساعت {current_time} - عبور از شرط زمانی")

        # 3) بررسی تنظیمات
        if not all([GIST_TOKEN, GIST_ID]):
            logger.error("❌ GIST_TOKEN و GIST_ID باید تنظیم شوند")
            sys.exit(1)

        # 4) init manager
        telegram_alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID, QUEUE_GIST_TOKEN, QUEUE_GIST_ID)
        summary_generator = DailySummaryGenerator(alert_manager, telegram_alert)

        # ذخیره‌ی سرانه‌ی پایانی برای فیلتر ۱۹ (یک‌بار در روز؛ تو حالت تست فورس انجام نمی‌شه
        # چون داده‌ی وسط روز نباید به‌عنوان سرانه‌ی پایانی ثبت بشه)
        if not FORCE_SUMMARY:
            await save_sarane_history_step(alert_manager)

        # 5) چک ارسال‌شدن قبلی (قفل روزانه)
        if not FORCE_SUMMARY and await alert_manager.is_today_summary_sent():
            logger.info("⏭️ خلاصه امروز قبلاً ارسال شده — خروج")
            return

        logger.info("🚀 شروع تولید و ارسال خلاصه روزانه")

        # 6) تولید و ارسال
        success = await summary_generator.generate_and_send(
            min_count=3,
            top_n=None
        )

        # 7) ثبت قفل روزانه (توی حالت فورس قفل ثبت نمی‌شه)
        if success:
            if FORCE_SUMMARY:
                logger.info("🧪 حالت فورس: قفل روزانه ثبت نشد")
            else:
                await alert_manager.mark_today_summary_sent()
            logger.info("=" * 80)
            logger.info("✅ خلاصه روزانه با موفقیت ارسال و ثبت شد")
            logger.info("=" * 80)
        else:
            logger.error("❌ خطا در ارسال خلاصه روزانه")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("⚠️ اجرا متوقف شد")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ خطای غیرمنتظره: {e}", exc_info=True)
        sys.exit(1)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
