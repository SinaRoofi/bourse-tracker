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
from config import GIST_TOKEN, GIST_ID

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
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)
        summary_generator = DailySummaryGenerator(alert_manager, telegram_alert)

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
