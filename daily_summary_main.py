"""
Entry point برای Daily Summary Reporter
فقط یک‌بار در روز و فقط بعد از ساعت 12:30 تهران اجرا می‌شود
"""

import asyncio
from datetime import datetime
import pytz
import sys
import logging
import jdatetime

from utils.daily_summary_generator import DailySummaryGenerator
from utils.alerts import TelegramAlert
from utils.gist_alert_manager import GistAlertManager
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


def should_send_summary_by_time() -> bool:
    """فقط بعد از 12:30 تهران"""
    now = datetime.now(TEHRAN_TZ)
    if now.hour < 12:
        return False
    if now.hour == 12 and now.minute < 30:
        return False
    return True


async def main_async():
    logger.info("=" * 80)
    logger.info("📊 Daily Summary Reporter")
    logger.info("=" * 80)

    try:
        # 1) چک زمان
        now = datetime.now(TEHRAN_TZ)
        current_time = now.strftime("%H:%M")

        if not should_send_summary_by_time():
            logger.info(f"⏭️ هنوز زود است. ساعت فعلی: {current_time}")
            return

        logger.info(f"✅ ساعت {current_time} - عبور از شرط زمانی")

        # 2) بررسی تنظیمات
        if not all([GIST_TOKEN, GIST_ID]):
            logger.error("❌ GIST_TOKEN و GIST_ID باید تنظیم شوند")
            sys.exit(1)

        # 3) init manager
        telegram_alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)
        summary_generator = DailySummaryGenerator(alert_manager, telegram_alert)

        # 4) چک ارسال‌شدن قبلی (قفل روزانه)
        today_jalali = jdatetime.date.today().strftime("%Y-%m-%d")

        if await alert_manager.is_today_summary_sent():
            logger.info("⏭️ خلاصه امروز قبلاً ارسال شده — خروج")
            return

        logger.info("🚀 شروع تولید و ارسال خلاصه روزانه")

        # 5) تولید و ارسال
        success = await summary_generator.generate_and_send(
            min_count=3,
            top_n=None
        )

        # 6) ثبت قفل روزانه
        if success:
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
