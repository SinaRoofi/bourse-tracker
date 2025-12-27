"""
Entry point برای Daily Summary Reporter
فقط بعد از ساعت 12:30 تهران اجرا می‌شه
"""

import asyncio
from datetime import datetime
import pytz
import sys
import logging

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


def should_send_summary() -> bool:
    """
    بررسی اینکه آیا زمان ارسال خلاصه است یا نه
    فقط بعد از 12:30 تهران
    """
    now = datetime.now(TEHRAN_TZ)
    current_hour = now.hour
    current_minute = now.minute

    # فقط بعد از 12:30
    if current_hour < 12:
        return False
    if current_hour == 12 and current_minute < 30:
        return False

    return True


async def main_async():
    logger.info("=" * 80)
    logger.info("📊 Daily Summary Reporter")
    logger.info("=" * 80)

    try:
        # بررسی زمان
        now = datetime.now(TEHRAN_TZ)
        current_time = now.strftime("%H:%M")

        if not should_send_summary():
            logger.info(f"⏭️  هنوز زود است. ساعت فعلی: {current_time}")
            logger.info("💡 خلاصه روزانه فقط بعد از 12:30 ارسال می‌شود")
            return

        logger.info(f"✅ ساعت {current_time} - شروع تولید خلاصه روزانه")

        # بررسی تنظیمات ضروری برای daily summary
        if not all([GIST_TOKEN, GIST_ID]):
            logger.error("❌ GIST_TOKEN و GIST_ID باید تنظیم شوند")
            sys.exit(1)
        
        logger.info("✅ تنظیمات معتبر است")

        # ایجاد شیء‌ها
        telegram_alert = TelegramAlert()
        alert_manager = GistAlertManager(GIST_TOKEN, GIST_ID)
        summary_generator = DailySummaryGenerator(alert_manager, telegram_alert)

        # تولید و ارسال گزارش
        success = await summary_generator.generate_and_send(
            min_count=2,  # حداقل 2 بار تکرار
            top_n=None    # همه نمادهای پرتکرار (بدون محدودیت)
        )

        if success:
            logger.info("=" * 80)
            logger.info("✅ خلاصه روزانه با موفقیت ارسال شد")
            logger.info("=" * 80)
        else:
            logger.error("=" * 80)
            logger.error("❌ خطا در ارسال خلاصه روزانه")
            logger.error("=" * 80)
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n⚠️ اجرا توسط کاربر متوقف شد")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ خطای غیرمنتظره: {e}", exc_info=True)
        sys.exit(1)


def main():
    """نقطه ورود اصلی برنامه"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()