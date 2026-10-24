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
    fit_history_size,
    has_day,
    build_today_values,
    add_day,
)
from config import (
    ERROR_CHAT_ID,
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


async def _notify(telegram_alert: TelegramAlert, text: str) -> None:
    """پیام وضعیت به کانال خطا/وضعیت (ERROR_CHAT_ID). شکست ارسال هیچ‌وقت چیزی رو خراب نمی‌کنه."""
    if not ERROR_CHAT_ID:
        logger.warning("⚠️ ERROR_CHAT_ID تنظیم نشده — پیام وضعیت سرانه فقط در لاگ ثبت شد")
        return
    try:
        await telegram_alert.send_message(text, chat_id=ERROR_CHAT_ID)
    except Exception as e:
        logger.error(f"❌ ارسال پیام وضعیت سرانه به کانال شکست خورد: {e}")


async def save_sarane_history_step(
    alert_manager: GistAlertManager, telegram_alert: TelegramAlert
) -> None:
    """
    ذخیره‌ی سرانه‌ی پایانیِ امروز برای فیلتر ۱۹ - یک‌بار در روز، بعد از بسته‌شدن بازار.
    چون اکشن فقط یک‌بار (آخرین‌بار ~۱۲:۳۱) اجرا می‌شه، فرصت دوم وجود نداره؛ پس دریافت داده و
    ذخیره تا SAVE_ATTEMPTS بار تلاش می‌کنن. هر نتیجه‌ی مهم هم لاگ می‌شه هم تو کانال میاد.
    مستقل از قفل خلاصه‌ی روزانه‌ست و هیچ‌وقت خلاصه‌ی بازار رو خراب نمی‌کنه.
    """
    cfg = HEAVY_SARANE_CONFIG
    SAVE_ATTEMPTS = 3
    RETRY_DELAY_SECONDS = 20
    header = "💾 <b>تاریخچه‌ی سرانه خرید</b>"
    try:
        if not cfg.get("enabled"):
            logger.info("⏭️ فیلتر ۱۹ غیرفعاله - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            return
        if not alert_manager.queue_gist_enabled:
            logger.warning("⚠️ QUEUE_GIST_ID/QUEUE_GIST_TOKEN تنظیم نشده - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            await _notify(telegram_alert, f"{header}\n⚠️ QUEUE_GIST_ID/QUEUE_GIST_TOKEN تنظیم نشده؛ ذخیره انجام نشد.")
            return
        if not API_BASE_URL:
            logger.warning("⚠️ API_BASE_URL تنظیم نشده - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            await _notify(telegram_alert, f"{header}\n⚠️ API_BASE_URL تنظیم نشده؛ ذخیره انجام نشد.")
            return

        now_str = datetime.now(TEHRAN_TZ).strftime("%H:%M")
        not_after = cfg.get("save_not_after", "12:45")
        if now_str > not_after:
            logger.warning(f"⏭️ ساعت {now_str} بعد از {not_after} است - ذخیره‌ی تاریخچه‌ی سرانه رد شد")
            await _notify(
                telegram_alert,
                f"{header}\n⚠️ اجرا دیر شد (ساعت {now_str}، حداکثر {not_after}). سرانه‌ی امروز ذخیره نشد.",
            )
            return

        keep_days = cfg.get("history_days", 20)
        today = alert_manager.today_jalali

        raw = await alert_manager.load_sarane_history()
        if raw is None:
            logger.error("❌ تاریخچه‌ی سرانه خونده نشد - ذخیره‌ی امروز رد شد")
            await _notify(telegram_alert, f"{header}\n❌ خوندن تاریخچه از Gist شکست خورد؛ سرانه‌ی {today} ذخیره نشد.")
            return
        history = normalize_history(raw, keep_days)
        if has_day(history, today):
            logger.info(f"⏭️ سرانه‌ی امروز ({today}) قبلاً ذخیره شده")
            return
        logger.info(f"📂 تاریخچه‌ی فعلی: {len(history.get('days', {}))} روز - شروع دریافت سرانه‌ی پایانی {today}")

        # تعداد نمادهای روز قبل = معیار «کامل بودن» دریافت امروز. اگه دریافت ناقص بود
        # (صنعتی شکست خورده)، دوباره تلاش می‌شه و بهترین نتیجه نگه داشته می‌شه.
        # ذخیره‌ی نتیجه همچنان انجام می‌شه (فقط داده‌ی بهتر و هشدار تو کانال).
        prev_days = sorted(history.get("days", {}))
        last_day_data = history["days"][prev_days[-1]] if prev_days else {}
        prev_count = len(last_day_data) if isinstance(last_day_data, dict) else 0
        min_symbols = cfg.get("min_symbols_to_save", 100)
        expected = max(min_symbols, int(prev_count * cfg.get("min_symbols_ratio_vs_prev", 0.9)))

        values: dict = {}
        for attempt in range(1, SAVE_ATTEMPTS + 1):
            fetcher = UnifiedDataFetcher(api1_base_url=API_BASE_URL)
            try:
                df_raw = fetcher.fetch_all_data()
            finally:
                fetcher.close()
            if df_raw is None or df_raw.empty:
                logger.error(f"❌ دریافت داده برای سرانه شکست خورد (تلاش {attempt}/{SAVE_ATTEMPTS})")
            else:
                df = BourseDataProcessor().process_all_data(df_raw)
                # build_today_values اول نمادهای تکراری (سهم/صندوق هم‌نام) رو دیداپ می‌کنه؛
                # پس شمارش min_symbols_to_save روی نمادهای یکتاست.
                attempt_values = build_today_values(df)
                logger.info(
                    f"📊 نماد معتبر (بعد از دیداپ): {len(attempt_values)} "
                    f"(مورد انتظار: {expected}) (تلاش {attempt}/{SAVE_ATTEMPTS})"
                )
                if len(attempt_values) > len(values):
                    values = attempt_values
                if len(values) >= expected:
                    break
                logger.warning(
                    f"⚠️ دریافت ناقص‌ به‌نظر میاد: {len(values)} نماد از حداقل {expected} "
                    f"(تلاش {attempt}/{SAVE_ATTEMPTS})"
                )
            if attempt < SAVE_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY_SECONDS)

        if len(values) < min_symbols:
            logger.error(f"❌ ذخیره‌ی سرانه رد شد: {len(values)} نماد معتبر < {min_symbols}")
            await _notify(
                telegram_alert,
                f"{header}\n❌ داده‌ی کافی نیست ({len(values)} نماد یکتا از حداقل {min_symbols})؛ "
                f"سرانه‌ی {today} ذخیره نشد تا تاریخچه خراب نشه.",
            )
            return

        new_history = add_day(history, today, values, keep_days)
        # حجم فایل زیر سقف بمونه (کمتر از ۱MB گیست) تا خوندن همیشه با یک درخواست انجام بشه
        new_history, dropped_days, size_bytes = fit_history_size(
            new_history,
            int(cfg.get("max_file_kb", 800)) * 1024,
            cfg.get("min_history_days", 5),
        )
        if dropped_days:
            logger.warning(f"⚠️ حجم تاریخچه از سقف گذشت؛ {dropped_days} روز قدیمی حذف شد (حجم: {size_bytes // 1024}KB)")
            await _notify(
                telegram_alert,
                f"{header}\n⚠️ حجم تاریخچه از سقف {cfg.get('max_file_kb', 800)}KB گذشت؛ "
                f"{dropped_days} روز قدیمی حذف شد.",
            )
        saved = False
        for attempt in range(1, SAVE_ATTEMPTS + 1):
            if await alert_manager.save_sarane_history(new_history):
                saved = True
                break
            logger.warning(f"⚠️ ذخیره‌ی تاریخچه‌ی سرانه شکست خورد (تلاش {attempt}/{SAVE_ATTEMPTS})")
            if attempt < SAVE_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY_SECONDS)

        if saved:
            days_count = len(new_history.get("days", {}))
            with_diff = sum(1 for v in values.values() if v[1] is not None)
            min_days = cfg.get("min_history_days", 10)
            logger.info(f"💾 سرانه‌ی پایانی {today} ذخیره شد ({len(values)} نماد، {days_count} روز در تاریخچه)")
            ready = (
                "✅ فیلتر ۱۹ از فردا فعاله."
                if days_count >= min_days
                else f"⏳ فیلتر ۱۹ وقتی فعال می‌شه که حداقل {min_days} روز تو تاریخچه باشه "
                     f"(الان {days_count} روز)."
            )
            incomplete = (
                f"\n⚠️ دریافت احتمالاً ناقص بود: {len(values)} نماد (روز قبل: {prev_count})"
                if len(values) < expected
                else ""
            )
            await _notify(
                telegram_alert,
                f"{header}\n✅ سرانه‌ی پایانی {today} ذخیره شد{incomplete}\n"
                f"• نماد: {len(values)} (با اختلاف سرانه: {with_diff})\n"
                f"• روز در تاریخچه: {days_count}/{keep_days} (حجم: {size_bytes // 1024}KB)\n{ready}",
            )
        else:
            logger.error("❌ ذخیره‌ی تاریخچه‌ی سرانه بعد از همه‌ی تلاش‌ها شکست خورد")
            await _notify(
                telegram_alert,
                f"{header}\n❌ ذخیره‌ی سرانه‌ی {today} بعد از {SAVE_ATTEMPTS} تلاش شکست خورد (خطای Gist). "
                "اکشن دیگه تا فردا اجرا نمی‌شه؛ دستی بررسی کن.",
            )
    except Exception as e:
        logger.error(f"❌ خطا در ذخیره‌ی تاریخچه‌ی سرانه (خلاصه‌ی بازار ادامه پیدا می‌کنه): {e}", exc_info=True)
        await _notify(telegram_alert, f"{header}\n❌ خطای غیرمنتظره: <code>{e}</code>")


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
            await save_sarane_history_step(alert_manager, telegram_alert)

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
