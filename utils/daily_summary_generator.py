"""
ماژول تولید گزارش خلاصه روزانه
تحلیل نمادهای پرتکرار از هشدارهای ثبت شده در Gist
"""

import logging
from datetime import datetime
import jdatetime
import pytz
import pandas as pd
from typing import Dict, List

logger = logging.getLogger(__name__)

# تنظیم timezone تهران
TEHRAN_TZ = pytz.timezone("Asia/Tehran")


class DailySummaryGenerator:
    """کلاس تولید و ارسال گزارش خلاصه روزانه"""

    def __init__(self, alert_manager, telegram_alert):
        """
        Args:
            alert_manager: شیء GistAlertManager
            telegram_alert: شیء TelegramAlert
        """
        self.alert_manager = alert_manager
        self.telegram = telegram_alert
        self.today_jalali = jdatetime.date.today().strftime("%Y-%m-%d")

    async def get_frequent_symbols(self, min_count: int = 3, top_n: int = None) -> Dict[str, int]:
        """
        دریافت نمادهای پرتکرار از Gist

        Args:
            min_count: حداقل تعداد تکرار (پیش‌فرض: 3)
            top_n: تعداد نمادهای برتر (None = همه نمادها)

        Returns:
            dict: {symbol: count} مرتب شده براساس تعداد
        """
        logger.info(f"📊 شروع تحلیل هشدارهای امروز ({self.today_jalali})...")

        # خواندن داده از Gist
        data = await self.alert_manager._load_gist_content()
        today_alerts = data.get(self.today_jalali, [])

        if not today_alerts:
            logger.warning("⚠️ هیچ هشداری برای امروز یافت نشد")
            return {}

        logger.info(f"✅ {len(today_alerts)} هشدار یافت شد")

        # شمارش تکرار هر نماد
        symbol_count = {}
        for alert in today_alerts:
            symbol = alert.get("symbol")
            if symbol:
                symbol_count[symbol] = symbol_count.get(symbol, 0) + 1

        # فیلتر نمادهایی که حداقل min_count بار تکرار شدن
        frequent_symbols = {
            symbol: count
            for symbol, count in symbol_count.items()
            if count >= min_count
        }

        if not frequent_symbols:
            logger.info(f"ℹ️ هیچ نمادی بیش از {min_count} بار تکرار نشده")
            return {}

        # مرتب‌سازی براساس تعداد (نزولی)
        sorted_symbols = sorted(frequent_symbols.items(), key=lambda x: x[1], reverse=True)

        # اگه top_n تعیین شده، محدود کن
        if top_n is not None:
            sorted_symbols = sorted_symbols[:top_n]

        sorted_symbols = dict(sorted_symbols)

        logger.info(f"🎯 {len(sorted_symbols)} نماد پرتکرار یافت شد")
        return sorted_symbols



    def format_summary_message(
        self,
        frequent_symbols: Dict[str, int],
        total_unique_symbols: int
    ) -> str:
        """
        فرمت پیام خلاصه

        Args:
            frequent_symbols: دیکشنری {symbol: count}
            total_unique_symbols: تعداد کل نمادهای منحصربفرد

        Returns:
            str: پیام فرمت شده
        """
        date_str, time_str = self._get_tehran_datetime()

        # شروع پیام
        message = f"📊 <b>خلاصه هشدارها</b>\n\n"

        # بخش نمادها
        if frequent_symbols:
            # گروه‌بندی نمادها براساس تعداد تکرار
            count_groups = {}
            for symbol, count in frequent_symbols.items():
                count_groups.setdefault(count, []).append(symbol)

            # نمایش نمادها گروه به گروه (از بیشترین به کمترین)
            for count in sorted(count_groups.keys(), reverse=True):
                symbols_list = sorted(count_groups[count])  # مرتب‌سازی الفبایی
                hashtags = " ".join([f"#{self._format_symbol_hashtag(s)}" for s in symbols_list])
                message += f"<b>({count}×)</b> {hashtags}\n"
        else:
            message += f"هیچ نماد پرتکراری نبود\n"

        # آمار کلی
        message += f"\n🎯 {len(frequent_symbols)} نماد پرتکرار از {total_unique_symbols} نماد هشداردهنده\n\n"

        # تاریخ و ساعت
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.telegram.channel_name}"

        return message

    @staticmethod
    def _format_symbol_hashtag(symbol: str) -> str:
        """فرمت نماد برای هشتگ (حذف فاصله‌ها)"""
        if not symbol:
            return ""
        return str(symbol).replace(' ', '_').replace('\u200c', '_').strip()

    @staticmethod
    def _get_tehran_datetime() -> tuple:
        """دریافت تاریخ و ساعت تهران"""
        now = datetime.now(TEHRAN_TZ)
        jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        date_str = jnow.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")
        return date_str, time_str

    async def generate_and_send(self, min_count: int = 3, top_n: int = None) -> bool:
        """
        تولید و ارسال گزارش خلاصه

        Args:
            min_count: حداقل تعداد تکرار
            top_n: تعداد نمادهای برتر (None = همه)

        Returns:
            bool: موفقیت ارسال
        """
        try:
            # محاسبه تعداد کل نمادهای منحصربفرد
            data = await self.alert_manager._load_gist_content()
            today_alerts = data.get(self.today_jalali, [])
            total_unique_symbols = len(set(alert["symbol"] for alert in today_alerts if alert.get("symbol")))

            # دریافت نمادهای پرتکرار
            frequent_symbols = await self.get_frequent_symbols(min_count, top_n)

            # فرمت پیام
            message = self.format_summary_message(frequent_symbols, total_unique_symbols)

            # ارسال به تلگرام
            logger.info("📤 ارسال پیام خلاصه...")
            success = await self.telegram.send_message(message, parse_mode='HTML')

            if success:
                logger.info("✅ پیام خلاصه با موفقیت ارسال شد")
            else:
                logger.error("❌ خطا در ارسال پیام خلاصه")

            return success

        except Exception as e:
            logger.error(f"❌ خطا در تولید گزارش خلاصه: {e}", exc_info=True)
            return False