"""
ماژول ارسال هشدارها به تلگرام
"""
import asyncio
from telegram import Bot
import pandas as pd
from typing import Optional
from datetime import datetime
import jdatetime
import logging

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام"""

    def __init__(self, channel_name: str = "@tehran_stock_alerts"):
        """
        Args:
            channel_name: نام کانال برای نمایش در پیام‌ها
        """
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """
        ارسال پیام به کانال
        
        Args:
            message: متن پیام
            parse_mode: نوع فرمت (HTML یا Markdown)
            
        Returns:
            True در صورت موفقیت
        """
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode
            )
            logger.info("✅ پیام با موفقیت ارسال شد")
            return True

        except Exception as e:
            logger.error(f"❌ خطا در ارسال پیام: {e}")
            return False

    def send_message_sync(self, message: str, parse_mode: str = 'HTML') -> bool:
        """نسخه همگام send_message"""
        return asyncio.run(self.send_message(message, parse_mode))

    def _format_number(self, num: float) -> str:
        """فرمت‌بندی اعداد"""
        if pd.isna(num):
            return "0"

        if abs(num) >= 1000:
            return f"{num:.0f} هزار میلیارد"
        elif abs(num) >= 1:
            return f"{num:.2f} میلیارد"
        elif abs(num) >= 0.001:
            return f"{num*1000:.1f} میلیون"
        else:
            return f"{num:.3f}"

    # ========================================
    # فرمت پیش‌فرض برای همه فیلترها
    # ========================================
    def _format_default_alert(self, df: pd.DataFrame, alert_title: str) -> str:
        """فرمت پیش‌فرض برای هشدارها"""
        if df.empty:
            return ""

        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')

        message = f"🔔 <b>{alert_title}</b>\n\n"

        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n\n"

            emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
            message += f"💰 قیمت آخرین: {row.get('last_price', 0):,} ریال "
            message += f"({emoji_price}<b>{row.get('last_price_change_percent', 0):+.2f}%</b>)\n"

            if 'value_to_avg_monthly_value' in row:
                message += f"📊 ارزش معاملات / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"

            if 'pol_hagigi_to_avg_monthly_value' in row:
                pol_ratio = row['pol_hagigi_to_avg_monthly_value']
                message += f"💵 پول حقیقی / میانگین ماهانه: {pol_ratio:.2f}\n"

            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"

            if 'godrat_kharid' in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n\n"

            message += "\n"

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"

        return message

    def format_filter_2_sarane_cross(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 2: کراس سرانه خرید"""
        if df.empty:
            return ""

        message = f"🔔 <b>هشدار کراس سرانه خرید</b>\n\n"

        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n\n"

            if "value" in row and "value_to_avg_monthly_value" in row:
                value_formatted = self._format_number(row["value"])
                value_ratio = row["value_to_avg_monthly_value"]
                message += f"💰 ارزش معاملات: {value_formatted} تومان\n"
                message += f"   📊 نسبت به میانگین ماهانه: {value_ratio:.2f}x\n"

            if "godrat_kharid" in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"

            if "sarane_kharid" in row:
                sarane_kharid_formatted = self._format_number(row["sarane_kharid"])
                message += f"📈 سرانه خرید: {sarane_kharid_formatted} تومان\n"

            if "pol_hagigi" in row:
                pol_hagigi_formatted = self._format_number(row["pol_hagigi"])
                emoji = "✅" if row["pol_hagigi"] > 0 else "⚠️"
                message += f"{emoji} ورود پول حقیقی: {pol_hagigi_formatted}\n"

            message += "\n"

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"

        return message

    def format_filter_3_watchlist(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 3: هشدار درصد تغییر نمادهای خاص"""
        if df.empty:
            return ""

        message = f"⚠️ <b>هشدار عبور از آستانه</b>\n\n"

        for idx, row in df.iterrows():
            if row.get("last_price_change_percent", 0) > 5:
                emoji = "🚀"
            elif row.get("last_price_change_percent", 0) > 3:
                emoji = "📈"
            else:
                emoji = "✅"

            message += f"{emoji} <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"

            message += f"📊 درصد تغییر آخرین: <b>{row.get('last_price_change_percent', 0):.2f}%</b>\n"
            if "threshold" in row:
                message += f"🎯 آستانه تعریف شده: {row['threshold']:.2f}%\n"
                message += f"🔺 عبور: +{row.get('last_price_change_percent', 0) - row['threshold']:.2f}%\n"

            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال\n"
            if "final_price" in row:
                message += f"💵 قیمت پایانی: {row['final_price']:,} ریال\n"

            if "volume" in row and "value" in row:
                volume_formatted = self._format_number(row["volume"])
                value_formatted = self._format_number(row["value"])
                message += f"📦 حجم: {volume_formatted}\n"
                message += f"💰 ارزش: {value_formatted} تومان\n"

            message += "\n"

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"

        return message

    def format_filter_4_ceiling_queue(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 4: صف خرید سنگین در سقف قیمت"""
        if df.empty:
            return ""

        message = f"🔥 <b>صف‌های خرید سنگین</b>\n\n"

        for idx, row in df.iterrows():
            message += f"🎯 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"

            if "final_price_change_percent" in row:
                message += f"📊 تغییر قیمت: <b>+{row['last_price_change_percent']:.2f}%</b>\n"
            if "final_price" in row:
                message += f"💰 قیمت پایانی: {row['final_price']:,} ریال\n\n"

            if "buy_order_value" in row:
                buy_queue_formatted = self._format_number(row["buy_order_value"])
                message += f"🟢 <b>ارزش صف خرید: {buy_queue_formatted} تومان</b>\n"

            if "sell_order_value" in row:
                sell_queue_formatted = self._format_number(row["sell_order_value"])
                if row["sell_order_value"] == 0:
                    message += f"🔴 ارزش صف فروش: <b>صفر</b> ✅\n\n"
                else:
                    message += f"🔴 ارزش صف فروش: {sell_queue_formatted} تومان\n\n"

            if "value" in row and "volume" in row:
                value_formatted = self._format_number(row["value"])
                volume_formatted = self._format_number(row["volume"])
                message += f"💵 ارزش معاملات: {value_formatted} تومان\n"
                message += f"📦 حجم معاملات: {volume_formatted}\n"

            message += "\n"

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"

        return message

    def format_filter_5_pol_hagigi_ratio(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 5: نسبت پول حقیقی به ارزش معاملات"""
        if df.empty:
            return ""

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message = f"💎 <b>هشدار ورود پول حقیقی قوی</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n\n"

        for idx, row in df.iterrows():
            if row.get("pol_hagigi_to_avg_monthly_value", 0) > 2:
                emoji = "🔥"
            elif row.get("pol_hagigi_to_avg_monthly_value", 0) > 1:
                emoji = "⭐"
            else:
                emoji = "✅"

            message += f"{emoji} <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"

            if "pol_hagigi_to_avg_monthly_value" in row:
                message += f"📊 <b>نسبت پول حقیقی به ارزش معاملات: {row['pol_hagigi_to_avg_monthly_value']:.2f}x</b>\n"

            if "pol_hagigi" in row:
                pol_formatted = self._format_number(row["pol_hagigi"])
                emoji_pol = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {pol_formatted} تومان\n"

            if "value" in row:
                value_formatted = self._format_number(row["value"])
                message += f"💰 ارزش معاملات : {value_formatted} تومان\n"

            if "godrat_kharid" in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"

            if "last_price_change_percent" in row:
                emoji_price = "🟢" if row["last_price_change_percent"] > 0 else "🔴"
                message += f"{emoji_price} تغییر قیمت: {row['last_price_change_percent']:+.2f}%\n"
            if "last_price" in row:
                message += f"💵 آخرین قیمت: {row['last_price']:,} ریال\n"
            if "volume" in row:
                message += f"📦 حجم: {row['volume']:,}\n"

            message += "\n"

        message += f"📢 {self.channel_name}"
        return message

    def format_filter_6_tick_time(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 6: تیک و ساعت"""
        if df.empty:
            return ""

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message = f"⏰ <b>تیک و ساعت</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n\n"

        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"

            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال "
            if "last_price_change_percent" in row:
                message += f"(<b>{row['last_price_change_percent']:+.2f}%</b>)\n\n"

            if "tick_diff" in row:
                message += f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
                if "final_price_change_percent" in row:
                    message += f"   (آخرین: {row.get('last_price_change_percent', 0):.2f}% | "
                    message += f"پایانی: {row['final_price_change_percent']:.2f}%)\n\n"

            if "value" in row and "value_to_avg_monthly_value" in row:
                value_formatted = self._format_number(row['value'])
                value_ratio = row['value_to_avg_monthly_value']
                message += f"💵 ارزش معاملات: {value_formatted} تومان\n"
                message += f"📊 نسبت به میانگین ماهانه: {value_ratio:.2f}x\n"

            if 'pol_hagigi' in row:
                pol_formatted = self._format_number(row['pol_hagigi'])
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {pol_formatted} تومان\n"

            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"

            message += "\n"

        message += f"📢 {self.channel_name}"
        return message

    def format_filter_7_suspicious_volume(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 7 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "حجم مشکوک")

    def format_filter_8_swing_trade(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 8 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "نوسان‌گیری")

    def format_filter_9_first_hour(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 9 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "۱ ساعت اول")

    def format_filter_10_heavy_buy_queue(self, df: pd.DataFrame) -> str:
        """فرمت پیام برای فیلتر 10: صف خرید میلیاردی (API دوم)"""
        if df.empty:
            return ""

        message = f"💰 <b>صف خرید میلیاردی</b>\n\n"

        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"

            if "buy_queue_value" in row:
                buy_queue_formatted = self._format_number(row["buy_queue_value"])
                message += f"🟢 <b>صف خرید: {buy_queue_formatted} تومان</b>\n"

            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال\n"
            if "last_price_change_percent" in row:
                emoji = "🟢" if row["last_price_change_percent"] > 0 else "🔴"
                message += f"{emoji} تغییر: {row['last_price_change_percent']:+.2f}%\n"

            if "value" in row:
                value_formatted = self._format_number(row["value"])
                message += f"💵 ارزش معاملات: {value/10_000_000_000:,0f}میلیارد تومان\n"

            message += "\n"

        now = jdatetime.datetime.now()
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"

        return message

    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
        """ارسال هشدار یک فیلتر"""
        if df.empty:
            logger.info(f"فیلتر {filter_name}: سهمی یافت نشد")
            return False

        format_map = {
            'filter_2_sarane_cross': self.format_filter_2_sarane_cross,
            'filter_3_watchlist': self.format_filter_3_watchlist,
            'filter_4_ceiling_queue': self.format_filter_4_ceiling_queue,
            'filter_5_pol_hagigi_ratio': self.format_filter_5_pol_hagigi_ratio,
            'filter_6_tick_time': self.format_filter_6_tick_time,
            'filter_7_suspicious_volume': self.format_filter_7_suspicious_volume,
            'filter_8_swing_trade': self.format_filter_8_swing_trade,
            'filter_9_first_hour': self.format_filter_9_first_hour,
            'filter_10_heavy_buy_queue': self.format_filter_10_heavy_buy_queue,
        }

        format_func = format_map.get(filter_name, lambda df: self._format_default_alert(df, filter_name))
        message = format_func(df)

        if not message:
            return False

        return await self.send_message(message)

    def send_filter_alert_sync(self, df: pd.DataFrame, filter_name: str) -> bool:
        """نسخه همگام send_filter_alert"""
        return asyncio.run(self.send_filter_alert(df, filter_name))