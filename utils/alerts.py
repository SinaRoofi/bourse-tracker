import asyncio
from telegram import Bot
import pandas as pd
import logging
import jdatetime
import pytz

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام"""

    def __init__(self, channel_name: str = "@tehran_stock_alerts"):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode=parse_mode)
            logger.info("✅ پیام با موفقیت ارسال شد")
            return True
        except Exception as e:
            logger.error(f"❌ خطا در ارسال پیام: {e}")
            return False

    def send_message_sync(self, message: str, parse_mode: str = 'HTML') -> bool:
        return asyncio.run(self.send_message(message, parse_mode))

    def _current_tehran_jdatetime(self):
        """زمان فعلی به وقت تهران، خروجی: (تاریخ شمسی، ساعت)"""
        tehran_tz = pytz.timezone("Asia/Tehran")
        now = jdatetime.datetime.now(tehran_tz)
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")
        return date_str, time_str

    def _format_default_alert(self, df: pd.DataFrame, alert_title: str) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._current_tehran_jdatetime()
        message = f"🔔 <b>{alert_title}</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"
            emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
            message += f"💰 قیمت آخرین: {row.get('last_price', 0)} ({emoji_price}<b>{row.get('last_price_change_percent', 0):+.2f}%</b>)\n"
            if 'value_to_avg_monthly_value' in row:
                message += f"📊 ارزش معاملات / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"
            if 'pol_hagigi_to_avg_monthly_value' in row:
                message += f"💵 پول حقیقی / میانگین ماهانه: {row['pol_hagigi_to_avg_monthly_value']:.2f}\n"
            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"
            if 'value' in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n"
            if 'pol_hagigi' in row:
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            message += "\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_2_sarane_cross(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"🔔 <b>هشدار کراس سرانه خرید</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n\n"
            if "value" in row and "value_to_avg_monthly_value" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n"
                message += f"📊 نسبت به میانگین ماهانه: {row['value_to_avg_monthly_value']:.2f}x\n"
            if "godrat_kharid" in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "pol_hagigi" in row:
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_3_watchlist(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"⚠️ <b>هشدار عبور از آستانه</b>\n\n"
        for _, row in df.iterrows():
            percent = row.get("last_price_change_percent", 0)
            emoji = "🚀" if percent > 5 else "📈" if percent > 3 else "✅"
            message += f"{emoji} <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"
            message += f"📊 درصد تغییر آخرین: <b>{percent:.2f}%</b>\n"
            if "threshold" in row:
                message += f"🔺 عبور: +{percent - row['threshold']:.2f}%\n"
            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']}\n"
            if "final_price" in row:
                message += f"💵 قیمت پایانی: {row['final_price']}\n"
            if "value" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "pol_hagigi" in row:
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_4_ceiling_queue(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"🔥 <b>صف‌های خرید سنگین</b>\n\n"
        for _, row in df.iterrows():
            message += f"🎯 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"
            if "last_price_change_percent" in row:
                message += f"📊 تغییر قیمت: <b>+{row['last_price_change_percent']}</b>\n"
            if "final_price" in row:
                message += f"💵 قیمت پایانی: {row['final_price']}\n"
            if "buy_order_value" in row:
                message += f"🟢 <b>ارزش صف خرید: {row['buy_order_value']:,.0f} میلیارد تومان</b>\n"
            if "sell_order_value" in row:
                sell_val = row["sell_order_value"]
                message += f"🔴 ارزش صف فروش: {sell_val if sell_val !=0 else 'صفر'} میلیارد تومان\n"
            if "value" in row and "volume" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n📦 حجم معاملات: {row['volume']:,.0f}\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "pol_hagigi" in row:
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_5_pol_hagigi_ratio(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"💎 <b>هشدار ورود پول حقیقی قوی</b>\n"
        for _, row in df.iterrows():
            ratio = row.get("pol_hagigi_to_avg_monthly_value", 0)
            emoji = "🔥" if ratio > 2 else "⭐" if ratio > 1 else "✅"
            message += f"{emoji} <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"
            message += f"📊 نسبت پول حقیقی به ارزش معاملات: {ratio:.2f}\n"
            emoji_pol = "🟢" if row.get("pol_hagigi", 0) > 0 else "🔴"
            message += f"{emoji_pol} ورود پول حقیقی: {row.get('pol_hagigi', 0):,.0f} میلیارد تومان\n"
            if "value" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n"
            if "godrat_kharid" in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "last_price_change_percent" in row:
                emoji_price = "🟢" if row["last_price_change_percent"] > 0 else "🔴"
                message += f"{emoji_price} تغییر قیمت: {row['last_price_change_percent']:+.2f}%\n"
            if "last_price" in row:
                message += f"💵 آخرین قیمت: {row['last_price']}\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_6_tick_time(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"⏰ <b>تیک و ساعت</b>\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if "industry_name" in row:
                message += f" - {row['industry_name']}\n"
            else:
                message += "\n"
            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']} "
            if "last_price_change_percent" in row:
                message += f"(<b>{row['last_price_change_percent']:+.2f}%</b>)\n"
            if "tick_diff" in row:
                message += f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
                if "final_price_change_percent" in row:
                    message += f"   (آخرین: {row.get('last_price_change_percent',0):.2f}% | پایانی: {row['final_price_change_percent']:.2f}%)\n"
            if "value" in row and "value_to_avg_monthly_value" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n📊 نسبت به میانگین ماهانه: {row['value_to_avg_monthly_value']:.2f}x\n"
            if "pol_hagigi" in row:
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "godrat_kharid" in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_7_suspicious_volume(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "حجم مشکوک")

    def format_filter_8_swing_trade(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "نوسان‌گیری")

    def format_filter_9_first_hour(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "۱ ساعت اول")

    def format_filter_10_heavy_buy_queue(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"💰 <b>صف خرید با اردر سنگین</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"
            if "buy_queue_value" in row:
                message += f"🟢 <b>صف خرید: {row['buy_queue_value']:,.0f} میلیارد تومان</b>\n"
            if "last_price" in row:
                message += f"💰 قیمت آخرین: {row['last_price']}\n"
            if "last_price_change_percent" in row:
                emoji = "🟢" if row["last_price_change_percent"] > 0 else "🔴"
                message += f"{emoji} تغییر: {row['last_price_change_percent']:+.2f}%\n"
            if "value" in row:
                message += f"💵 ارزش معاملات: {row['value']:,.0f} میلیارد تومان\n"
            if "sarane_kharid" in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:,.0f} میلیون تومان\n"
            if "pol_hagigi" in row:
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {row['pol_hagigi']:,.0f} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
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
        return asyncio.run(self.send_filter_alert(df, filter_name))