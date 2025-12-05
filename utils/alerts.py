"""
ماژول ارسال هشدارها به تلگرام
سازگار با GitHub Actions و محیط‌های بدون loop از پیش‌راه‌اندازی شده
"""
import asyncio
from telegram import Bot
import pandas as pd
from typing import List, Tuple
import jdatetime
import logging

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام (سازگار با GitHub Actions)"""

    def __init__(self, channel_name: str = "📊 گزارش بورس"):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """ارسال پیام async"""
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode=parse_mode)
            logger.info("✅ پیام با موفقیت ارسال شد")
            return True
        except Exception as e:
            logger.error(f"❌ خطا در ارسال پیام: {e}")
            return False

    def _format_number(self, num: float) -> str:
        """فرمت‌بندی اعداد"""
        if pd.isna(num):
            return "0"
        if abs(num) >= 1000:
            return f"{num:.1f} هزار میلیارد"
        elif abs(num) >= 1:
            return f"{num:.2f} میلیارد"
        elif abs(num) >= 0.001:
            return f"{num*1000:.1f} میلیون"
        else:
            return f"{num:.3f}"

    def _get_datetime_strings(self) -> Tuple[str, str]:
        now = jdatetime.datetime.now()
        return now.strftime('%Y/%m/%d'), now.strftime('%H:%M')

    # ========================================
    # توابع فرمت پیام برای هر فیلتر
    # ========================================
    def _format_default_alert(self, df: pd.DataFrame, alert_title: str) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"🔔 <b>{alert_title}</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n\n"
            emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
            message += f"💰 قیمت آخرین: {row.get('last_price', 0):,} ریال ({emoji_price}<b>{row.get('last_price_change_percent', 0):+.2f}%</b>)\n"
            if 'value_to_avg_monthly_value' in row:
                message += f"📊 ارزش معاملات / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"
            if 'pol_hagigi_to_avg_monthly_value' in row:
                message += f"💵 پول حقیقی / میانگین ماهانه: {row['pol_hagigi_to_avg_monthly_value']:.2f}\n"
            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_2_sarane_cross(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 2: کراس سرانه خرید"""
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"🔔 <b>هشدار کراس سرانه خرید</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if 'industry_name' in row:
                message += f" - {row['industry_name']}\n\n"
            else:
                message += "\n\n"
            if 'value' in row and 'value_to_avg_monthly_value' in row:
                message += f"💰 ارزش معاملات: {self._format_number(row['value'])} تومان\n"
                message += f"   📊 نسبت به میانگین ماهانه: {row['value_to_avg_monthly_value']:.2f}x\n\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n\n"
            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {self._format_number(row['sarane_kharid'])} تومان\n\n"
            if 'pol_hagigi' in row:
                emoji = "✅" if row['pol_hagigi'] > 0 else "⚠️"
                message += f"{emoji} ورود پول حقیقی: {self._format_number(row['pol_hagigi'])}\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    def format_filter_3_watchlist(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"⚠️ <b>هشدار عبور از آستانه</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            pct = row.get('last_price_change_percent', 0)
            emoji = "🚀" if pct > 5 else "📈" if pct > 3 else "✅"
            message += f"{emoji} <b>{row['symbol']}</b>"
            if 'industry_name' in row:
                message += f" - {row['industry_name']}\n\n"
            else:
                message += "\n\n"
            message += f"📊 درصد تغییر آخرین: <b>{pct:.2f}%</b>\n"
            if 'threshold' in row:
                message += f"🎯 آستانه تعریف شده: {row['threshold']:.2f}%\n"
                message += f"🔺 عبور: +{pct - row['threshold']:.2f}%\n\n"
            if 'last_price' in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال\n"
            if 'final_price' in row:
                message += f"💵 قیمت پایانی: {row['final_price']:,} ریال\n\n"
            if 'volume' in row and 'value' in row:
                message += f"📦 حجم: {self._format_number(row['volume'])}\n"
                message += f"💰 ارزش: {self._format_number(row['value'])} تومان\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    def format_filter_4_ceiling_queue(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"🔥 <b>صف‌های خرید سنگین در سقف قیمت</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            message += f"🎯 <b>{row['symbol']}</b>"
            if 'industry_name' in row:
                message += f" - {row['industry_name']}\n\n"
            else:
                message += "\n\n"
            if 'final_price_change_percent' in row:
                message += f"📊 تغییر قیمت: <b>+{row['final_price_change_percent']:.2f}%</b> (سقف مثبت)\n"
            if 'final_price' in row:
                message += f"💰 قیمت پایانی: {row['final_price']:,} ریال\n\n"
            if 'buy_order_value' in row:
                message += f"🟢 <b>ارزش صف خرید: {self._format_number(row['buy_order_value'])} تومان</b>\n"
            if 'sell_order_value' in row:
                val = row['sell_order_value']
                message += f"🔴 ارزش صف فروش: <b>{0 if val == 0 else self._format_number(val)}</b>{' ✅' if val==0 else ''}\n\n"
            if 'value' in row and 'volume' in row:
                message += f"💵 ارزش معاملات: {self._format_number(row['value'])} تومان\n"
                message += f"📦 حجم معاملات: {self._format_number(row['volume'])}\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    def format_filter_5_pol_hagigi_ratio(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"💎 <b>هشدار ورود پول حقیقی قوی</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            ratio = row.get('pol_hagigi_to_avg_monthly_value', 0)
            emoji = "🔥" if ratio > 2 else "⭐" if ratio > 1 else "✅"
            message += f"{emoji} <b>{row['symbol']}</b>"
            if 'industry_name' in row:
                message += f" - {row['industry_name']}\n\n"
            else:
                message += "\n\n"
            if 'pol_hagigi_to_avg_monthly_value' in row:
                message += f"📊 <b>نسبت پول حقیقی: {ratio:.2f}x</b>\n   (پول حقیقی / میانگین ماهانه)\n\n"
            if 'pol_hagigi' in row:
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {self._format_number(row['pol_hagigi'])} تومان\n\n"
            if 'value' in row and 'avg_monthly_value' in row:
                message += f"💰 ارزش معاملات امروز: {self._format_number(row['value'])} تومان\n"
                message += f"📈 میانگین ماهانه: {self._format_number(row['avg_monthly_value'])} تومان\n\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n\n"
            if 'final_price_change_percent' in row:
                emoji_price = "🟢" if row['final_price_change_percent'] > 0 else "🔴"
                message += f"{emoji_price} تغییر قیمت: {row['final_price_change_percent']:+.2f}%\n"
            if 'final_price' in row:
                message += f"💵 قیمت پایانی: {row['final_price']:,} ریال\n"
            if 'volume' in row:
                message += f"📦 حجم: {row['volume']:,} سهم\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    def format_filter_6_tick_time(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"⏰ <b>تیک و ساعت</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            if 'industry_name' in row:
                message += f" - {row['industry_name']}\n\n"
            else:
                message += "\n\n"
            if 'last_price' in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال "
            if 'last_price_change_percent' in row:
                message += f"(<b>{row['last_price_change_percent']:+.2f}%</b>)\n\n"
            if 'tick_diff' in row:
                message += f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
                if 'final_price_change_percent' in row:
                    message += f"   (آخرین: {row.get('last_price_change_percent',0):.2f}% | پایانی: {row['final_price_change_percent']:.2f}%)\n\n"
            if 'value' in row and 'value_to_avg_monthly_value' in row:
                message += f"💵 ارزش معاملات: {self._format_number(row['value'])} تومان\n"
                message += f"📊 نسبت به میانگین ماهانه: {row['value_to_avg_monthly_value']:.2f}x\n\n"
            if 'pol_hagigi' in row:
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {self._format_number(row['pol_hagigi'])} تومان\n\n"
            if 'sarane_kharid' in row:
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n"
            if 'godrat_kharid' in row:
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    # فیلترهای ساده که از default استفاده می‌کنند
    def format_filter_7_suspicious_volume(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "حجم مشکوک")

    def format_filter_8_swing_trade(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "نوسان‌گیری")

    def format_filter_9_first_hour(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "۱ ساعت اول")

    def format_filter_10_heavy_buy_queue(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._get_datetime_strings()
        message = f"💰 <b>صف خرید میلیاردی</b>\n📅 {date_str} | 🕐 {time_str}\n━━━━━━━━━━━━━━━━━━━━━\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n\n"
            if 'buy_order_value' in row:
                message += f"🟢 <b>صف خرید: {self._format_number(row['buy_order_value'])} تومان</b>\n\n"
            if 'last_price' in row:
                message += f"💰 قیمت آخرین: {row['last_price']:,} ریال\n"
            if 'last_price_change_percent' in row:
                emoji = "🟢" if row['last_price_change_percent'] > 0 else "🔴"
                message += f"{emoji} تغییر: {row['last_price_change_percent']:+.2f}%\n\n"
            if 'value' in row:
                message += f"💵 ارزش معاملات: {self._format_number(row['value'])} تومان\n"
            if 'volume' in row:
                message += f"📦 حجم: {self._format_number(row['volume'])}\n\n"
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        message += f"📢 {self.channel_name}"
        return message

    # ========================================
    # ارسال هشدارها
    # ========================================
    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
        """ارسال یک هشدار async"""
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

    async def send_multiple_alerts(self, alerts: List[Tuple[pd.DataFrame, str]]) -> None:
        """ارسال همزمان