import asyncio
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
import pandas as pd
import logging
import jdatetime
import pytz

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام - نسخه Async & Parallel"""

    def __init__(self, channel_name: str = "@tehran_stock_alerts"):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)
        # Semaphore برای محدود کردن تعداد درخواست‌های همزمان
        # تلگرام: حدود 20 پیام/دقیقه = 1 پیام هر 3 ثانیه
        # پس Semaphore=3 با delay کوچک بهترین حالته
        self.semaphore = asyncio.Semaphore(3)  # حداکثر 3 پیام همزمان

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """ارسال پیام با مدیریت Semaphore و rate limiting"""
        async with self.semaphore:
            try:
                await self.bot.send_message(
                    chat_id=self.chat_id, 
                    text=message, 
                    parse_mode=parse_mode
                )
                # تأخیر کوچک برای رعایت rate limit تلگرام (20 msg/min)
                await asyncio.sleep(3.5)  # ~17 پیام در دقیقه
                return True
            except RetryAfter as e:
                logger.warning(f"⚠️ Flood control: انتظار {e.retry_after} ثانیه")
                await asyncio.sleep(e.retry_after)
                # تلاش مجدد
                try:
                    await self.bot.send_message(
                        chat_id=self.chat_id, 
                        text=message, 
                        parse_mode=parse_mode
                    )
                    return True
                except Exception as retry_error:
                    logger.error(f"❌ خطا در تلاش مجدد: {retry_error}")
                    return False
            except TimedOut:
                logger.warning("⚠️ Timeout - تلاش مجدد")
                await asyncio.sleep(2)
                try:
                    await self.bot.send_message(
                        chat_id=self.chat_id, 
                        text=message, 
                        parse_mode=parse_mode
                    )
                    return True
                except Exception as retry_error:
                    logger.error(f"❌ خطا در تلاش مجدد: {retry_error}")
                    return False
            except Exception as e:
                logger.error(f"❌ خطا در ارسال پیام: {e}")
                return False

    def _current_tehran_jdatetime(self):
        """زمان فعلی به وقت تهران، خروجی: (تاریخ شمسی، ساعت)"""
        tehran_tz = pytz.timezone("Asia/Tehran")
        now = jdatetime.datetime.now(tehran_tz)
        date_str = now.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")
        return date_str, time_str

    @staticmethod
    def _format_billion(value: float) -> str:
        """فرمت اعداد به میلیارد تومان"""
        if pd.isna(value) or value == 0:
            return "0"
        elif value >= 1:
            return f"{value:.2f}"
        else:
            return f"{value:.3f}"

    def _format_default_alert(self, df: pd.DataFrame, alert_title: str) -> str:
        if df.empty:
            return ""
        date_str, time_str = self._current_tehran_jdatetime()
        message = f"🔔 <b>{alert_title}</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"
            if 'last_price' in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
                change_pct = row.get('last_price_change_percent', 0)
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{change_pct:+.2f}%</b>)\n"
            if 'value' in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if 'value_to_avg_monthly_value' in row and pd.notna(row['value_to_avg_monthly_value']):
                message += f"📊 ارزش / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"
            if 'sarane_kharid' in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if 'godrat_kharid' in row and pd.notna(row['godrat_kharid']):
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"
            if 'pol_hagigi' in row and pd.notna(row['pol_hagigi']):
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
            if 'pol_hagigi_to_avg_monthly_value' in row and pd.notna(row['pol_hagigi_to_avg_monthly_value']):
                message += f"💎 پول حقیقی / میانگین ماهانه: {row['pol_hagigi_to_avg_monthly_value']:.2f}\n"
            message += "\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_2_sarane_cross(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"🔔 <b>هشدار کراس سرانه خرید</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            message += f" - {row['industry_name']}\n" if "industry_name" in row else "\n"
            if 'last_price' in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
                change_pct = row.get('last_price_change_percent', 0)
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{change_pct:+.2f}%</b>)\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "value_to_avg_monthly_value" in row and pd.notna(row['value_to_avg_monthly_value']):
                message += f"📊 نسبت به میانگین ماهانه: {row['value_to_avg_monthly_value']:.2f}x\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "godrat_kharid" in row and pd.notna(row['godrat_kharid']):
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
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
            message += f" - {row['industry_name']}\n" if "industry_name" in row else "\n"
            if "last_price" in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if percent > 0 else "🔴"
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{percent:+.2f}%</b>)\n"
            if "threshold" in row:
                message += f"🔺 عبور از آستانه: +{percent - row['threshold']:.2f}%\n"
            if "final_price" in row and pd.notna(row['final_price']):
                message += f"💵 قیمت پایانی: {row['final_price']}\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
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
            message += f" - {row['industry_name']}\n" if "industry_name" in row else "\n"
            if "last_price_change_percent" in row and pd.notna(row['last_price_change_percent']):
                emoji_price = "🟢" if row['last_price_change_percent'] > 0 else "🔴"
                message += f"💰 تغییر قیمت: ({emoji_price}<b>+{row['last_price_change_percent']:.2f}%</b>)\n"
            if "final_price" in row and pd.notna(row['final_price']):
                message += f"💵 قیمت پایانی: {row['final_price']}\n"
            if "buy_order_value" in row and pd.notna(row['buy_order_value']):
                message += f"🟢 <b>ارزش صف خرید: {self._format_billion(row['buy_order_value'])} میلیارد تومان</b>\n"
            if "sell_order_value" in row and pd.notna(row['sell_order_value']):
                message += f"🔴 ارزش صف فروش: {self._format_billion(row['sell_order_value'])} میلیارد تومان\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "volume" in row and pd.notna(row['volume']):
                message += f"📦 حجم معاملات: {row['volume']:,.0f}\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_5_pol_hagigi_ratio(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"💎 <b>هشدار ورود پول حقیقی قوی</b>\n\n"
        for _, row in df.iterrows():
            ratio = row.get("pol_hagigi_to_avg_monthly_value", 0)
            emoji = "🔥" if ratio > 2 else "⭐" if ratio > 1 else "✅"
            message += f"{emoji} <b>{row['symbol']}</b>"
            message += f" - {row['industry_name']}\n" if "industry_name" in row else "\n"
            if "last_price" in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
                change_pct = row.get('last_price_change_percent', 0)
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{change_pct:+.2f}%</b>)\n"
            if pd.notna(ratio):
                message += f"📊 نسبت پول حقیقی به میانگین ماهانه: <b>{ratio:.2f}</b>\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji_pol} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "godrat_kharid" in row and pd.notna(row['godrat_kharid']):
                message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_6_tick_time(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"⏰ <b>تیک و ساعت</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>"
            message += f" - {row['industry_name']}\n" if "industry_name" in row else "\n"
            if "last_price" in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
                change_pct = row.get('last_price_change_percent', 0)
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{change_pct:+.2f}%</b>)\n"
            if "tick_diff" in row and pd.notna(row['tick_diff']):
                message += f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
                if "final_price_change_percent" in row:
                    message += f"   (آخرین: {row.get('last_price_change_percent',0):.2f}% | پایانی: {row['final_price_change_percent']:.2f}%)\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "godrat_kharid" in row and pd.notna(row['godrat_kharid']):
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji = "🟢" if row['pol_hagigi'] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    def format_filter_7_suspicious_volume(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "حجم مشکوک")

    def format_filter_8_swing_trade(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "نوسان‌گیری")

    def format_filter_9_first_hour(self, df: pd.DataFrame) -> str:
        return self._format_default_alert(df, "نیم ساعت اول (9:00-9:30)")

    def format_filter_10_heavy_buy_queue(self, df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        message = f"💰 <b>صف خرید با اردر سنگین</b>\n\n"
        for _, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"
            if "last_price" in row and pd.notna(row['last_price']):
                emoji_price = "🟢" if row.get('last_price_change_percent', 0) > 0 else "🔴"
                change_pct = row.get('last_price_change_percent', 0)
                message += f"💰 قیمت آخرین: {row['last_price']} ({emoji_price}<b>{change_pct:+.2f}%</b>)\n"
            if "buy_queue_value" in row and pd.notna(row['buy_queue_value']):
                message += f"🟢 <b>صف خرید: {self._format_billion(row['buy_queue_value'])} میلیارد تومان</b>\n"
            if "buy_order" in row and pd.notna(row['buy_order']):
                message += f"📋 سفارش هر کد: {row['buy_order']:.0f} میلیون تومان\n"
            if "value" in row and pd.notna(row['value']):
                message += f"💵 ارزش معاملات: {self._format_billion(row['value'])} میلیارد تومان\n"
            if "value_to_avg_monthly_value" in row and pd.notna(row['value_to_avg_monthly_value']):
                message += f"📊 ارزش / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"
            if "sarane_kharid" in row and pd.notna(row['sarane_kharid']):
                message += f"📈 سرانه خرید: {row['sarane_kharid']:.0f} میلیون تومان\n"
            if "godrat_kharid" in row and pd.notna(row['godrat_kharid']):
                message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n"
            if "pol_hagigi" in row and pd.notna(row['pol_hagigi']):
                emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
                message += f"{emoji} ورود پول حقیقی: {self._format_billion(row['pol_hagigi'])} میلیارد تومان\n"
            if "pol_hagigi_to_value" in row and pd.notna(row['pol_hagigi_to_value']):
                message += f"💎 پول حقیقی / ارزش معاملات: {row['pol_hagigi_to_value']:.2f}\n"
            message += "\n"
        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
        """ارسال پیام یک chunk - نسخه async"""
        if df.empty:
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
        try:
            message = format_func(df)
        except Exception as e:
            logger.error(f"❌ خطا در فرمت پیام فیلتر {filter_name}: {e}")
            return False

        if not message.strip():
            return False

        return await self.send_message(message)