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
    
    def __init__(self, channel_name: str = "📊 گزارش بورس"):
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
    
    # ========================================
    # فرمت پیش‌فرض برای همه فیلترها
    # ========================================
    def _format_default_alert(self, df: pd.DataFrame, alert_title: str) -> str:
        """
        فرمت پیش‌فرض برای هشدارها
        
        شامل اطلاعات پیش‌فرض:
        - نماد
        - قیمت آخرین و درصد تغییر
        - ارزش معاملات / میانگین ماهانه
        - پول حقیقی / میانگین ماهانه
        - سرانه خرید
        - قدرت خرید
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"🔔 <b>{alert_title}</b>\n\n"
        
        for idx, row in df.iterrows():
            # نماد (اول)
            message += f"📌 <b>{row['symbol']}</b>\n\n"
            
            # قیمت آخرین و درصد
            emoji_price = "🟢" if row['last_price_change_percent'] > 0 else "🔴"
            message += f"💰 قیمت آخرین: {row['last_price']:,} ریال "
            message += f"({emoji_price}<b>{row['last_price_change_percent']:+.2f}%</b>)\n"
            
            # ارزش معاملات / میانگین ماهانه
            message += f"📊 ارزش معاملات / میانگین ماهانه: <b>{row['value_to_avg_monthly_value']:.2f}x</b>\n"
            
            # پول حقیقی / میانگین ماهانه
            if 'pol_hagigi_to_avg_monthly_value' in row:
                pol_ratio = row['pol_hagigi_to_avg_monthly_value']
                message += f"💵 پول حقیقی / میانگین ماهانه: {pol_ratio:.2f}\n"
            
            # سرانه خرید
            message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n"
            
            # قدرت خرید
            message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # تاریخ، ساعت و کانال در آخر
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # فرمت پیام فیلتر 7: حجم مشکوک
    # ========================================
    def format_filter_7_suspicious_volume(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 7 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "حجم مشکوک")
    
    # ========================================
    # فرمت پیام فیلتر 8: نوسان‌گیری
    # ========================================
    def format_filter_8_swing_trade(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 8 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "نوسان‌گیری")
    
    # ========================================
    # فرمت پیام فیلتر 9: یک ساعت اول
    # ========================================
    def format_filter_9_first_hour(self, df: pd.DataFrame) -> str:
        """فرمت پیام فیلتر 9 با اطلاعات پیش‌فرض"""
        return self._format_default_alert(df, "۱ ساعت اول")
    
    # ========================================
    # فرمت پیام فیلتر 2: کراس سرانه خرید
    # ========================================
    def format_filter_2_sarane_cross(self, df: pd.DataFrame) -> str:
        """
        فرمت پیام برای فیلتر 2: کراس سرانه خرید
        
        فرمت:
        - هشدار کراس سرانه خرید
        - نماد
        - ارزش معاملات (نسبت به میانگین ماهانه)
        - قدرت خریدار
        - سرانه خرید
        - ورود پول حقیقی
        - ساعت و تاریخ
        - اسم کانال
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"🔔 <b>هشدار کراس سرانه خرید</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b> - {row['industry_name']}\n\n"
            
            # ارزش معاملات
            value_formatted = self._format_number(row['value'])
            value_ratio = row['value_to_avg_monthly_value']
            message += f"💰 ارزش معاملات: {value_formatted} تومان\n"
            message += f"   📊 نسبت به میانگین ماهانه: {value_ratio:.2f}x\n\n"
            
            # قدرت خریدار
            message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n\n"
            
            # سرانه خرید
            sarane_kharid_formatted = self._format_number(row['sarane_kharid'])
            message += f"📈 سرانه خرید: {sarane_kharid_formatted} تومان\n\n"
            
            # ورود پول حقیقی
            pol_hagigi_formatted = self._format_number(row['pol_hagigi'])
            emoji = "✅" if row['pol_hagigi'] > 0 else "⚠️"
            message += f"{emoji} ورود پول حقیقی: {pol_hagigi_formatted}\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # اسم کانال در انتها
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # فرمت پیام فیلتر 3: هشدار نمادهای خاص
    # ========================================
    def format_filter_3_watchlist(self, df: pd.DataFrame) -> str:
        """
        فرمت پیام برای فیلتر 3: هشدار درصد تغییر نمادهای خاص
        
        فرمت:
        - هشدار عبور از آستانه
        - نماد
        - درصد تغییر آخرین
        - آستانه تعریف شده
        - قیمت آخرین
        - حجم و ارزش
        - ساعت و تاریخ
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"⚠️ <b>هشدار عبور از آستانه</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            # ایموجی بر اساس درصد تغییر
            if row['last_price_change_percent'] > 5:
                emoji = "🚀"
            elif row['last_price_change_percent'] > 3:
                emoji = "📈"
            else:
                emoji = "✅"
            
            message += f"{emoji} <b>{row['symbol']}</b> - {row['industry_name']}\n\n"
            
            # درصد تغییر و آستانه
            message += f"📊 درصد تغییر آخرین: <b>{row['last_price_change_percent']:.2f}%</b>\n"
            message += f"🎯 آستانه تعریف شده: {row['threshold']:.2f}%\n"
            message += f"🔺 عبور: +{row['last_price_change_percent'] - row['threshold']:.2f}%\n\n"
            
            # قیمت
            message += f"💰 قیمت آخرین: {row['last_price']:,} ریال\n"
            message += f"💵 قیمت پایانی: {row['final_price']:,} ریال\n\n"
            
            # حجم و ارزش
            volume_formatted = self._format_number(row['volume'])
            value_formatted = self._format_number(row['value'])
            message += f"📦 حجم: {volume_formatted}\n"
            message += f"💰 ارزش: {value_formatted} تومان\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # اسم کانال
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # فرمت پیام فیلتر 4: صف خرید سنگین در سقف
    # ========================================
    def format_filter_4_ceiling_queue(self, df: pd.DataFrame) -> str:
        """
        فرمت پیام برای فیلتر 4: صف خرید سنگین در سقف قیمت
        
        فرمت:
        - عنوان: سنگین‌ترین صف‌های خرید در سقف
        - نماد و صنعت
        - درصد تغییر قیمت (در سقف دامنه)
        - قیمت پایانی
        - ارزش صف خرید
        - ارزش صف فروش
        - ارزش معاملات
        - ساعت و تاریخ
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"🔥 <b>صف‌های خرید سنگین در سقف قیمت</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            message += f"🎯 <b>{row['symbol']}</b> - {row['industry_name']}\n\n"
            
            # درصد تغییر (سقف دامنه)
            message += f"📊 تغییر قیمت: <b>+{row['final_price_change_percent']:.2f}%</b> (سقف مثبت)\n"
            message += f"💰 قیمت پایانی: {row['final_price']:,} ریال\n\n"
            
            # صف خرید (سنگین!)
            buy_queue_formatted = self._format_number(row['buy_order_value'])
            message += f"🟢 <b>ارزش صف خرید: {buy_queue_formatted} تومان</b>\n"
            
            # صف فروش (صفر یا کم)
            sell_queue_formatted = self._format_number(row['sell_order_value'])
            if row['sell_order_value'] == 0:
                message += f"🔴 ارزش صف فروش: <b>صفر</b> ✅\n\n"
            else:
                message += f"🔴 ارزش صف فروش: {sell_queue_formatted} تومان\n\n"
            
            # اطلاعات تکمیلی
            value_formatted = self._format_number(row['value'])
            volume_formatted = self._format_number(row['volume'])
            message += f"💵 ارزش معاملات: {value_formatted} تومان\n"
            message += f"📦 حجم معاملات: {volume_formatted}\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # اسم کانال
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # فرمت پیام فیلتر 5: نسبت پول حقیقی
    # ========================================
    def format_filter_5_pol_hagigi_ratio(self, df: pd.DataFrame) -> str:
        """
        فرمت پیام برای فیلتر 5: نسبت پول حقیقی به ارزش معاملات
        
        فرمت کامل شامل:
        - نماد و صنعت
        - نسبت پول حقیقی به میانگین ماهانه
        - ورود پول حقیقی
        - میانگین ارزش معاملات ماهانه
        - ارزش معاملات امروز
        - قدرت خریدار
        - سرانه خرید
        - تغییر قیمت
        - حجم معاملات
        - ساعت و تاریخ
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"💎 <b>هشدار ورود پول حقیقی قوی</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            # ایموجی بر اساس نسبت
            if row['pol_hagigi_to_avg_monthly_value'] > 2:
                emoji = "🔥"
            elif row['pol_hagigi_to_avg_monthly_value'] > 1:
                emoji = "⭐"
            else:
                emoji = "✅"
            
            message += f"{emoji} <b>{row['symbol']}</b> - {row['industry_name']}\n\n"
            
            # نسبت پول حقیقی (مهم‌ترین!)
            message += f"📊 <b>نسبت پول حقیقی: {row['pol_hagigi_to_avg_monthly_value']:.2f}x</b>\n"
            message += f"   (پول حقیقی / میانگین ماهانه)\n\n"
            
            # پول حقیقی
            pol_formatted = self._format_number(row['pol_hagigi'])
            emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
            message += f"{emoji_pol} ورود پول حقیقی: {pol_formatted} تومان\n\n"
            
            # ارزش معاملات
            value_formatted = self._format_number(row['value'])
            avg_monthly_formatted = self._format_number(row['avg_monthly_value'])
            message += f"💰 ارزش معاملات امروز: {value_formatted} تومان\n"
            message += f"📈 میانگین ماهانه: {avg_monthly_formatted} تومان\n\n"
            
            # قدرت خرید و سرانه
            message += f"💪 قدرت خریدار: {row['godrat_kharid']:.2f}\n"
            sarane_formatted = self._format_number(row['sarane_kharid'] / 1000)  # تبدیل به میلیارد
            message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n\n"
            
            # تغییر قیمت و حجم
            emoji_price = "🟢" if row['final_price_change_percent'] > 0 else "🔴"
            message += f"{emoji_price} تغییر قیمت: {row['final_price_change_percent']:+.2f}%\n"
            message += f"💵 قیمت پایانی: {row['final_price']:,} ریال\n"
            volume_formatted = self._format_number(row['volume'] / 1_000_000_000)  # به میلیارد سهم
            message += f"📦 حجم: {row['volume']:,} سهم\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # اسم کانال
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # فرمت پیام فیلتر 6: تیک و ساعت
    # ========================================
    def format_filter_6_tick_time(self, df: pd.DataFrame) -> str:
        """
        فرمت پیام برای فیلتر 6: تیک و ساعت
        
        فرمت شامل:
        - نماد (اول)
        - قیمت آخرین و درصد تغییر آخرین
        - اختلاف تیک (درصد آخرین - درصد پایانی)
        - ارزش معاملات و نسبت به میانگین ماهانه
        - ورود پول حقیقی
        - سرانه خرید
        - قدرت خرید
        """
        if df.empty:
            return ""
        
        # تاریخ و ساعت فارسی
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"⏰ <b>تیک و ساعت</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            # نماد اول
            message += f"📌 <b>{row['symbol']}</b> - {row['industry_name']}\n\n"
            
            # قیمت آخرین و درصد تغییر
            message += f"💰 قیمت آخرین: {row['last_price']:,} ریال "
            message += f"(<b>{row['last_price_change_percent']:+.2f}%</b>)\n\n"
            
            # تیک مثبت (اختلاف درصدها)
            message += f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
            message += f"   (آخرین: {row['last_price_change_percent']:.2f}% | "
            message += f"پایانی: {row['final_price_change_percent']:.2f}%)\n\n"
            
            # ارزش معاملات
            value_formatted = self._format_number(row['value'])
            value_ratio = row['value_to_avg_monthly_value']
            message += f"💵 ارزش معاملات: {value_formatted} تومان\n"
            message += f"📊 نسبت به میانگین ماهانه: {value_ratio:.2f}x\n\n"
            
            # ورود پول حقیقی
            pol_formatted = self._format_number(row['pol_hagigi'])
            emoji_pol = "🟢" if row['pol_hagigi'] > 0 else "🔴"
            message += f"{emoji_pol} ورود پول حقیقی: {pol_formatted} تومان\n\n"
            
            # سرانه خرید و قدرت خرید
            message += f"📈 سرانه خرید: {row['sarane_kharid']:.1f} میلیون تومان\n"
            message += f"💪 قدرت خرید: {row['godrat_kharid']:.2f}\n\n"
            
            message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # اسم کانال
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # ارسال نتایج فیلتر
    # ========================================
    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
        """
        ارسال هشدار یک فیلتر
        
        Args:
            df: DataFrame فیلتر شده
            filter_name: نام فیلتر
            
        Returns:
            True در صورت موفقیت
        """
        if df.empty:
            logger.info(f"فیلتر {filter_name}: سهمی یافت نشد")
            return False
        
        # انتخاب فرمت بر اساس نام فیلتر
        if filter_name == 'filter_2_sarane_cross':
            message = self.format_filter_2_sarane_cross(df)
        elif filter_name == 'filter_3_watchlist':
            message = self.format_filter_3_watchlist(df)
        elif filter_name == 'filter_4_ceiling_queue':
            message = self.format_filter_4_ceiling_queue(df)
        elif filter_name == 'filter_5_pol_hagigi_ratio':
            message = self.format_filter_5_pol_hagigi_ratio(df)
        elif filter_name == 'filter_6_tick_time':
            message = self.format_filter_6_tick_time(df)
        elif filter_name == 'filter_7_suspicious_volume':
            message = self.format_filter_7_suspicious_volume(df)
        elif filter_name == 'filter_8_swing_trade':
            message = self.format_filter_8_swing_trade(df)
        elif filter_name == 'filter_9_first_hour':
            message = self.format_filter_9_first_hour(df)
        else:
            # فرمت پیش‌فرض
            message = self._format_default_alert(df, filter_name)
        
        if not message:
            return False
        
        return await self.send_message(message)
    
    def send_filter_alert_sync(self, df: pd.DataFrame, filter_name: str) -> bool:
        """نسخه همگام send_filter_alert"""
        return asyncio.run(self.send_filter_alert(df, filter_name))
    
    # ========================================
    # فرمت پیش‌فرض (برای سایر فیلترها)
    # ========================================
    def _format_default(self, df: pd.DataFrame, filter_name: str) -> str:
        """فرمت پیش‌فرض برای فیلترهایی که فرمت خاص ندارند"""
        if df.empty:
            return ""
        
        now = jdatetime.datetime.now()
        date_str = now.strftime('%Y/%m/%d')
        time_str = now.strftime('%H:%M')
        
        message = f"🔔 <b>هشدار {filter_name}</b>\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += "━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, row in df.iterrows():
            message += f"📌 <b>{row['symbol']}</b>\n"
            message += f"   صنعت: {row['industry_name']}\n"
            message += f"   قیمت: {row['last_price']:,} ریال\n"
            message += f"   تغییر: {row['final_price_change_percent']:.2f}%\n\n"
        
        message += f"📢 {self.channel_name}"
        
        return message
    
    # ========================================
    # توابع کمکی
    # ========================================
    def _format_number(self, num: float) -> str:
        """
        فرمت‌بندی اعداد
        
        توجه: اعداد از قبل به میلیارد یا میلیون تبدیل شده‌اند
        """
        if pd.isna(num):
            return "0"
        
        # اگه عدد بزرگ‌تر از 1000 میلیارد
        if abs(num) >= 1000:
            return f"{num:.1f} هزار میلیارد"
        # اگه عدد به میلیارد
        elif abs(num) >= 1:
            return f"{num:.2f} میلیارد"
        # اگه عدد خیلی کوچیک (کمتر از 1 میلیارد)
        elif abs(num) >= 0.001:
            return f"{num*1000:.1f} میلیون"
        else:
            return f"{num:.3f}"