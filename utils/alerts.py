import asyncio
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import pandas as pd
import jdatetime
import pytz
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
import logging

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

LineBuilder = Callable[[pd.Series], Optional[str]]


# ============================================================
# فرمترهای پایه (بدون تغییر نسبت به نسخه‌ی قبلی)
# ============================================================
def _format_symbol_hashtag(symbol: str) -> str:
    if pd.isna(symbol):
        return ""
    return str(symbol).replace(" ", "_").replace("\u200c", "_").strip()


def _format_billion(value: float) -> str:
    if pd.isna(value) or value == 0:
        return "0"
    return f"{value:.2f}" if value >= 1 else f"{value:.3f}"


def _format_price(value: float) -> str:
    if pd.isna(value):
        return "0"
    return f"{value:,.0f}"


# ============================================================
# Line builders مشترک — پارامتری، برای پوشش تفاوت‌های واقعی
# (بولد/غیربولد، لیبل، نام ستون) بدون کپی کد
# ============================================================
def line_price(row: pd.Series) -> Optional[str]:
    if "last_price" not in row or pd.isna(row["last_price"]):
        return None
    change_pct = row.get("last_price_change_percent", 0)
    emoji = "🟢" if change_pct > 0 else "🔴"
    return (
        f"💰 قیمت آخرین: {_format_price(row['last_price'])} "
        f"({emoji}<b>{change_pct:+.2f}%</b>)\n"
    )


def line_value(row: pd.Series) -> Optional[str]:
    if "value" not in row or pd.isna(row["value"]):
        return None
    return f"💵 ارزش معاملات: {_format_billion(row['value'])} میلیارد تومان\n"


def line_value_ratio(bold: bool = True) -> LineBuilder:
    def _builder(row: pd.Series) -> Optional[str]:
        if "value_to_avg_monthly_value" not in row or pd.isna(
            row["value_to_avg_monthly_value"]
        ):
            return None
        pct = row["value_to_avg_monthly_value"] * 100
        text = f"{pct:.0f}%"
        if bold:
            text = f"<b>{text}</b>"
        return f"📊 حجم نسبی: {text}\n"

    return _builder


def line_sarane_kharid(label: str = "سرانه خرید", bold: bool = False) -> LineBuilder:
    def _builder(row: pd.Series) -> Optional[str]:
        if "sarane_kharid" not in row or pd.isna(row["sarane_kharid"]):
            return None
        val = f"{row['sarane_kharid']:.0f}"
        if bold:
            val = f"<b>{val}</b>"
        return f"📈 {label}: {val} میلیون تومان\n"

    return _builder


def line_godrat_kharid(label: str = "قدرت خرید", bold: bool = False) -> LineBuilder:
    def _builder(row: pd.Series) -> Optional[str]:
        if "godrat_kharid" not in row or pd.isna(row["godrat_kharid"]):
            return None
        text = f"{label}: {row['godrat_kharid']:.2f}"
        if bold:
            text = f"<b>{text}</b>"
        return f"💪 {text}\n"

    return _builder


def line_pol_hagigi(always_negative_abs: bool = False) -> LineBuilder:
    """
    always_negative_abs=True -> حالت filter_11: همیشه 🔴 و مقدار مطلق
    (چون این فیلتر خروج پول حقیقی رو نشون می‌ده، نه ورود).
    """

    def _builder(row: pd.Series) -> Optional[str]:
        if "pol_hagigi" not in row or pd.isna(row["pol_hagigi"]):
            return None
        if always_negative_abs:
            return f"🔴 پول حقیقی: {_format_billion(abs(row['pol_hagigi']))} میلیارد تومان\n"
        emoji = "🟢" if row["pol_hagigi"] > 0 else "🔴"
        return f"{emoji} ورود پول حقیقی: {_format_billion(row['pol_hagigi'])} میلیارد تومان\n"

    return _builder


def line_pol_power(column: str = "pol_hagigi_to_avg_monthly_value") -> LineBuilder:
    """حالت پیش‌فرض: 💎 قدرت پول از ستون pol_hagigi_to_avg_monthly_value"""

    def _builder(row: pd.Series) -> Optional[str]:
        if column not in row or pd.isna(row[column]):
            return None
        return f"💎 قدرت پول: {row[column] * 100:.0f}%\n"

    return _builder


def line_pol_power_negative(column: str = "pol_hagigi_to_value") -> LineBuilder:
    """حالت filter_11: 🔻 با ستون pol_hagigi_to_value (بدون علامت +)"""

    def _builder(row: pd.Series) -> Optional[str]:
        if column not in row or pd.isna(row[column]):
            return None
        return f"🔻 قدرت پول: {row[column] * 100:.0f}%\n"

    return _builder


# ---- Line builderهای اختصاصی (تأیید شده به‌عنوان عمدی) ----
def line_threshold(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_3: فاصله از آستانه‌ی watchlist"""
    if "threshold" not in row:
        return None
    percent = row.get("last_price_change_percent", 0)
    return f"🔺 عبور از آستانه: +{percent - row['threshold']:.2f}%\n"


def line_final_price(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_3"""
    if "final_price" not in row or pd.isna(row["final_price"]):
        return None
    return f"💵 قیمت پایانی: {_format_price(row['final_price'])}\n"


def line_tick_diff(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_6"""
    if "tick_diff" not in row or pd.isna(row["tick_diff"]):
        return None
    line = f"📈 <b>تیک: +{row['tick_diff']:.2f}%</b>\n"
    if "final_price_change_percent" in row:
        line += (
            f"   (آخرین: {row.get('last_price_change_percent', 0):.2f}% | "
            f"پایانی: {row['final_price_change_percent']:.2f}%)\n"
        )
    return line


def line_buy_queue_value(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_10 (فقط از API دوم میاد)"""
    if "buy_queue_value" not in row or pd.isna(row["buy_queue_value"]):
        return None
    return f"🟢 <b>صف خرید: {_format_billion(row['buy_queue_value'])} میلیارد تومان</b>\n"


def line_buy_order(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_10 (فقط از API دوم میاد)"""
    if "buy_order" not in row or pd.isna(row["buy_order"]):
        return None
    return f"📋 سفارش هر کد: {row['buy_order']:.0f} میلیون تومان\n"


def line_godrat_5day_avg(row: pd.Series) -> Optional[str]:
    """اختصاصی filter_1"""
    if "5_day_godrat_kharid" not in row or pd.isna(row["5_day_godrat_kharid"]):
        return None
    return f"📉 میانگین قدرت خرید 5 روز: {row['5_day_godrat_kharid']:.2f}\n"


# ============================================================
# رجیستری اعلانی
# ============================================================
@dataclass
class FilterDisplay:
    hashtag: str  # خط اول پیام، مثل "💪#قدرت_خرید_قوی"
    header_emoji: Callable[[pd.Series], str]  # اموجی پویا برای هر ردیف
    show_industry: bool  # آیا "- نام صنعت" بعد از نماد نشون داده بشه
    lines: List[LineBuilder] = field(default_factory=list)


def _static_emoji(symbol: str) -> Callable[[pd.Series], str]:
    return lambda row: symbol


FILTER_DISPLAY_CONFIG = {
    "filter_1_strong_buying": FilterDisplay(
        hashtag="💪#قدرت_خرید_قوی",
        header_emoji=lambda row: (
            "🔥" if row.get("godrat_kharid", 0) > 3
            else "⚡" if row.get("godrat_kharid", 0) > 2
            else "✅"
        ),
        show_industry=True,
        lines=[
            line_price, line_value, line_value_ratio(bold=True),
            line_sarane_kharid(bold=False),
            line_godrat_kharid(bold=True),
            line_godrat_5day_avg,
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_2_sarane_cross": FilterDisplay(
        hashtag="🔔#کراس_سرانه_خرید",
        header_emoji=_static_emoji("📌"),
        show_industry=True,
        lines=[
            line_price, line_value,
            line_value_ratio(bold=True),  # FIX: قبلاً بولد نبود، حالا یکسان با بقیه
            line_sarane_kharid(label="سرانه خرید"),
            line_godrat_kharid(label="قدرت خرید"),  # FIX: قبلاً "قدرت خریدار" بود
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_3_watchlist": FilterDisplay(
        hashtag="⚠️#عبور_از_آستانه",
        header_emoji=lambda row: (
            "🚀" if row.get("last_price_change_percent", 0) > 5
            else "📈" if row.get("last_price_change_percent", 0) > 3
            else "✅"
        ),
        show_industry=True,
        lines=[
            line_price, line_threshold, line_final_price, line_value,
            line_value_ratio(bold=True),
            line_sarane_kharid(),
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_4_ceiling_queue": FilterDisplay(
        hashtag="🔥#رنج_مثبت",
        header_emoji=_static_emoji("🎯"),
        show_industry=True,
        lines=[
            line_price, line_value, line_value_ratio(bold=True),
            line_sarane_kharid(),
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_5_pol_hagigi_ratio": FilterDisplay(
        hashtag="💎#ورود_پول_حقیقی_قوی",
        header_emoji=lambda row: (
            "🔥" if row.get("pol_hagigi_to_avg_monthly_value", 0) > 2
            else "⭐" if row.get("pol_hagigi_to_avg_monthly_value", 0) > 1
            else "✅"
        ),
        show_industry=True,
        lines=[
            line_price, line_value, line_value_ratio(bold=True),
            line_sarane_kharid(),
            line_godrat_kharid(),  # FIX: قبلاً "قدرت خریدار" بود
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_6_tick_time": FilterDisplay(
        hashtag="⏰#تیک_و_ساعت",
        header_emoji=_static_emoji("📌"),
        show_industry=True,
        lines=[
            line_price, line_tick_diff, line_value, line_value_ratio(bold=True),
            line_sarane_kharid(),
            line_godrat_kharid(),
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_7_suspicious_volume": None,   # از default_alert استفاده می‌کنه
    "filter_8_swing_trade": None,
    "filter_9_first_hour": None,
    "filter_10_heavy_buy_queue": FilterDisplay(
        hashtag="💰#صف_خرید_با_اردر_سنگین",
        header_emoji=_static_emoji("📌"),
        show_industry=False,  # نسخه‌ی قبلی هم اینجا industry نداشت (داده از API دوم میاد)
        lines=[
            line_price, line_buy_queue_value, line_buy_order,
            line_value, line_value_ratio(bold=True),
            line_sarane_kharid(),
            line_godrat_kharid(),
            line_pol_hagigi(),
            line_pol_power(),
        ],
    ),
    "filter_11_hoghooghi_haghighi_strong_buy": FilterDisplay(
        hashtag="💪#خرید_حقوقی_و_حقیقی_قوی",
        header_emoji=lambda row: (
            "🔥" if abs(row.get("pol_hagigi_to_value", 0)) > 0.5
            else "⚡" if abs(row.get("pol_hagigi_to_value", 0)) > 0.3
            else "✅"
        ),
        show_industry=True,
        lines=[
            line_price, line_value, line_value_ratio(bold=True),
            line_sarane_kharid(),  # FIX: قبلاً بولد بود، حالا یکسان با بقیه
            line_godrat_kharid(),
            line_pol_hagigi(always_negative_abs=True),
            line_pol_power_negative(),
        ],
    ),
}

DEFAULT_ALERT_TITLES = {
    "filter_7_suspicious_volume": "#حجم_مشکوک",
    "filter_8_swing_trade": "#نوسان‌_گیری",
    "filter_9_first_hour": "#نیم_ساعت_اول",
}
DEFAULT_ALERT_LINES = [
    line_price, line_value, line_value_ratio(bold=True),
    line_sarane_kharid(), line_godrat_kharid(), line_pol_hagigi(), line_pol_power(),
]


class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام - نسخه Async & Parallel"""

    def __init__(self, channel_name: str = "@tehran_stock_alerts"):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)
        self.semaphore = asyncio.Semaphore(3)

    def _current_tehran_jdatetime(self):
        tehran_tz = pytz.timezone("Asia/Tehran")
        now = jdatetime.datetime.now(tehran_tz)
        return now.strftime("%Y/%m/%d"), now.strftime("%H:%M")

    async def send_message(self, message: str, parse_mode: str = "HTML") -> bool:
        async with self.semaphore:
            try:
                await self.bot.send_message(
                    chat_id=self.chat_id, text=message, parse_mode=parse_mode
                )
                await asyncio.sleep(4)
                return True
            except RetryAfter as e:
                logger.warning(f"⚠️ Flood control: انتظار {e.retry_after} ثانیه")
                await asyncio.sleep(e.retry_after)
            except TimedOut:
                logger.warning("⚠️ Timeout - تلاش مجدد")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ خطا در ارسال پیام: {e}")
                return False

            try:
                await self.bot.send_message(
                    chat_id=self.chat_id, text=message, parse_mode=parse_mode
                )
                return True
            except Exception as retry_error:
                logger.error(f"❌ خطا در تلاش مجدد: {retry_error}")
                return False

    # ------------------------------------------------------------
    # موتور رندر مشترک
    # ------------------------------------------------------------
    def _render(self, df: pd.DataFrame, filter_name: str) -> str:
        if df.empty:
            return ""

        cfg = FILTER_DISPLAY_CONFIG.get(filter_name)

        if cfg is None:
            title = DEFAULT_ALERT_TITLES.get(filter_name, filter_name)
            message = f"🔔 <b>{title}</b>\n\n"
            for _, row in df.iterrows():
                message += f"📌 <b>#{_format_symbol_hashtag(row['symbol'])}</b>"
                # FIX: قبلاً industry_name اصلاً نشون داده نمی‌شد؛ حالا یکسان با بقیه‌ی فیلترها
                if "industry_name" in row and pd.notna(row["industry_name"]):
                    message += f" - {row['industry_name']}\n"
                else:
                    message += "\n"
                for builder in DEFAULT_ALERT_LINES:
                    line = builder(row)
                    if line:
                        message += line
                message += "\n"
        else:
            message = f"{cfg.hashtag}\n\n"
            for _, row in df.iterrows():
                emoji = cfg.header_emoji(row)
                message += f"{emoji} <b>#{_format_symbol_hashtag(row['symbol'])}</b>"
                if cfg.show_industry and "industry_name" in row:
                    message += f" - {row['industry_name']}\n"
                else:
                    message += "\n"
                for builder in cfg.lines:
                    line = builder(row)
                    if line:
                        message += line
                message += "\n"

        date_str, time_str = self._current_tehran_jdatetime()
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {self.channel_name}"
        return message

    # ------------------------------------------------------------
    # Wrapperهای نام‌دار (سازگاری با کد قدیمی/تست‌های موجود)
    # ------------------------------------------------------------
    def format_filter_1_strong_buying(self, df):
        return self._render(df, "filter_1_strong_buying")

    def format_filter_2_sarane_cross(self, df):
        return self._render(df, "filter_2_sarane_cross")

    def format_filter_3_watchlist(self, df):
        return self._render(df, "filter_3_watchlist")

    def format_filter_4_ceiling_queue(self, df):
        return self._render(df, "filter_4_ceiling_queue")

    def format_filter_5_pol_hagigi_ratio(self, df):
        return self._render(df, "filter_5_pol_hagigi_ratio")

    def format_filter_6_tick_time(self, df):
        return self._render(df, "filter_6_tick_time")

    def format_filter_7_suspicious_volume(self, df):
        return self._render(df, "filter_7_suspicious_volume")

    def format_filter_8_swing_trade(self, df):
        return self._render(df, "filter_8_swing_trade")

    def format_filter_9_first_hour(self, df):
        return self._render(df, "filter_9_first_hour")

    def format_filter_10_heavy_buy_queue(self, df):
        return self._render(df, "filter_10_heavy_buy_queue")

    def format_filter_11_hoghooghi_haghighi_strong_buy(self, df):
        return self._render(df, "filter_11_hoghooghi_haghighi_strong_buy")

    async def send_filter_alert(self, df: pd.DataFrame, filter_name: str) -> bool:
        """ارسال پیام یک chunk - نسخه async"""
        if df.empty:
            return False
        try:
            message = self._render(df, filter_name)
        except Exception as e:
            logger.error(f"❌ خطا در فرمت پیام فیلتر {filter_name}: {e}")
            return False

        if not message.strip():
            return False
        return await self.send_message(message)
