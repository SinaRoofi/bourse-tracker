import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

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
    return f"{value:.2f}" if abs(value) >= 1 else f"{value:.3f}"


def _format_price(value: float) -> str:
    if pd.isna(value):
        return "0"
    return f"{value:,.0f}"


def _format_marketcap_trillion(value: float) -> str:
    """کمتر از 1 (هزار میلیارد) -> 2 رقم اعشار (مثلاً 0.75)؛ در غیر این صورت بدون اعشار."""
    if pd.isna(value) or value == 0:
        return "0"
    return f"{value:.2f}"


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


def line_marketcap(row: pd.Series) -> Optional[str]:
    if "marketcap" not in row or pd.isna(row["marketcap"]):
        return None
    value_in_trillion = row["marketcap"] / 1000  # میلیارد -> هزار میلیارد تومان
    return f"🏦 ارزش بازار: {_format_marketcap_trillion(value_in_trillion)} هزار میلیارد تومان\n"


def line_5_day_return(row: pd.Series) -> Optional[str]:
    """بازده هفتگی (بازدهی 5 روز اخیر)"""
    if "5_day_return" not in row or pd.isna(row["5_day_return"]):
        return None
    value = row["5_day_return"]
    emoji = "🟢" if value > 0 else "🔴" if value < 0 else "⚪"
    return f"{emoji} بازده هفتگی: <b>{value:+.2f}%</b>\n"


def line_20_day_return(row: pd.Series) -> Optional[str]:
    """بازدهی ماهانه (بازدهی 20 روز اخیر)"""
    if "20_day_return" not in row or pd.isna(row["20_day_return"]):
        return None
    value = row["20_day_return"]
    emoji = "🟢" if value > 0 else "🔴" if value < 0 else "⚪"
    return f"{emoji} بازده ماهانه: <b>{value:+.2f}%</b>\n"


def line_value_5_to_20(row: pd.Series) -> Optional[str]:
    """
    میانگین ارزش معاملات هفتگی (5 روزه) در مقایسه با میانگین ماهانه (20 روزه)
    و میانگین 3 ماهه (60 روزه). سبز فقط وقتی که هفته‌ی اخیر از هر دو
    بیشتر باشه (تأیید دوطرفه، کمتر مستعد bias خودتزریقی میانگین 20 روزه).
    """
    has_20 = "value_5_to_20_ratio" in row and pd.notna(row["value_5_to_20_ratio"])
    has_60 = "value_5_to_60_ratio" in row and pd.notna(row["value_5_to_60_ratio"])
    if not has_20 and not has_60:
        return None

    ratio_20 = row["value_5_to_20_ratio"] if has_20 else None
    ratio_60 = row["value_5_to_60_ratio"] if has_60 else None

    if has_20 and has_60:
        emoji = "🟢" if (ratio_20 > 1 and ratio_60 > 1) else "🔴"
        return f"{emoji} حجم هفتگی: <b>{ratio_20:.2f}x</b> ماهانه، <b>{ratio_60:.2f}x</b> سه‌ماهه\n"
    elif has_20:
        emoji = "🟢" if ratio_20 > 1 else "🔴"
        return f"{emoji} حجم هفتگی به ماهانه: <b>{ratio_20:.2f}x</b>\n"
    else:
        emoji = "🟢" if ratio_60 > 1 else "🔴"
        return f"{emoji} حجم هفتگی به ۳ماهه: <b>{ratio_60:.2f}x</b>\n"


def line_diff_buy_sell_order(row: pd.Series) -> Optional[str]:
    if "diff_buy_sell_order" not in row or pd.isna(row["diff_buy_sell_order"]):
        return None
    value = row["diff_buy_sell_order"]
    emoji = "🟢" if value > 0 else "🔴" if value < 0 else "⚪"
    return f"{emoji} چربش اردر: {value:+.2f} میلیارد تومان\n"


def line_sarane_kharid(label: str = "سرانه خرید", bold: bool = False) -> LineBuilder:
    def _builder(row: pd.Series) -> Optional[str]:
        if "sarane_kharid" not in row or pd.isna(row["sarane_kharid"]):
            return None
        val = f"{row['sarane_kharid']:.0f}"
        if bold:
            val = f"<b>{val}</b>"
        return f"📈 {label}: {val} میلیون تومان\n"

    return _builder


def line_sarane_diff(row: pd.Series) -> Optional[str]:
    if "sarane_kharid" not in row or "sarane_forosh" not in row:
        return None
    if pd.isna(row["sarane_kharid"]) or pd.isna(row["sarane_forosh"]):
        return None
    diff = row["sarane_kharid"] - row["sarane_forosh"]
    emoji = "🟢" if diff > 0 else "🔴" if diff < 0 else "⚪"
    return f"{emoji} اختلاف سرانه: {diff:+.0f} میلیون تومان\n"


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


def line_pol_hagigi_weekly(row: pd.Series) -> Optional[str]:
    """
    پول حقیقی هفتگی (مجموع تجمعی ورود/خروج پول حقیقی طی 5 روز اخیر).
    سبز = مجموع مثبت (ورود پول)، قرمز = مجموع منفی (خروج پول).
    """
    if "5_day_pol_hagigi" not in row or pd.isna(row["5_day_pol_hagigi"]):
        return None
    value = row["5_day_pol_hagigi"]
    emoji = "🟢" if value > 0 else "🔴" if value < 0 else "⚪"
    return f"{emoji} پول حقیقی هفتگی: {_format_billion(value)} میلیارد تومان\n"


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


def line_bubble(row: pd.Series) -> Optional[str]:
    """فقط صندوق‌ها: حباب فعلی + میانگین حباب 1 ماهه (سهام bubble_percent ندارن -> None)"""
    if "bubble_percent" not in row or pd.isna(row["bubble_percent"]):
        return None
    bubble = row["bubble_percent"]
    avg_bubble = row.get("avg_1_month_bubble")
    has_avg = avg_bubble is not None and pd.notna(avg_bubble)

    if has_avg:
        # سبز = حباب فعلی کمتر از میانگین (ارزون‌تر از حد معمول)
        # قرمز = حباب فعلی بیشتر از میانگین (گرون‌تر از حد معمول)
        emoji = "🟢" if bubble < avg_bubble else "🔴" if bubble > avg_bubble else "⚪"
    else:
        emoji = "🔴" if bubble > 0 else "🟢" if bubble < 0 else "⚪"

    line = f"{emoji} حباب: <b>{bubble:+.2f}%</b>\n"
    if has_avg:
        line += f"📊 میانگین حباب ماهانه: {avg_bubble:+.2f}%\n"
    return line


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
            line_price, line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(bold=False),
            line_sarane_diff,
            line_godrat_kharid(bold=True),
            line_godrat_5day_avg,
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
        ],
    ),
    "filter_2_sarane_cross": FilterDisplay(
        hashtag="🔔#کراس_سرانه_خرید",
        header_emoji=_static_emoji("📌"),
        show_industry=True,
        lines=[
            line_price, line_value,
            line_value_ratio(bold=True), line_value_5_to_20,  # FIX: قبلاً بولد نبود، حالا یکسان با بقیه
            line_sarane_kharid(label="سرانه خرید"),
            line_sarane_diff,
            line_godrat_kharid(label="قدرت خرید"),  # FIX: قبلاً "قدرت خریدار" بود
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
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
            line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),
            line_sarane_diff,
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
        ],
    ),
    "filter_4_range_mosbat": FilterDisplay(
        hashtag="🔥#رنج_مثبت",
        header_emoji=_static_emoji("🎯"),
        show_industry=True,
        lines=[
            line_price, line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),
            line_sarane_diff,
            line_godrat_kharid(),  # FIX: خط قدرت خرید جا افتاده بود
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
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
            line_price, line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),
            line_sarane_diff,
            line_godrat_kharid(),  # FIX: قبلاً "قدرت خریدار" بود
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
        ],
    ),
    "filter_6_tick_time": FilterDisplay(
        hashtag="⏰#تیک_و_ساعت",
        header_emoji=_static_emoji("📌"),
        show_industry=True,
        lines=[
            line_price, line_tick_diff, line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),
            line_sarane_diff,
            line_godrat_kharid(),
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
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
            line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),
            line_sarane_diff,
            line_godrat_kharid(),
            line_pol_hagigi(),
            line_pol_hagigi_weekly,
            line_pol_power(),
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
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
            line_price, line_value, line_value_ratio(bold=True), line_value_5_to_20,
            line_sarane_kharid(),  # FIX: قبلاً بولد بود، حالا یکسان با بقیه
            line_sarane_diff,
            line_godrat_kharid(),
            line_pol_hagigi(always_negative_abs=True),
            line_pol_hagigi_weekly,
            line_pol_power_negative(),
            line_diff_buy_sell_order,
            line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
        ],
    ),
}

DEFAULT_ALERT_TITLES = {
    "filter_7_suspicious_volume": "#حجم_مشکوک",
    "filter_8_swing_trade": "#نوسان‌_گیری",
    "filter_9_first_hour": "#نیم_ساعت_اول",
}
DEFAULT_ALERT_LINES = [
    line_price, line_value, line_value_ratio(bold=True), line_value_5_to_20,
    line_sarane_kharid(), line_sarane_diff, line_godrat_kharid(), line_pol_hagigi(),
    line_pol_hagigi_weekly,
    line_pol_power(),
    line_diff_buy_sell_order,
    line_bubble, line_5_day_return, line_20_day_return, line_marketcap,
]


class TelegramAlert:
    """کلاس ارسال هشدارها به تلگرام - نسخه Async & Parallel"""

    # فاصله‌ی حداقلی بین دو ارسال متوالی به یک چت مشخص (per-chat).
    # طبق core.telegram.org/bots/faq: تو یک چت معمولی ~1 پیام/ثانیه؛ ولی طبق
    # مستندات python-telegram-bot (AIORateLimiter)، چون API نمی‌تونه کانال
    # رو از سوپرگروه تشخیص بده، کانال‌ها هم زیر محدودیت گروهی (۲۰ پیام/دقیقه
    # ≈ هر ۳ ثانیه) قرار می‌گیرن نه ۱ ثانیه. تو لاگ واقعی این پروژه با sleep=4s
    # هیچ RetryAfterای دیده نشد با نرخ ~۰.۷۵ پیام/ثانیه به یک کانال - یعنی رفتار
    # واقعی این چت‌ها محافظه‌کارتر از سقف ۲۰/دقیقه‌ی مستندشده است. عدد زیر یه
    # نقطه‌ی میانی معقوله؛ RetryAfter هندلر پایین safety net واقعیه.
    SEND_DELAY_SECONDS = 1.5

    # سقف کلی هم‌زمانی بین همه‌ی چت‌ها با هم (طبق حد ~30 پیام/ثانیه‌ی سراسری
    # تلگرام برای broadcast به چت‌های مختلف). با تعداد کم چت (۲-۳ تا) عملاً
    # هیچ‌وقت به این سقف نمی‌رسیم؛ فقط برای محافظت در صورت اضافه‌شدن چت‌های بیشتر.
    GLOBAL_CONCURRENCY = 10

    def __init__(self, channel_name: str = "@tehran_stock_alerts"):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.channel_name = channel_name
        self.bot = Bot(token=self.bot_token)

        # هر چت قفل مستقل خودش رو داره - یعنی ارسال به کانال اصلی و کانال
        # واچ‌لیست کاملاً موازی پیش می‌رن (چون محدودیت تلگرام per-chat هست، نه
        # global بین کل بات)، ولی ارسال‌های متوالی به همون یک چت سریالایز می‌شن.
        self._chat_locks: Dict[str, asyncio.Lock] = {}
        self._chat_last_sent: Dict[str, float] = {}
        self._global_semaphore = asyncio.Semaphore(self.GLOBAL_CONCURRENCY)

    def _get_chat_lock(self, chat_id: str) -> asyncio.Lock:
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    def _current_tehran_jdatetime(self):
        tehran_tz = pytz.timezone("Asia/Tehran")
        now = jdatetime.datetime.now(tehran_tz)
        return now.strftime("%Y/%m/%d"), now.strftime("%H:%M")

    async def send_message(
        self, message: str, parse_mode: str = "HTML", chat_id: str = None
    ) -> bool:
        target_chat_id = chat_id or self.chat_id
        chat_lock = self._get_chat_lock(target_chat_id)

        # قفل per-chat: تضمین می‌کنه دو ارسال هم‌زمان به همون چت مشخص پیش
        # نره (که باعث flood control می‌شه)، بدون اینکه ارسال به یک چت دیگه
        # رو بلاک کنه.
        async with chat_lock:
            last_sent = self._chat_last_sent.get(target_chat_id)
            if last_sent is not None:
                elapsed = time.monotonic() - last_sent
                remaining = self.SEND_DELAY_SECONDS - elapsed
                if remaining > 0:
                    await asyncio.sleep(remaining)

            async def _attempt() -> bool:
                async with self._global_semaphore:
                    await self.bot.send_message(
                        chat_id=target_chat_id, text=message, parse_mode=parse_mode
                    )
                self._chat_last_sent[target_chat_id] = time.monotonic()
                return True

            try:
                return await _attempt()
            except RetryAfter as e:
                logger.warning(
                    f"⚠️ Flood control (چت {target_chat_id}): انتظار {e.retry_after} ثانیه"
                )
                await asyncio.sleep(e.retry_after)
            except TimedOut:
                logger.warning("⚠️ Timeout - تلاش مجدد")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ خطا در ارسال پیام: {e}")
                return False

            try:
                return await _attempt()
            except Exception as retry_error:
                logger.error(f"❌ خطا در تلاش مجدد: {retry_error}")
                return False

    # ------------------------------------------------------------
    # موتور رندر مشترک
    # ------------------------------------------------------------
    def _render(
        self, df: pd.DataFrame, filter_name: str, channel_label: str = None
    ) -> str:
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
        message += f"📅 {date_str} | 🕐 {time_str}\n📢 {channel_label or self.channel_name}"
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

    def format_filter_4_range_mosbat(self, df):
        return self._render(df, "filter_4_range_mosbat")

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

    async def send_filter_alert(
        self,
        df: pd.DataFrame,
        filter_name: str,
        chat_id: str = None,
        channel_label: str = None,
    ) -> bool:
        """
        ارسال پیام یک chunk - نسخه async

        chat_id: اگر داده بشه، پیام به این چت (به‌جای کانال پیش‌فرض) می‌ره
                 (مثلاً برای کپی/روتینگ به کانال واچ‌لیست شخصی).
        channel_label: برچسب نمایشی در فوتر پیام (📢 ...)؛ اگر ندی همون
                       self.channel_name نمایش داده می‌شه که ممکنه با چت
                       واقعی مقصد (وقتی chat_id override شده) یکی نباشه.
        """
        if df.empty:
            return False
        try:
            message = self._render(df, filter_name, channel_label=channel_label)
        except Exception as e:
            logger.error(f"❌ خطا در فرمت پیام فیلتر {filter_name}: {e}")
            return False

        if not message.strip():
            return False
        return await self.send_message(message, chat_id=chat_id)
