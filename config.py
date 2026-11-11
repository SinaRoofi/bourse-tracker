"""
تنظیمات پروژه Bourse Tracker

ترتیب فایل: تلگرام ← API ← Gist ← زمان‌بندی و لاگ ← آستانه‌های مشترک ←
صنایع و صندوق‌ها ← فیلترها (به ترتیب شماره) ← خلاصه‌ی بازار ← اعتبارسنجی.
توضیح هر فیلتر بالای خودش آمده؛ داخل دیکشنری‌ها کامنت نیست.
"""

import os
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی از فایل .env
load_dotenv()

# ========================================
# تلگرام
# ========================================
# ERROR_CHAT_ID اختیاری: کانال جدا برای خطاهای فنی (فیلتر خراب)
# WATCHLIST_CHAT_ID اختیاری: کانال دوم برای واچ‌لیست شخصی
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ERROR_CHAT_ID = os.getenv("ERROR_CHAT_ID", "")
WATCHLIST_CHAT_ID = os.getenv("WATCHLIST_CHAT_ID", "")

# ========================================
# API
# ========================================
API_BASE_URL = os.getenv("API_BASE_URL")

# ========================================
# GitHub Gist
# ========================================
# Gist اصلی: دیداپ هشدارها.
GIST_TOKEN = os.getenv("GIST_TOKEN")
GIST_ID = os.getenv("GIST_ID")

# Gist صف‌ها (می‌تواند با توکن اکانت دیگر باشد تا محدودیت نوشتن GitHub دیداپ را خراب نکند):
# اسنپ‌شات صف فیلتر ۱۷/۱۸ و خلاصه‌ی بازار، و تاریخچه‌ی سرانه فیلتر ۱۹.
# فالبک به Gist اصلی نیست؛ اگر این دو تنظیم نشوند، ردیاب صف و ارزش صف در خلاصه‌ی بازار غیرفعال می‌شوند.
QUEUE_GIST_TOKEN = os.getenv("QUEUE_GIST_TOKEN")
QUEUE_GIST_ID = os.getenv("QUEUE_GIST_ID")

# ========================================
# زمان‌بندی و لاگ
# ========================================
MARKET_START_TIME = "09:00"
MARKET_END_TIME = "12:30"

# روزهای کاری (0=شنبه تا 6=جمعه)
WORKING_DAYS = [0, 1, 2, 3, 4]

LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# ========================================
# آستانه‌ی مشترک
# ========================================
# حداقل سرانه خرید (میلیون تومان). با یک عدد، روی همه‌ی فیلترهایی که شرط سرانه خرید
# دارند اعمال می‌شود: ۱، ۲، ۵، ۸، ۱۱، ۱۷ و ۱۹.
MIN_SARANE_KHARID = 150

# واحدها در فیلترها: سرانه، اختلاف سرانه و اردر = میلیون تومان | ارزش صف = میلیارد تومان.

# ========================================
# کدها و نام صنایع
# ========================================
INDUSTRY_CODES = [
    "01",
    "10",
    "11",
    "13",
    "14",
    "17",
    "19",
    "20",
    "21",
    "22",
    "23",
    "25",
    "27",
    "28",
    "29",
    "31",
    "32",
    "34",
    "38",
    "39",
    "40",
    "42",
    "43",
    "44",
    "47",
    "49",
    "53",
    "54",
    "55",
    "56",
    "57",
    "58",
    "60",
    "64",
    "66",
    "67",
    "70",
    "72",
    "73",
    "74",
]

# نام صنایع (برای هشتگ)
INDUSTRY_NAMES = {
    "01": "زراعت",
    "10": "ذغال_سنگ",
    "11": "استخراج_نفت",
    "13": "کانه_فلزی",
    "14": "سایر_معادن",
    "17": "منسوجات",
    "19": "محصولات_چرمی",
    "20": "محصولات_چوبی",
    "21": "محصولات_کاغذ",
    "22": "انتشار_و_چاپ",
    "23": "فرآورده_نفتی",
    "25": "لاستیک",
    "27": "فلزات_اساسی",
    "28": "محصولات_فلزی",
    "29": "ماشین_آلات",
    "31": "دستگاه_های_برقی",
    "32": "وسایل_ارتباطی",
    "34": "خودرو",
    "38": "قند_و_شکر",
    "39": "چند_رشته_ای_صنعتی",
    "40": "تامین_آب_برق_گاز",
    "42": "غذایی_بجز_قند",
    "43": "مواد_دارویی",
    "44": "شیمیایی",
    "47": "خرده_فروشی",
    "49": "کاشی_سرامیک",
    "53": "سیمان",
    "54": "کانی_غیرفلزی",
    "55": "هتل_رستوران",
    "56": "سرمایه_گذاری",
    "57": "بانک",
    "58": "سایر_مالی",
    "60": "حمل_نقل",
    "64": "رادیویی",
    "66": "بیمه_بازنشسته",
    "67": "اداره_بازارهای_مالی",
    "70": "انبوه_سازی",
    "72": "رایانه",
    "73": "اطلاعات_ارتباطات",
    "74": "فنی_مهندسی",
}

# ========================================
# صندوق‌ها
# ========================================
FUND_TYPES = {
    "index": {
        "slug": "index-funds",
        "name": "صندوق‌های شاخصی",
        "enabled": True,
    },
    "real_state": {
        "slug": "real-state-funds",
        "name": "صندوق‌های املاک",
        "enabled": True,
    },
    "fund_in_fund": {
        "slug": "fund-in-funds",
        "name": "صندوق‌های فراصندوق",
        "enabled": True,
    },
    "classic_stock": {
        "slug": "classic-stock-funds",
        "name": "صندوق‌های سهامی کلاسیک",
        "enabled": True,
    },
    "mixed": {
        "slug": "mixed-funds",
        "name": "صندوق‌های مختلط",
        "enabled": True,
    },
    "energy": {
        "slug": "energy-funds",
        "name": "صندوق‌های انرژی",
        "enabled": True,
    },
    "leveraged": {
        "slug": "leveraged-funds",
        "name": "صندوق‌های اهرمی",
        "enabled": True,
    },
    "sector": {
        "slug": "sector-funds",
        "name": "صندوق‌های بخشی",
        "enabled": True,
    },
}

# ========================================
# فیلترها (به ترتیب شماره)
# ========================================

# فیلتر ۱ - قدرت خرید قوی: ارزش نسبی + سرانه + قدرت خرید؛ قدرت خرید باید از
# (godrat_5day_multiplier × میانگین ۵ روزه) هم بیشتر باشد (فقط وقتی godrat_greater_than_5day=True).
STRONG_BUYING_CONFIG = {
    "min_value_to_avg_monthly": 1,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "min_godrat_kharid": 2,
    "godrat_greater_than_5day": True,
    "godrat_5day_multiplier": 2,
}

# فیلتر ۲ - کراس سرانه خرید: سرانه خرید > سرانه فروش؛ ارزش ردیف اول صف خرید باید کمتر از max_buy_queue_value باشد.
SARANE_CROSS_CONFIG = {
    "sarane_kharid_greater_than_forosh": True,
    "min_value_to_avg_monthly": 0.5,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "max_buy_queue_value": 2,
}

# فیلتر ۳ - واچ‌لیست شخصی: هشدار وقتی درصد تغییر قیمت از آستانه رد شد (نمادِ در صف خرید این دور رد می‌شود).
PERSONAL_WATCHLIST = [
    "شپنا",
    "فملی",
    "رافزا",
    "دحاوی",
    "فزر",
    "حتاید",
    "هجرت",
    "دزاگرس",
    "غمایه",
    "فایرا",
    "شاروم",
    "کویر",
    "کمنگنز",
    "فنورد",
    "مسگون",
]
PERSONAL_WATCHLIST_THRESHOLD = 2.5
PERSONAL_WATCHLIST_SKIP_IF_BUY_QUEUE = True

# فیلتر ۴ - رنج مثبت.
range_mosbat = {
    "tick_diff_percent": 2.0,
    "min_value_to_avg_monthly": 0.5,
}

# فیلتر ۵ - ورود پول حقیقی قوی.
POL_HAGIGI_FILTER_CONFIG = {
    "min_pol_to_value_ratio": 0.5,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "min_godrat_kharid": 1.5,
}

# فیلتر ۶ - تیک و ساعت.
TICK_FILTER_CONFIG = {
    "first_to_low_ratio": 0.99,
    "last_to_first_ratio": 0.99,
    "tick_diff_percent": 1.0,
}

# فیلتر ۷ - حجم مشکوک (یک روزِ جهنده): ارزش معاملات به میانگین ماهانه.
SUSPICIOUS_VOLUME_CONFIG = {
    "min_value_to_avg_ratio": 2.0,
}

# فیلتر ۸ - نوسان‌گیری.
SWING_TRADE_CONFIG = {
    "min_allowed_price": -2.8,
    "max_last_change_percent": -1.0,
    "min_godrat_kharid": 2.0,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "min_value_to_avg_monthly": 1.0,
}

# فیلتر ۹ - نیم ساعت اول (۹:۰۰ تا ۹:۳۰).
FIRST_HOUR_CONFIG = {
    "min_value_to_avg_ratio": 1.0,
    "start_hour": 9,
    "start_minute": 0,
    "end_hour": 9,
    "end_minute": 30,
}

# فیلتر ۱۰ - صف خرید با اردر سنگین: قیمت = سقف، اردر و ارزش صف بالای آستانه.
HEAVY_BUY_QUEUE_CONFIG = {
    "min_buy_order": 100,
    "min_buy_queue_value": 5,
    "price_at_ceiling": True,
}

# فیلتر ۱۱ - خرید حقوقی و حقیقی قوی: خروج پول حقیقی (نسبت ≤ آستانه) با قیمت مثبت و سرانه خرید > سرانه فروش.
HOGHOOGHI_HAGHIGHI_STRONG_BUY_CONFIG = {
    "max_pol_hagigi_to_value": -0.3,
    "min_last_price_change_percent": 0,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "sarane_kharid_greater_than_forosh": True,
}

# فیلتر ۱۲ - ماروبوزو صعودی با حجم: بدنه‌ی کندل نسبت به رنج روز (نزدیک ۱ = سایه‌ی کمتر)
# و نسبت به قیمت بازشدن امروز (نه دیروز).
BULLISH_MARUBOZU_CONFIG = {
    "min_body_to_range_ratio": 0.9,
    "min_intraday_move_percent": 1.0,
    "min_value_to_avg_monthly": 1.0,
}

# فیلتر ۱۳ - اختلاف سرانه بالا (سرانه خرید − سرانه فروش)؛ اگر صف خرید بالاتر از سقف باشد حذف می‌شود.
SARANE_DIFF_CONFIG = {
    "min_sarane_diff": 100,
    "max_buy_queue_value": 2,
}

# فیلتر ۱۴ - صف خرید تازه: قفل واقعی (عرضه سطح ۱ صفر) با ارزش صف بین min و max.
# بالاتر از max را فیلتر ۱۰ پوشش می‌دهد. دیداپ روزانه + _consume_heavy_locked_dedup (main.py)
# باعث می‌شود اولین لحظه‌ی قفل‌شدنِ سبک صف در روز گرفته شود.
BUY_QUEUE_SIMPLE_CONFIG = {
    "min_buy_queue_value": 0.1,
    "max_buy_queue_value": 5,
}

# فیلتر ۱۵ - حجم مشکوک هفتگی: میانگین ۵ روز اخیر نسبت به میانگین ماهانه (یک هفته‌ی کامل با حجم بالا).
SUSPICIOUS_VOLUME_WEEKLY_CONFIG = {
    "min_value_5_to_20_ratio": 3.0,
}

# فیلتر ۱۶ - کراس قیمت (واچ‌لیست قیمتی؛ هشدار به کانال واچ‌لیست).
# برای هر نماد "upper" (هشدار وقتی آخرین قیمت ≥ عدد) و/یا "lower" (هشدار وقتی ≤ عدد)؛
# هر دو اختیاری‌اند. مثال: {"شپنا": {"upper": 5000, "lower": 4500}, "فملی": {"upper": 7000}}
PRICE_CROSS_WATCHLIST = {}

# فیلتر ۱۷ و ۱۸ - ردیاب صف‌های سنگین (نیاز به حافظه‌ی بین اجراها؛ اسنپ‌شات در queue_snapshot.json، روزانه ریست).
#   ۱۷ (buy_supply): صف خرید سنگینِ دیده‌شده که کامل عرضه شد؛ فقط اگر قدرت خرید و سرانه خرید هنوز بالا باشند (None = بدون شرط).
#   ۱۸ (sell_collect): صف فروش سنگین روی کف که کامل جمع شد؛ اگر قیمت کف از API نیامد، صف وقتی قفل است که
#       بهترین قیمت فروش = آخرین قیمت و درصد تغییر ≤ fallback_max_change_percent.
# هر نماد در روز برای هر فیلتر حداکثر یک هشدار می‌گیرد. کم‌شدن تدریجی صف هشدار نمی‌دهد.
# snapshot_refresh_minutes: حداقل فاصله‌ی نوشتن اسنپ‌شات در Gist (جلوگیری از 403)؛
# snapshot_final_window_minutes: در این چند دقیقه‌ی آخر بازار هر اجرا ذخیره می‌شود (0 = غیرفعال).
QUEUE_TRACKER_CONFIG = {
    "enabled": True,
    "snapshot_refresh_minutes": 5,
    "snapshot_final_window_minutes": 10,
    "buy_supply": {
        "enabled": True,
        "min_queue_value": 5,
        "min_buy_order": 100,
        "min_godrat_kharid": 1,
        "min_sarane_kharid": MIN_SARANE_KHARID,
    },
    "sell_collect": {
        "enabled": True,
        "min_queue_value": 5,
        "fallback_max_change_percent": -3.0,
    },
}

# فیلتر ۱۹ - سرانه خرید سنگین. همه‌ی شرط‌ها با هم: سرانه لحظه‌ای ≥ min_ratio_to_avg × میانگین avg_window_days
# روز قبل (امروز در میانگین نیست)، حجم نسبی ≥ min_value_to_avg_monthly، قدرت خرید > min_godrat_kharid، سرانه > min_sarane_kharid.
# تاریخچه‌ی سرانه‌ی پایانی هر روز یک‌بار (اجرای خلاصه‌ی بازار، بعد از ۱۲:۳۱ تا save_not_after) در Gist صف‌ها
# (sarane_history.json) ذخیره می‌شود و history_days روز آخر نگه‌داری می‌شود.
# max_file_kb: سقف حجم فایل (زیر ۱ مگابایت؛ با رد شدن قدیمی‌ترین روز حذف و در کانال خبر می‌دهد).
# weekly_days / weekly_min_days: میانگین هفتگیِ نمایش‌داده‌شده در هشدارها (کمتر از weekly_min_days روز داده = خط نمی‌آید).
# min_history_days: حداقل روزِ دارای داده‌ی هر نماد برای محاسبه‌ی میانگین (کمتر = شروع زودتر).
# min_symbols_to_save: اگر نمادهای معتبر کمتر بود (خرابی API) ذخیره نمی‌شود.
# min_symbols_ratio_vs_prev: اگر نمادهای دریافتی کمتر از این نسبتِ روز قبل بود، دریافت دوباره تلاش می‌شود
# و اگر باز ناقص بود با هشدار در کانال ذخیره می‌شود.
HEAVY_SARANE_CONFIG = {
    "enabled": True,
    "history_days": 20,
    "max_file_kb": 800,
    "avg_window_days": 10,
    "weekly_days": 5,
    "weekly_min_days": 3,
    "min_history_days": 5,
    "min_ratio_to_avg": 2,
    "min_value_to_avg_monthly": 1,
    "min_godrat_kharid": 1,
    "min_sarane_kharid": MIN_SARANE_KHARID,
    "min_symbols_to_save": 100,
    "min_symbols_ratio_vs_prev": 0.9,
    "save_not_after": "12:45",
}

# ========================================
# خلاصه‌ی بازار
# ========================================
# لیست «سرانه خرید بیشتر از میانگین ماهانه»: صنعتی که سهم میانگین ۲۰ روزه‌ی ارزش معاملاتش از کل
# همان گروه کمتر از این درصد باشد، از رتبه‌بندی این لیست کنار می‌رود (در «نبض بازار» همچنان شمرده می‌شود).
# 0 = خاموش.
SUMMARY_SARANE_MIN_VALUE_SHARE_PCT = 0.3


# ========================================
# اعتبارسنجی تنظیمات
# ========================================
def validate_config():
    """بررسی صحت تنظیمات"""
    errors = []

    if not TELEGRAM_BOT_TOKEN:
        errors.append("TELEGRAM_BOT_TOKEN تنظیم نشده است")

    if not TELEGRAM_CHAT_ID:
        errors.append("TELEGRAM_CHAT_ID تنظیم نشده است")

    if not API_BASE_URL:
        errors.append("API_BASE_URL تنظیم نشده است")

    if not GIST_TOKEN:
        errors.append("GIST_TOKEN تنظیم نشده است")

    if not GIST_ID:
        errors.append("GIST_ID تنظیم نشده است")

    multiplier = STRONG_BUYING_CONFIG.get("godrat_5day_multiplier")
    if not isinstance(multiplier, (int, float)) or isinstance(multiplier, bool) or multiplier <= 0:
        errors.append(
            f"STRONG_BUYING_CONFIG['godrat_5day_multiplier'] باید عدد مثبت باشد "
            f"(مقدار فعلی: {multiplier!r})"
        )

    if errors:
        raise ValueError("خطاهای تنظیمات:\n" + "\n".join(errors))

    return True
