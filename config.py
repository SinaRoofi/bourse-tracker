"""
تنظیمات پروژه Bourse Tracker
"""

import os
from dotenv import load_dotenv

# بارگذاری متغیرهای محیطی از فایل .env
load_dotenv()

# ========================================
# تنظیمات تلگرام
# ========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
ERROR_CHAT_ID = os.getenv("ERROR_CHAT_ID", "")  # اختیاری: کانال جدا برای خطاهای فنی (فیلتر خراب)
WATCHLIST_CHAT_ID = os.getenv("WATCHLIST_CHAT_ID", "")  # اختیاری: کانال دوم برای واچ‌لیست شخصی

# ========================================
# تنظیمات API
# ========================================
API_BASE_URL = os.getenv("API_BASE_URL")

# ========================================
# GitHub Gist
# ========================================
GIST_TOKEN = os.getenv("GIST_TOKEN")
GIST_ID = os.getenv("GIST_ID")
# اسنپ‌شات صف‌ها (فیلتر ۱۷/۱۸ و خلاصه‌ی بازار) می‌تونه تو یه Gist جدا و با توکن یه
# اکانت دیگه نگه‌داری بشه تا محدودیت نوشتن GitHub دیداپ هشدارها رو خراب نکنه.
# اختیاریه: اگه این دو تنظیم نشن، از همون GIST_ID/GIST_TOKEN بالا استفاده می‌شه.
QUEUE_GIST_TOKEN = os.getenv("QUEUE_GIST_TOKEN")
QUEUE_GIST_ID = os.getenv("QUEUE_GIST_ID")

# ========================================
# کدهای صنایع مختلف بورس
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
# تنظیمات صندوق‌ها
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
# تنظیمات فیلترها
# ========================================

# فیلتر 1: قدرت خرید قوی
STRONG_BUYING_CONFIG = {
    "min_value_to_avg_monthly": 1,
    "min_sarane_kharid": 100,  # میلیون تومان
    "min_godrat_kharid": 2,
    "godrat_greater_than_5day": True,  # قدرت خرید > میانگین 5 روز
    "godrat_5day_multiplier": 2,  # ضریب مقایسه با میانگین 5 روزه (فقط وقتی بالا True باشه اثر داره)
}

# فیلتر 2: کراس سرانه خرید
SARANE_CROSS_CONFIG = {
    "sarane_kharid_greater_than_forosh": True,
    "min_value_to_avg_monthly": 0.5,
    "min_sarane_kharid": 100,
    "max_buy_queue_value": 2,  # میلیارد تومان - ارزش سفارش ردیف اول صف خرید باید کمتر از این باشه
}

# فیلتر 3: واچ‌لیست شخصی — عبور از یک آستانه‌ی واحد
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
]
PERSONAL_WATCHLIST_THRESHOLD = 2.5  # درصد تغییر قیمت
PERSONAL_WATCHLIST_SKIP_IF_BUY_QUEUE = True

# فیلتر 16: کراس قیمت (واچ‌لیست قیمتی)
# برای هر نماد دو قیمت مارک تعریف می‌کنی:
#   "upper": شکست رو به بالا -> هشدار وقتی آخرین قیمت >= این عدد بشه
#   "lower": شکست رو به پایین -> هشدار وقتی آخرین قیمت <= این عدد بشه
# هر دو اختیاری‌ان؛ اگه فقط یکی رو بنویسی (یا اون یکی None/خالی باشه) خطا
# نمی‌ده و فقط همون سمت چک می‌شه. واحد قیمت همون عددیه که تو پیام کنار
# «قیمت آخرین» می‌بینی. هشدار به کانال واچ‌لیست (WATCHLIST_CHAT_ID) می‌ره.
PRICE_CROSS_WATCHLIST = {
    # "شپنا": {"upper": 5000, "lower": 4500},
    # "فملی": {"upper": 7000},      # فقط شکست رو به بالا
    # "کویر": {"lower": 3200},      # فقط شکست رو به پایین
}

# فیلتر 4: رنج مثبت
range_mosbat = {
    "tick_diff_percent": 2.0,
    "min_value_to_avg_monthly": 0.5,
}

# فیلتر 5: ورود پول حقیقی قوی
POL_HAGIGI_FILTER_CONFIG = {
    "min_pol_to_value_ratio": 0.5,
    "min_sarane_kharid": 100,
    "min_godrat_kharid": 1.5,
}

# فیلتر 6: تیک و ساعت
TICK_FILTER_CONFIG = {
    "first_to_low_ratio": 0.99,
    "last_to_first_ratio": 0.99,
    "tick_diff_percent": 1.0,
}

# فیلتر 7: حجم مشکوک
SUSPICIOUS_VOLUME_CONFIG = {
    "min_value_to_avg_ratio": 2.0,
}

# فیلتر 15: حجم مشکوک هفتگی (میانگین ارزش معاملات ۵ روز اخیر نسبت به
# میانگین ماهانه - برخلاف فیلتر ۷ که فقط یک روزِ جهنده رو می‌گیره، این
# فیلتر یک هفته‌ی کامل با حجم پیوسته بالا رو نشون می‌ده)
SUSPICIOUS_VOLUME_WEEKLY_CONFIG = {
    "min_value_5_to_20_ratio": 3.0,
}

# فیلتر 8: نوسان‌ گیری
SWING_TRADE_CONFIG = {
    "min_allowed_price": -2.8,
    "max_last_change_percent": -1.0,
    "min_godrat_kharid": 2.0,
    "min_sarane_kharid": 100, 
    "min_value_to_avg_monthly": 1.0,
}

# فیلتر 9: نیم ساعت اول (۹:۰۰ تا ۹:۳۰)
FIRST_HOUR_CONFIG = {
    "min_value_to_avg_ratio": 1.0,
    "start_hour": 9,
    "start_minute": 0,
    "end_hour": 9,
    "end_minute": 30,
}

# فیلتر 10: صف خرید با اردر سنگین (بالای ۵ میلیارد)
HEAVY_BUY_QUEUE_CONFIG = {
    "min_buy_order": 100,  # میلیون تومان
    "min_buy_queue_value": 5,  # میلیارد تومان
    "price_at_ceiling": True,  # آخرین قیمت = سقف
}

# فیلتر 14: صف خرید تازه (قفل واقعی: عرضه صفر) + بازه‌ی ارزش ۰.۱ تا ۵ میلیارد
# شرط اصلی تشخیص "صف" دیگه ask_volume == 0 (عرضه خالی) هست، نه فقط
# قیمت=سقف. بازه‌ی ارزش صف خرید یک فیلتر ثانویه‌ست: حداقل صرفاً برای حذف
# نویز، حداکثر برای کنار گذاشتن صف‌های بزرگ - که از این آستانه به بالا رو
# فیلتر ۱۰ پوشش می‌ده (دو آستانه هم‌تراز شدن تا شکافی بین دو فیلتر نمونه).
# دیداپ روزانه‌ی موجود (هر نماد فقط یک‌بار در روز هشدار می‌گیره) به‌همراه
# `_consume_heavy_locked_dedup` تو main.py باعث می‌شه دقیقاً لحظه‌ی اول
# قفل‌شدنِ سبک صف در روز گرفته بشه - حتی اگه صف سنگین بعداً موقتاً (با
# کنسل‌شدن سفارش‌ها) به زیر آستانه برگرده.
BUY_QUEUE_SIMPLE_CONFIG = {
    "min_buy_queue_value": 0.1,  # میلیارد تومان (۱۰۰ میلیون) - فقط حذف نویز
    "max_buy_queue_value": 5,    # میلیارد تومان - بالاتر از این، فیلتر ۱۰ پوشش می‌ده
}

# فیلتر 11: خرید حقوقی و حقیقی قوی
HOGHOOGHI_HAGHIGHI_STRONG_BUY_CONFIG = {
    "max_pol_hagigi_to_value": -0.3,  
    "min_last_price_change_percent": 0, 
    "min_sarane_kharid": 100,  
    "sarane_kharid_greater_than_forosh": True, 
} 

# فیلتر 12: ماروبوزو صعودی با حجم
BULLISH_MARUBOZU_CONFIG = {
    "min_body_to_range_ratio": 0.9,   # نسبت بدنه به کل رنج روز - هرچی به 1 نزدیک‌تر یعنی سایه کمتر
    "min_intraday_move_percent": 1.0,  # حداقل اندازه‌ی بدنه‌ی کندل (نسبت به قیمت باز شدن امروز، نه دیروز)
    "min_value_to_avg_monthly": 1.0,  
}

# فیلتر 13: اختلاف سرانه بالا (سرانه خرید - سرانه فروش)
SARANE_DIFF_CONFIG = {
    "min_sarane_diff": 100,  # میلیون تومان
    "max_buy_queue_value": 2,  # میلیارد تومان - اگه صف خرید بالای این باشه، حذف می‌شه
}

# فیلتر 17 و 18: ردیاب صف‌های سنگین (نیازمند حافظه‌ی بین اجراها)
# اسنپ‌شات ارزش صف هر نماد تو فایل جدای queue_snapshot.json تو همون Gist نگه‌داری
# می‌شه (نه تو alert_cache.json) و هر روز ریست می‌شه.
# منطق: نمادی که صف سنگینش (min_queue_value) دیده شده، وقتی صفش «کامل» از بین
# بره (قیمت از سقف/کف خارج بشه) یک‌بار هشدار می‌گیره. کم شدن تدریجی صف هشدار
# نمی‌ده. هر نماد در روز برای هر فیلتر حداکثر یک هشدار می‌گیره (دیداپ روزانه).
QUEUE_TRACKER_CONFIG = {
    "enabled": True,
    # اسنپ‌شات فقط وقتی تو Gist نوشته می‌شه که وضعیت صف‌ها عوض شده باشه، یا از آخرین
    # ذخیره این‌قدر دقیقه گذشته باشه (تعداد نوشتن‌ها رو کم می‌کنه؛ GitHub برای نوشتن
    # زیاد و پشت‌سرهم تو Gist 403 می‌ده).
    "snapshot_refresh_minutes": 5,
    # تو این چند دقیقه‌ی آخر بازار (تا MARKET_END_TIME) اسنپ‌شات هر اجرا ذخیره می‌شه تا
    # خلاصه‌ی بازار آخرین ارزش صف‌ها رو داشته باشه. 0 = غیرفعال.
    "snapshot_final_window_minutes": 10,
    # فیلتر 17: عرضه صف خرید (صف خرید سنگینی که کامل عرضه شد)
    "buy_supply": {
        "enabled": True,
        "min_queue_value": 5,   # میلیارد تومان - فقط صف‌های با این ارزش یا بیشتر
        "min_buy_order": 100,   # میلیون تومان - سرانه‌ی هر اردر صف (مثل فیلتر 10)؛ 0 = بدون شرط
        # فقط اگه با وجود عرضه، قدرت خرید (godrat_kharid) هنوز بیشتر از این عدد باشه
        # هشدار بده. None = بدون شرط قدرت خرید.
        "min_godrat_kharid": 1,
        # و سرانه‌ی خرید حقیقی (میلیون تومان) هم باید بیشتر از این عدد باشه. None = بدون شرط.
        "min_sarane_kharid": 100,
    },
    # فیلتر 18: جمع‌آوری صف فروش (صف فروش سنگین روی کف که کامل جمع شد)
    "sell_collect": {
        "enabled": True,
        "min_queue_value": 5,   # میلیارد تومان
        # اگه قیمت کف از API نیومد: صف فروش وقتی قفل حساب می‌شه که بهترین قیمت
        # فروش برابر آخرین قیمت باشه و درصد تغییر آخرین قیمت از این عدد کمتر
        # یا مساوی باشه
        "fallback_max_change_percent": -3.0,
    },
}

# ========================================
# زمان‌بندی
# ========================================
MARKET_START_TIME = "09:00"
MARKET_END_TIME = "12:30"

# روزهای کاری (0=شنبه تا 6=جمعه)
WORKING_DAYS = [0, 1, 2, 3, 4]

# ========================================
# تنظیمات لاگ
# ========================================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


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
