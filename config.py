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
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# ========================================
# تنظیمات API
# ========================================
API_BASE_URL = os.getenv('API_BASE_URL')  # API اول (https://tradersarena.ir)
BRSAPI_KEY = os.getenv('BRSAPI_KEY')      # کلید BrsApi

# ========================================
# GitHub Gist
# ========================================
GIST_TOKEN = os.getenv('GIST_TOKEN')
GIST_ID = os.getenv('GIST_ID')

# ========================================
# کدهای صنایع مختلف بورس
# ========================================
INDUSTRY_CODES = [
    '01', '10', '11', '13', '14', '17', '19', '20', '21', '22',
    '23', '25', '27', '28', '29', '31', '32', '34', '38', '39',
    '40', '42', '43', '44', '47', '49', '53', '54', '55', '56',
    '57', '58', '60', '64', '66', '67', '70', '72', '73', '74'
]

# نام صنایع (برای هشتگ)
INDUSTRY_NAMES = {
    '01': 'زراعت',
    '10': 'ذغال_سنگ',
    '11': 'استخراج_نفت',
    '13': 'کانه_فلزی',
    '14': 'سایر_معادن',
    '17': 'منسوجات',
    '19': 'محصولات_چرمی',
    '20': 'محصولات_چوبی',
    '21': 'محصولات_کاغذ',
    '22': 'انتشار_و_چاپ',
    '23': 'فرآورده_نفتی',
    '25': 'لاستیک',
    '27': 'فلزات_اساسی',
    '28': 'محصولات_فلزی',
    '29': 'ماشین_آلات',
    '31': 'دستگاه_های_برقی',
    '32': 'وسایل_ارتباطی',
    '34': 'خودرو',
    '38': 'قند_و_شکر',
    '39': 'چند_رشته_ای_صنعتی',
    '40': 'تامین_آب_برق_گاز',
    '42': 'غذایی_بجز_قند',
    '43': 'مواد_دارویی',
    '44': 'شیمیایی',
    '47': 'خرده_فروشی',
    '49': 'کاشی_سرامیک',
    '53': 'سیمان',
    '54': 'کانی_غیرفلزی',
    '55': 'هتل_رستوران',
    '56': 'سرمایه_گذاری',
    '57': 'بانک',
    '58': 'سایر_مالی',
    '60': 'حمل_نقل',
    '64': 'رادیویی',
    '66': 'بیمه_بازنشسته',
    '67': 'اداره_بازارهای_مالی',
    '70': 'انبوه_سازی',
    '72': 'رایانه',
    '73': 'اطلاعات_ارتباطات',
    '74': 'فنی_مهندسی'
}

# ========================================
# تنظیمات فیلترها
# ========================================

# فیلتر 1: قدرت خرید قوی
STRONG_BUYING_CONFIG = {
    'min_value_to_avg_monthly': 2.0,
    'min_sarane_kharid': 50,  # میلیون تومان
    'min_godrat_kharid': 1,
    'godrat_greater_than_5day': True,  # قدرت خرید > میانگین 5 روز
}

# فیلتر 2: کراس سرانه خرید
SARANE_CROSS_CONFIG = {
    'sarane_kharid_greater_than_forosh': True,
    'min_value_to_avg_monthly': 0.5,  # شرط جدید
}

# فیلتر 3: نمادهای خاص
WATCHLIST_SYMBOLS = {
    'فولاد': 2.99,
    'خودرو': 2.99,
    'شپنا': 2.99,
    'فملی': 2.99,
}

# فیلتر 4: رزرو شده
FILTER_4_CONFIG = {}

# فیلتر 5: نسبت پول حقیقی
POL_HAGIGI_FILTER_CONFIG = {
    'min_pol_to_value_ratio': 0.5,
    'min_sarane_kharid': 50,  # میلیون تومان
    'min_godrat_kharid': 1.5,
}

# فیلتر 6: تیک و ساعت
TICK_FILTER_CONFIG = {
    'first_to_low_ratio': 0.98,
    'last_to_first_ratio': 0.98,
    'tick_diff_percent': 2.0,
}

# فیلتر 7: حجم مشکوک
SUSPICIOUS_VOLUME_CONFIG = {
    'min_value_to_avg_ratio': 2.0,
}

# فیلتر 8: نوسان‌گیری
SWING_TRADE_CONFIG = {
    'min_allowed_price': -3.0,
    'max_last_change_percent': -2.0,
    'min_godrat_kharid': 2.0,
    'min_sarane_kharid': 50,  # میلیون تومان
    'min_value_to_avg_monthly': 1.0,
}

# فیلتر 9: یک ساعت اول
FIRST_HOUR_CONFIG = {
    'min_value_to_avg_ratio': 1.0,
    'start_hour': 9,
    'end_hour': 10,
}

# فیلتر 10: صف خرید میلیاردی
HEAVY_BUY_QUEUE_CONFIG = {
    'min_buy_order': 70,  # میلیون تومان
    'min_buy_queue_value': 10.0,  # میلیارد تومان - شرط جدید
    'price_at_ceiling': True,  # آخرین قیمت = سقف
}

# ========================================
# زمان‌بندی
# ========================================
MARKET_START_TIME = "08:00"
MARKET_END_TIME = "12:30"
CHECK_INTERVAL_MINUTES = 5

# روزهای کاری (0=شنبه تا 6=جمعه)
WORKING_DAYS = [0, 1, 2, 3, 4, 5, 6]

# ========================================
# تنظیمات لاگ
# ========================================
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

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

    if not API_BASE_URL and not BRSAPI_KEY:
        errors.append("حداقل یکی از API_BASE_URL یا BRSAPI_KEY باید تنظیم شود")

    if errors:
        raise ValueError("خطاهای تنظیمات:\n" + "\n".join(errors))

    return True
