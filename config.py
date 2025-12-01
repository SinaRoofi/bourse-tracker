# config.py
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
API_BASE_URL = os.getenv('API_BASE_URL')  

# کدهای صنایع مختلف بورس
INDUSTRY_CODES = ['43', '42', '01', '23', '44', '27', '40', '34', '57', '53']

# نام صنایع (برای نمایش در گزارش‌ها)
INDUSTRY_NAMES = {
    '43': 'فلزات اساسی',
    '42': 'استخراج کانه‌های فلزی',
    '01': 'کشاورزی و خدمات وابسته',
    '23': 'فرآورده‌های نفتی',
    '44': 'محصولات فلزی',
    '27': 'کاشی و سرامیک',
    '40': 'ماشین‌آلات و تجهیزات',
    '34': 'سیمان، آهک و گچ',
    '57': 'خودرو و ساخت قطعات',
    '53': 'فعالیت‌های کامپیوتری'
}

# ========================================
# زمان‌بندی
# ========================================
MARKET_START_TIME = "09:00"  # شروع بازار
MARKET_END_TIME = "12:30"    # پایان بازار
SUMMARY_TIME = "13:00"        # زمان ارسال خلاصه روزانه
CHECK_INTERVAL_MINUTES = 5    # بررسی هر 5 دقیقه

# روزهای کاری (0=شنبه تا 6=جمعه)
WORKING_DAYS = [0, 1, 2, 3, 4]  # شنبه تا چهارشنبه

# ========================================
# تعطیلات رسمی سال 1404 (شمسی)
# ========================================
HOLIDAYS_1404 = [
    # نوروز
    '1404-01-01', '1404-01-02', '1404-01-03', '1404-01-04',
    
    # روز جمهوری اسلامی
    '1404-01-12',
    
    # سیزده به در
    '1404-01-13',
    
    # رحلت حضرت امام خمینی
    '1404-03-14',
    
    # قیام 15 خرداد
    '1404-03-15',
    
    # شهادت امام علی
    '1404-04-11',
    
    # عید فطر
    '1404-04-19', '1404-04-20',
    
    # شهادت امام صادق
    '1404-05-15',
    
    # عید قربان
    '1404-06-26',
    
    # عید غدیر
    '1404-07-04',
    
    # تاسوعا
    '1404-07-18',
    
    # عاشورا
    '1404-07-19',
    
    # اربعین
    '1404-08-28',
    
    # رحلت پیامبر و شهادت امام حسن
    '1404-09-08',
    
    # شهادت امام رضا
    '1404-09-10',
    
    # ولادت پیامبر
    '1404-09-17',
    
    # شهادت فاطمه زهرا
    '1404-11-20',
    
    # ولادت امام علی
    '1404-12-03',
    
    # مبعث
    '1404-12-15',
    
    # ولادت امام زمان
    '1404-12-25'
]

# ========================================
# ستون‌های دیتافریم
# ========================================
CSV_COLUMNS = [
    "id",
    "symbol",
    "volume",
    "value",
    "first_price",
    "first_price_change_percent",
    "high_price",
    "high_price_change_percent",
    "low_price",
    "low_price_change_percent",
    "last_price",
    "last_price_change_percent",
    "final_price",
    "final_price_change_percent",
    "diff_last_final",
    "volatility",
    "sarane_kharid",
    "sarane_forosh",
    "godrat_kharid",
    "pol_hagigi",
    "buy_order_value",
    "sell_order_value",
    "diff_buy_sell_order",
    "avg_5_day_pol_hagigi",
    "avg_20_day_pol_hagigi",
    "avg_60_day_pol_hagigi",
    "5_day_pol_hagigi",
    "20_day_pol_hagigi",
    "60_day_pol_hagigi",
    "5_day_godrat_kharid",
    "20_day_godrat_kharid",
    "avg_monthly_value",
    "value_to_avg_monthly_value",
    "avg_3_month_value",
    "value_to_avg_3_month_value",
    "5_day_return",
    "20_day_return",
    "60_day_return",
    "marketcap",
    "value_to_marketcap"
]

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
    
    if not API_BASE_URL:
        errors.append("API_BASE_URL تنظیم نشده است")
    
    if errors:
        raise ValueError("خطاهای تنظیمات:\n" + "\n".join(errors))
    
    return True
