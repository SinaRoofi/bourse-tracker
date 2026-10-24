"""
تاریخ/ساعت وقت تهران، مستقل از timezone ماشین (GitHub Actions روی UTC اجرا می‌شه).
تو بازه‌ی اجرای فعلی (۰۵:۳۰ تا ۰۹:۰۱ UTC) تاریخ UTC و تهران یکی‌ه، ولی این helper تضمین
می‌کنه اجرای دستی/محلی یا تغییر ساعت سرور هم تاریخ رو اشتباه نکنه.
"""

import jdatetime
import pytz

TEHRAN_TZ = pytz.timezone("Asia/Tehran")


def tehran_today() -> jdatetime.date:
    """تاریخ امروز (شمسی) به وقت تهران"""
    now = jdatetime.datetime.now(TEHRAN_TZ)
    return jdatetime.date(now.year, now.month, now.day)


def tehran_today_str() -> str:
    """امروز به‌صورت YYYY-MM-DD (شمسی)"""
    return tehran_today().strftime("%Y-%m-%d")
