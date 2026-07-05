"""
ماژول مدیریت تعطیلات رسمی ایران

"""
from datetime import datetime
from typing import List, Dict, Set, Optional
from functools import lru_cache
import logging

import requests
import jdatetime

from config import WORKING_DAYS

logger = logging.getLogger(__name__)

API_URL = "https://holidayapi.ir/gregorian"
API_TIMEOUT = 5

# ========================================
# تعطیلات رسمی سال 1405 و 1406 (fallback وقتی API در دسترس نیست)
# ========================================
HOLIDAYS_1405 = [
    "1405-01-01","1405-01-02","1405-01-03","1405-01-04",
    "1405-01-12","1405-01-13","1405-03-14","1405-03-15",
    "1405-03-30","1405-04-03","1405-05-13","1405-05-24",
    "1405-05-31","1405-06-09","1405-08-23","1405-10-02",
    "1405-10-16","1405-11-05","1405-11-19","1405-11-20",
    "1405-12-09",
]

HOLIDAYS_1406: List[str] = ["1406-01-01","1406-01-02","1406-01-03","1406-01-04",
    "1406-01-12","1406-01-13"]

ALL_HOLIDAYS = HOLIDAYS_1405 + HOLIDAYS_1406

MANUAL_EMERGENCY_HOLIDAYS: Set[str] = {
    "1405-04-14", "1405-04-15", "1405-04-16"
}


def _to_gregorian(date_str: str):
    """تبدیل رشته‌ی شمسی 'YYYY-MM-DD' به jdatetime.date میلادی (توگرگوریان)."""
    year, month, day = map(int, date_str.split("-"))
    return jdatetime.date(year, month, day).togregorian()


class HolidayManager:
    """مدیریت تعطیلات بورس"""

    def __init__(self):
        self.holidays_set: Set[str] = set(ALL_HOLIDAYS)
        self.holidays_by_year: Dict[int, Set[str]] = {}
        for h in self.holidays_set:
            year = int(h.split("-")[0])
            self.holidays_by_year.setdefault(year, set()).add(h)

    def is_holiday(self, date_str: str = None) -> bool:
        """
        بررسی تعطیل بودن یک تاریخ شمسی 'YYYY-MM-DD' (پیش‌فرض: امروز).
        فقط تعطیلات رسمی تقویم را چک می‌کند — weekday مسئولیت caller است.

        اولویت: اضطراری دستی -> API زنده -> لیست هاردکد -> fail-safe (True)
        """
        if date_str is None:
            date_str = jdatetime.date.today().strftime("%Y-%m-%d")
        return self._is_holiday_cached(date_str)

    def is_working_day(self, date_str: str = None) -> bool:
        """بررسی اینکه آیا روز کاری است یا نه (فقط از نظر تقویم رسمی)"""
        return not self.is_holiday(date_str)

    @lru_cache(maxsize=128)
    def _is_holiday_cached(self, date_str: str) -> bool:
        # 0) بالاترین اولویت: تعطیلات اضطراری دستی
        if date_str in MANUAL_EMERGENCY_HOLIDAYS:
            logger.info(f"🚨 {date_str} در MANUAL_EMERGENCY_HOLIDAYS است — تعطیل اضطراری")
            return True

        # 1) API زنده
        api_result = self._check_holiday_api(date_str)
        if api_result is not None:
            return api_result

        # 2) fallback: لیست هاردکد
        jalali_year = int(date_str.split("-")[0])
        if jalali_year in self.holidays_by_year:
            return date_str in self.holidays_by_year[jalali_year]

        # 3) هیچ منبعی در دسترس نیست -> fail-safe: فرض کن تعطیله
        logger.error(
            f"🚨 نه API نه لیست هاردکد برای سال {jalali_year} در دسترس بود — "
            f"fail-safe فعال شد، {date_str} به‌عنوان تعطیل در نظر گرفته می‌شود"
        )
        return True

    @staticmethod
    def _check_holiday_api(date_str: str) -> Optional[bool]:
        """
        Returns:
            bool: نتیجه‌ی معتبر از holidayapi.ir
            None: API در دسترس نبود/پاسخ نامعتبر بود (سیگنال fallback)
        """
        try:
            g = _to_gregorian(date_str)
            response = requests.get(
                f"{API_URL}/{g.year}/{g.month:02d}/{g.day:02d}", timeout=API_TIMEOUT
            )
            response.raise_for_status()
            return bool(response.json().get("is_holiday", False))

        except requests.RequestException as e:
            logger.warning(f"⚠️ holidayapi.ir در دسترس نیست ({e}) — fallback به لیست هاردکد")
            return None
        except (ValueError, KeyError) as e:
            logger.warning(f"⚠️ پاسخ نامعتبر از holidayapi.ir ({e}) — fallback به لیست هاردکد")
            return None

    def get_holidays_in_range(self, start_date: str, end_date: str) -> List[str]:
        holidays_in_range = []
        for holiday in sorted(self.holidays_set):
            if start_date <= holiday <= end_date:
                holidays_in_range.append(holiday)
        return holidays_in_range

    def get_next_working_day(self, date_str: str = None) -> str:
        if date_str is None:
            current_date = jdatetime.date.today()
        else:
            year, month, day = map(int, date_str.split("-"))
            current_date = jdatetime.date(year, month, day)

        for _ in range(30):
            current_date += jdatetime.timedelta(days=1)
            date_str = current_date.strftime("%Y-%m-%d")
            if self.is_working_day(date_str):
                return date_str

        raise ValueError("روز کاری در 30 روز آینده پیدا نشد!")

    def count_working_days(self, start_date: str, end_date: str) -> int:
        year1, month1, day1 = map(int, start_date.split("-"))
        year2, month2, day2 = map(int, end_date.split("-"))

        current = jdatetime.date(year1, month1, day1)
        end = jdatetime.date(year2, month2, day2)

        working_days = 0
        while current <= end:
            if self.is_working_day(current.strftime("%Y-%m-%d")):
                working_days += 1
            current += jdatetime.timedelta(days=1)

        return working_days


# نمونه برای استفاده راحت‌تر
holiday_manager = HolidayManager()

# توابع کمکی
def is_holiday(date_str: str = None) -> bool:
    return holiday_manager.is_holiday(date_str)

def is_working_day(date_str: str = None) -> bool:
    return holiday_manager.is_working_day(date_str)

def get_next_working_day(date_str: str = None) -> str:
    return holiday_manager.get_next_working_day(date_str)


def is_trading_day(now: datetime) -> bool:
    """
    آیا `now` (یک datetime آگاه از تایم‌زون تهران) در یک روز معاملاتی بورس است؟

    ترکیب دو شرط مستقل:
      ۱) روز هفته باید جزو WORKING_DAYS باشد (شنبه تا چهارشنبه، پنج‌شنبه/جمعه رد می‌شود)
      ۲) روز نباید تعطیل رسمی/اضطراری باشد (is_holiday)

    این تابع single source of truth برای main.py و daily_summary_main.py است
    تا این دو entry point در تشخیص روز معاملاتی از هم drift نکنند.
    """
    weekday = (now.weekday() + 2) % 7  # تبدیل به شمارش شنبه=0 ... جمعه=6
    if weekday not in WORKING_DAYS:
        return False

    jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
    today_str = jnow.strftime("%Y-%m-%d")
    return not is_holiday(today_str)
