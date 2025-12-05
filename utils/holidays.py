"""
مدیریت تعطیلات رسمی بورس ایران
"""

from datetime import datetime
from typing import List, Dict, Set
import jdatetime

# ========================================
# تعطیلات رسمی سال 1404 (شمسی)
# ========================================
HOLIDAYS_1404 = [
    # نوروز
    "1404-01-01",
    "1404-01-02",
    "1404-01-03",
    "1404-01-04",
    # روز جمهوری اسلامی
    "1404-01-12",
    # سیزده به در
    "1404-01-13",
    # رحلت حضرت امام خمینی
    "1404-03-14",
    # قیام 15 خرداد
    "1404-03-15",
    # شهادت امام علی
    "1404-04-11",
    # عید فطر
    "1404-04-19",
    "1404-04-20",
    # شهادت امام صادق
    "1404-05-15",
    # عید قربان
    "1404-06-26",
    # عید غدیر
    "1404-07-04",
    # تاسوعا
    "1404-07-18",
    # عاشورا
    "1404-07-19",
    # اربعین
    "1404-08-28",
    # رحلت پیامبر و شهادت امام حسن
    "1404-09-08",
    # شهادت امام رضا
    "1404-09-10",
    # ولادت پیامبر
    "1404-09-17",
    # شهادت فاطمه زهرا
    "1404-11-20",
    # ولادت امام علی
    "1404-12-03",
    # مبعث
    "1404-12-15",
    # ولادت امام زمان
    "1404-12-25",
]

# ========================================
# تعطیلات رسمی سال 1405 (شمسی)
# ========================================
HOLIDAYS_1405 = [
    # نوروز
    "1405-01-01",
    "1405-01-02",
    "1405-01-03",
    "1405-01-04",
    # روز جمهوری اسلامی
    "1405-01-12",
    # سیزده به در
    "1405-01-13",
    # رحلت حضرت امام خمینی
    "1405-03-14",
    # قیام 15 خرداد
    "1405-03-15",
    # شهادت امام علی
    "1405-03-30",
    # عید فطر
    "1405-04-08",
    "1405-04-09",
    # شهادت امام صادق
    "1405-05-04",
    # عید قربان
    "1405-06-15",
    # عید غدیر
    "1405-06-23",
    # تاسوعا
    "1405-07-07",
    # عاشورا
    "1405-07-08",
    # اربعین
    "1405-08-17",
    # رحلت پیامبر و شهادت امام حسن
    "1405-08-27",
    # شهادت امام رضا
    "1405-08-29",
    # ولادت پیامبر
    "1405-09-06",
    # شهادت فاطمه زهرا
    "1405-11-09",
    # ولادت امام علی
    "1405-11-22",
    # مبعث
    "1405-12-04",
    # ولادت امام زمان
    "1405-12-14",
]

# ترکیب تمام تعطیلات
ALL_HOLIDAYS = HOLIDAYS_1404 + HOLIDAYS_1405

# ========================================
# توضیحات تعطیلات (اختیاری)
# ========================================
HOLIDAY_DESCRIPTIONS: Dict[str, str] = {
    # نوروز
    "1404-01-01": "نوروز - سال نو",
    "1404-01-02": "نوروز - روز دوم",
    "1404-01-03": "نوروز - روز سوم",
    "1404-01-04": "نوروز - روز چهارم",
    # سایر تعطیلات مهم
    "1404-01-12": "روز جمهوری اسلامی ایران",
    "1404-01-13": "سیزده به در",
    "1404-03-14": "رحلت حضرت امام خمینی (ره)",
    "1404-03-15": "قیام 15 خرداد",
    "1404-07-19": "عاشورا - شهادت امام حسین (ع)",
    "1404-08-28": "اربعین حسینی",
}


class HolidayManager:
    """مدیریت تعطیلات بورس"""

    def __init__(self):
        """مقداردهی اولیه"""
        self.holidays_set: Set[str] = set(ALL_HOLIDAYS)

    def is_holiday(self, date_str: str = None) -> bool:
        """
        بررسی تعطیل بودن یک تاریخ

        Args:
            date_str: تاریخ به فرمت 'YYYY-MM-DD' (شمسی)
                     اگر None باشد، تاریخ امروز بررسی می‌شود

        Returns:
            bool: True اگر تعطیل باشد
        """
        if date_str is None:
            today = jdatetime.date.today()
            date_str = today.strftime("%Y-%m-%d")

        return date_str in self.holidays_set

    def is_working_day(self, date_str: str = None) -> bool:
        """
        بررسی روز کاری بودن (نه تعطیل و نه جمعه)

        Args:
            date_str: تاریخ به فرمت 'YYYY-MM-DD' (شمسی)

        Returns:
            bool: True اگر روز کاری باشد
        """
        if date_str is None:
            today = jdatetime.date.today()
            date_str = today.strftime("%Y-%m-%d")
        else:
            year, month, day = map(int, date_str.split("-"))
            today = jdatetime.date(year, month, day)

        # جمعه = 6 در jdatetime
        if today.weekday() == 6:
            return False

        return not self.is_holiday(date_str)

    def get_holidays_in_range(self, start_date: str, end_date: str) -> List[str]:
        """
        دریافت لیست تعطیلات در یک بازه زمانی

        Args:
            start_date: تاریخ شروع 'YYYY-MM-DD'
            end_date: تاریخ پایان 'YYYY-MM-DD'

        Returns:
            List[str]: لیست تعطیلات در بازه
        """
        holidays_in_range = []

        for holiday in sorted(self.holidays_set):
            if start_date <= holiday <= end_date:
                holidays_in_range.append(holiday)

        return holidays_in_range

    def get_next_working_day(self, date_str: str = None) -> str:
        """
        پیدا کردن اولین روز کاری بعد از تاریخ مشخص

        Args:
            date_str: تاریخ شروع 'YYYY-MM-DD'

        Returns:
            str: تاریخ روز کاری بعدی
        """
        if date_str is None:
            current_date = jdatetime.date.today()
        else:
            year, month, day = map(int, date_str.split("-"))
            current_date = jdatetime.date(year, month, day)

        # حداکثر 30 روز جلو رو چک کن
        for _ in range(30):
            current_date += jdatetime.timedelta(days=1)
            date_str = current_date.strftime("%Y-%m-%d")

            if self.is_working_day(date_str):
                return date_str

        raise ValueError("روز کاری در 30 روز آینده پیدا نشد!")

    def get_holiday_description(self, date_str: str) -> str:
        """
        دریافت توضیحات یک تعطیلی

        Args:
            date_str: تاریخ 'YYYY-MM-DD'

        Returns:
            str: توضیحات تعطیلی یا پیام خطا
        """
        return HOLIDAY_DESCRIPTIONS.get(date_str, "توضیحی موجود نیست")

    def count_working_days(self, start_date: str, end_date: str) -> int:
        """
        شمارش روزهای کاری بین دو تاریخ

        Args:
            start_date: تاریخ شروع 'YYYY-MM-DD'
            end_date: تاریخ پایان 'YYYY-MM-DD'

        Returns:
            int: تعداد روزهای کاری
        """
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


# نمونه‌ای از کلاس برای استفاده راحت‌تر
holiday_manager = HolidayManager()


# ========================================
# توابع کمکی (برای سادگی استفاده)
# ========================================
def is_holiday(date_str: str = None) -> bool:
    """بررسی تعطیل بودن یک تاریخ"""
    return holiday_manager.is_holiday(date_str)


def is_working_day(date_str: str = None) -> bool:
    """بررسی روز کاری بودن"""
    return holiday_manager.is_working_day(date_str)


def get_next_working_day(date_str: str = None) -> str:
    """پیدا کردن روز کاری بعدی"""
    return holiday_manager.get_next_working_day(date_str)