from datetime import datetime
from typing import List, Dict, Set
import jdatetime

# ========================================
# تعطیلات رسمی سال 1404 و 1405
# ========================================
HOLIDAYS_1404 = [
    "1404-01-01","1404-01-02","1404-01-03","1404-01-04",
    "1404-01-12","1404-01-13","1404-03-14","1404-03-15",
    "1404-04-11","1404-04-19","1404-04-20","1404-05-15",
    "1404-06-26","1404-07-04","1404-07-18","1404-07-19",
    "1404-08-28","1404-09-08","1404-09-10","1404-09-17",
    "1404-11-20","1404-12-03","1404-12-15","1404-12-25",
]

HOLIDAYS_1405 = [
    "1405-01-01","1405-01-02","1405-01-03","1405-01-04",
    "1405-01-12","1405-01-13","1405-03-14","1405-03-15",
    "1405-03-30","1405-04-08","1405-04-09","1405-05-04",
    "1405-06-15","1405-06-23","1405-07-07","1405-07-08",
    "1405-08-17","1405-08-27","1405-08-29","1405-09-06",
    "1405-11-09","1405-11-22","1405-12-04","1405-12-14",
]

ALL_HOLIDAYS = HOLIDAYS_1404 + HOLIDAYS_1405

HOLIDAY_DESCRIPTIONS: Dict[str, str] = {
    "1404-01-01": "نوروز - سال نو",
    "1404-01-02": "نوروز - روز دوم",
    "1404-01-03": "نوروز - روز سوم",
    "1404-01-04": "نوروز - روز چهارم",
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
        self.holidays_set: Set[str] = set(ALL_HOLIDAYS)

    def is_holiday(self, date_str: str = None) -> bool:
        """موقتاً همه روزها کاری در نظر گرفته شوند"""
        return False

    def is_working_day(self, date_str: str = None) -> bool:
        if date_str is None:
            today = jdatetime.date.today()
            date_str = today.strftime("%Y-%m-%d")
        else:
            year, month, day = map(int, date_str.split("-"))
            today = jdatetime.date(year, month, day)

        if today.weekday() == 5:  # جمعه
            return False

        return not self.is_holiday(date_str)

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

    def get_holiday_description(self, date_str: str) -> str:
        return HOLIDAY_DESCRIPTIONS.get(date_str, "توضیحی موجود نیست")

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