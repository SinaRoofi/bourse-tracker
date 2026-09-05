"""
مدیریت هشدارها با ذخیره در GitHub Gist
نسخه Async برای استفاده موازی
+ قفل ارسال Daily Summary (فقط یک‌بار در روز)
"""
import json
import aiohttp
import asyncio
import requests
import jdatetime
import logging
from typing import Optional
import time

logger = logging.getLogger(__name__)

# پسوند dedup برای کپیِ واچ‌لیست شخصی در کانال دوم — عمداً از filter_name جدا نگه
# داشته می‌شه تا ارسال به کانال اصلی و ارسال به کانال دوم مستقل از هم dedup بشن
# (وگرنه چون GistAlertManager فقط بر اساس (symbol, alert_type) چک می‌کنه، ارسال
# دوم به‌اشتباه «قبلاً ارسال شده» تشخیص داده می‌شد و رد می‌شد).
# این ثابت اینجا تعریف شده (نه در main.py) چون هم main.py و هم
# daily_summary_generator.py بهش نیاز دارن؛ داشتنش در یک‌جا از عدم‌همگامی
# بین دو فایل جلوگیری می‌کنه.
WATCHLIST_COPY_SUFFIX = "__watchlist_copy"


class GistAlertManager:
    """مدیریت هشدارها با ذخیره مستقیم در GitHub Gist - نسخه Async"""

    def __init__(self, github_token: str, gist_id: str = None):
        self.github_token = github_token
        self.gist_id = gist_id
        self.api_url = "https://api.github.com/gists"
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        self.today_jalali = jdatetime.date.today().strftime("%Y-%m-%d")

        self._lock = asyncio.Lock()

        self._cache = None
        self._cache_time = 0
        self._cache_duration = 10

        if not self.gist_id:
            # ⚠️ این متد یک requests.post سینک (بلاکینگ) اجرا می‌کنه.
            # این شاخه امروز unreachable هست چون main.py/daily_summary_main.py
            # قبل از ساخت این کلاس، validate_config() رو صدا می‌زنن که بدون
            # GIST_ID با exception متوقف می‌شه. اگه یه‌روز GIST_ID رو اختیاری
            # کردی، این خط دوباره می‌تونه event loop رو بلاک کنه — اون‌موقع
            # باید __init__ رو به یک async factory (classmethod create) تبدیل کنی.
            self._create_new_gist_sync()

    # ------------------------------------------------------------------
    # ایجاد اولیه Gist
    # ------------------------------------------------------------------
    def _create_new_gist_sync(self):
        initial_data = {
            "_daily_summary_sent": {},
            "_industry_universe": {},
            self.today_jalali: []
        }

        payload = {
            "description": "Bourse Tracker Alert Cache",
            "public": False,
            "files": {
                "alert_cache.json": {
                    "content": json.dumps(initial_data, ensure_ascii=False)
                },
                "README.md": {
                    "content": "# Bourse Tracker Gist\nAlert cache + Daily Summary lock"
                }
            }
        }

        response = requests.post(
            self.api_url,
            headers=self.headers,
            json=payload,
            timeout=10
        )

        if response.status_code != 201:
            raise RuntimeError(f"Failed to create Gist: {response.text}")

        self.gist_id = response.json()["id"]
        logger.info(f"✅ Gist created: {self.gist_id}")

    # ------------------------------------------------------------------
    # Load Gist
    # ------------------------------------------------------------------
    async def _load_gist_content(self, use_cache: bool = True) -> dict:
        if not self.gist_id:
            return {}

        now = time.time()
        if use_cache and self._cache and (now - self._cache_time) < self._cache_duration:
            return self._cache.copy()

        url = f"{self.api_url}/{self.gist_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, timeout=10) as r:
                if r.status != 200:
                    logger.error(f"❌ Failed to load gist: {r.status}")
                    return {}

                gist = await r.json()
                content = gist["files"]["alert_cache.json"]["content"]

                try:
                    data = json.loads(content)
                except json.JSONDecodeError as e:
                    logger.error(f"❌ JSON خراب در Gist: {e} - ریست می‌شود")
                    data = {"_daily_summary_sent": {}, self.today_jalali: []}
                    await self._save_to_gist(data)

                self._cache = data
                self._cache_time = now
                return data

    # ------------------------------------------------------------------
    # Save Gist
    # ------------------------------------------------------------------
    async def _save_to_gist(self, data: dict) -> bool:
        async with self._lock:
            payload = {
                "files": {
                    "alert_cache.json": {
                        "content": json.dumps(data, ensure_ascii=False)
                    }
                }
            }

            url = f"{self.api_url}/{self.gist_id}"
            async with aiohttp.ClientSession() as session:
                async with session.patch(url, headers=self.headers, json=payload, timeout=10) as r:
                    if r.status == 200:
                        self._cache = data
                        self._cache_time = time.time()
                        return True

                    logger.error(f"❌ Failed to save gist: {r.status}")
                    return False

    # ------------------------------------------------------------------
    # پاکسازی داده‌ی قدیمی (روزهای هشدار + روزهای داخل _industry_universe)
    # ------------------------------------------------------------------
    @staticmethod
    def _prune_old_days(data: dict, keep_days: int = 3) -> None:
        """
        هر ساختاری که کلیدش تاریخ جلالی روزانه باشه رو به آخرین keep_days
        روز محدود می‌کنه - هم سطح بالای data (روزهای هشدار)، هم داخل
        _industry_universe (که قبلاً هیچ‌وقت پاک نمی‌شد و هر روز فقط
        بزرگ‌تر می‌شد). in-place تغییر می‌ده.
        """
        cutoff = (jdatetime.date.today() - jdatetime.timedelta(days=keep_days)).strftime("%Y-%m-%d")

        top_level_keys = [
            k for k in list(data.keys())
            if k not in ("_daily_summary_sent", "_industry_universe") and k < cutoff
        ]
        for k in top_level_keys:
            del data[k]
            logger.info(f"🗑️ روز قدیمی پاک شد: {k}")

        universe = data.get("_industry_universe")
        if isinstance(universe, dict):
            universe_keys = [k for k in list(universe.keys()) if k < cutoff]
            for k in universe_keys:
                del universe[k]
            if universe_keys:
                logger.info(f"🗑️ {len(universe_keys)} روز قدیمی از _industry_universe پاک شد")

    # ------------------------------------------------------------------
    # Daily Summary Lock
    # ------------------------------------------------------------------
    async def is_today_summary_sent(self) -> bool:
        data = await self._load_gist_content(use_cache=False)
        return data.get("_daily_summary_sent", {}).get(self.today_jalali, False)

    async def mark_today_summary_sent(self) -> bool:
        data = await self._load_gist_content(use_cache=False)
        data.setdefault("_daily_summary_sent", {})[self.today_jalali] = True
        return await self._save_to_gist(data)

    # ------------------------------------------------------------------
    # Industry Universe (برای نرمال‌سازی «برترین صنایع» بر اساس اندازه‌ی صنعت)
    # ------------------------------------------------------------------
    async def get_industry_universe(self) -> dict:
        """
        تعداد کل نمادهای هر صنعت (نه فقط نمادهای هشداردهنده) که یک‌بار در
        روز توسط main.py ذخیره می‌شه. برای محاسبه‌ی «درصد مشارکت صنعت»
        در گزارش خلاصه‌ی روزانه استفاده می‌شه؛ اگه هنوز برای امروز ذخیره
        نشده باشه (مثلاً روز اول دیپلوی این ویژگی)، دیکشنری خالی برمی‌گرده.
        """
        data = await self._load_gist_content()
        return data.get("_industry_universe", {}).get(self.today_jalali, {})

    async def save_industry_universe(self, mapping: dict) -> bool:
        """
        ذخیره‌ی تعداد کل نماد هر صنعت برای امروز. عمداً idempotent هست —
        اگه امروز قبلاً ذخیره شده، دوباره Gist رو ننویس (چون main.py طی روز
        چندبار اجرا می‌شه و اندازه‌ی صنایع در طول روز عوض نمی‌شه، پس بازنویسی
        مکرر فقط هزینه‌ی بی‌مورد به Gist API تحمیل می‌کنه).
        """
        if not mapping:
            return True

        data = await self._load_gist_content(use_cache=False)
        existing = data.setdefault("_industry_universe", {})
        if existing.get(self.today_jalali):
            return True  # امروز قبلاً ذخیره شده

        existing[self.today_jalali] = mapping
        self._prune_old_days(data)
        return await self._save_to_gist(data)

    # ------------------------------------------------------------------
    # Alert Dedup
    # ------------------------------------------------------------------
    async def should_send_alert(self, symbol: str, alert_type: str) -> bool:
        data = await self._load_gist_content()
        today_alerts = data.get(self.today_jalali, [])
        return not any(
            a["symbol"] == symbol and a["alert_type"] == alert_type
            for a in today_alerts
        )

    async def mark_multiple_as_sent(self, alerts: list) -> bool:
        """
        ذخیره هشدارهای ارسال‌شده در Gist

        Args:
            alerts: لیست تاپل‌ها با یکی از این فرمت‌ها (طول متغیر، از چپ به راست):
                (symbol, alert_type)
                (symbol, alert_type, value)
                (symbol, alert_type, value, is_fund)
                (symbol, alert_type, value, is_fund, industry_name)
                (symbol, alert_type, value, is_fund, industry_name, price_change_percent)

                value: می‌تواند None باشد (برای فیلترهایی که value ندارند)
                is_fund: می‌تواند None باشد (یعنی نامشخص)
                industry_name: نام صنعت نماد در لحظه‌ی ارسال (برای گزارش «برترین صنایع»)؛
                    برای صندوق‌ها معمولاً None است
                price_change_percent: درصد تغییر قیمت پایانی نماد در لحظه‌ی ارسال (برای
                    نمایش کنار نماد در Top-N هر فیلتر)
        """
        if not alerts:
            return True

        data = await self._load_gist_content(use_cache=False)
        data.setdefault(self.today_jalali, [])

        # پاکسازی روزهای قدیمی (نگه داشتن فقط ۳ روز اخیر) - هم سطح بالا
        # هم داخل _industry_universe
        self._prune_old_days(data)

        existing = {(a["symbol"], a["alert_type"]) for a in data[self.today_jalali]}

        new_items = []
        for item in alerts:
            # پشتیبانی از فرمت‌های ۲ تا ۶ عضوی (ترتیب ثابت، از چپ اضافه می‌شن)
            is_fund = None
            industry_name = None
            price_change_percent = None

            n = len(item)
            if n == 6:
                s, t, val, is_fund, industry_name, price_change_percent = item
            elif n == 5:
                s, t, val, is_fund, industry_name = item
            elif n == 4:
                s, t, val, is_fund = item
            elif n == 3:
                s, t, val = item
            else:
                s, t = item
                val = None

            if (s, t) not in existing:
                entry = {"symbol": s, "alert_type": t}
                if val is not None:
                    entry["value"] = val
                if is_fund is not None:
                    entry["is_fund"] = bool(is_fund)
                if industry_name is not None:
                    entry["industry_name"] = industry_name
                if price_change_percent is not None:
                    entry["price_change_percent"] = price_change_percent
                new_items.append(entry)

        if not new_items:
            return True

        data[self.today_jalali].extend(new_items)
        return await self._save_to_gist(data)

    # ------------------------------------------------------------------
    # Stats / Utils
    # ------------------------------------------------------------------
    async def get_today_stats(self) -> dict:
        data = await self._load_gist_content()
        alerts = data.get(self.today_jalali, [])
        stats = {}
        for a in alerts:
            stats[a["alert_type"]] = stats.get(a["alert_type"], 0) + 1

        return {
            "date": self.today_jalali,
            "total_alerts": len(alerts),
            "alerts_by_type": stats,
            "gist_id": self.gist_id
        }

    def get_gist_url(self) -> Optional[str]:
        if self.gist_id:
            return f"https://gist.github.com/{self.gist_id}"
        return None
