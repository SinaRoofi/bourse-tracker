"""
مدیریت هشدارها با ذخیره در GitHub Gist
نسخه Async برای استفاده موازی
"""
import json
import aiohttp
import asyncio
import requests  # فقط برای ایجاد اولیه Gist
from datetime import datetime
import jdatetime
import logging
from typing import Optional
import time

logger = logging.getLogger(__name__)


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

        # Lock برای جلوگیری از race condition
        self._lock = asyncio.Lock()

        # Cache محلی
        self._cache = None
        self._cache_time = 0
        self._cache_duration = 10  # ثانیه

        if not self.gist_id:
            # ایجاد Gist باید sync باشه (فقط یکبار اول)
            self._create_new_gist_sync()

    def _create_new_gist_sync(self):
        """ایجاد Gist جدید - sync (فقط در init)"""
        initial_data = {self.today_jalali: []}
        payload = {
            "description": "Bourse Tracker Alert Cache - هشدارهای ارسال شده بورس",
            "public": False,
            "files": {
                "alert_cache.json": {"content": json.dumps(initial_data, ensure_ascii=False, indent=2)},
                "README.md": {"content": "# Bourse Alert Cache\nاین Gist برای ذخیره هشدارها استفاده می‌شود."}
            }
        }
        try:
            response = requests.post(self.api_url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 201:
                gist_data = response.json()
                self.gist_id = gist_data["id"]
                logger.info(f"✅ Gist جدید ایجاد شد: {self.gist_id}")
            else:
                raise Exception(f"Failed to create Gist: {response.text}")
        except Exception as e:
            logger.error(f"❌ خطا در ایجاد Gist: {e}")
            raise

    async def _load_gist_content(self, use_cache: bool = True) -> dict:
        """بارگذاری محتوای Gist با aiohttp"""
        if not self.gist_id:
            return {}

        # استفاده از cache اگر هنوز معتبر است
        current_time = time.time()
        if use_cache and self._cache is not None and (current_time - self._cache_time) < self._cache_duration:
            return self._cache.copy()

        try:
            url = f"{self.api_url}/{self.gist_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        gist_data = await response.json()
                        content = gist_data["files"].get("alert_cache.json", {}).get("content", "{}")
                        data = json.loads(content)

                        # به‌روزرسانی cache
                        self._cache = data
                        self._cache_time = current_time

                        return data
                    else:
                        logger.error(f"❌ خطای دریافت Gist: {response.status}")
        except Exception as e:
            logger.error(f"❌ خطا در بارگذاری Gist: {e}")
        return {}

    async def _save_to_gist(self, data: dict, max_retries: int = 3) -> bool:
        """
        ذخیره داده در Gist با aiohttp و retry mechanism
        
        Args:
            data: داده‌ای که باید ذخیره شود
            max_retries: تعداد تلاش مجدد در صورت خطا
        
        Returns:
            True در صورت موفقیت
        """
        if not self.gist_id:
            logger.error("❌ Gist ID موجود نیست")
            return False

        # استفاده از Lock برای جلوگیری از همزمان بودن درخواست‌ها
        async with self._lock:
            for attempt in range(max_retries):
                try:
                    # در صورت Conflict، داده جدید را بخوانیم
                    if attempt > 0:
                        logger.warning(f"🔄 تلاش مجدد {attempt}/{max_retries}...")
                        await asyncio.sleep(0.5 * attempt)
                        # خواندن آخرین نسخه Gist
                        current_data = await self._load_gist_content(use_cache=False)
                        # ادغام داده‌های جدید با داده‌های موجود
                        if self.today_jalali in current_data and self.today_jalali in data:
                            # جلوگیری از duplicate
                            existing_alerts = {
                                (a["symbol"], a["alert_type"]) 
                                for a in current_data[self.today_jalali]
                            }
                            new_alerts = [
                                a for a in data[self.today_jalali]
                                if (a["symbol"], a["alert_type"]) not in existing_alerts
                            ]
                            current_data[self.today_jalali].extend(new_alerts)
                            data = current_data
                        else:
                            data.update(current_data)

                    # نگهداری فقط ۳ روز اخیر
                    sorted_days = sorted(data.keys(), reverse=True)[:3]
                    new_data = {day: data[day] for day in sorted_days}

                    payload = {
                        "files": {
                            "alert_cache.json": {
                                "content": json.dumps(new_data, ensure_ascii=False, indent=2)
                            }
                        }
                    }

                    url = f"{self.api_url}/{self.gist_id}"
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.patch(
                            url, 
                            headers=self.headers, 
                            json=payload, 
                            timeout=aiohttp.ClientTimeout(total=10)
                        ) as response:
                            if response.status == 200:
                                # به‌روزرسانی cache
                                self._cache = new_data
                                self._cache_time = time.time()

                                if attempt > 0:
                                    logger.info(f"✅ ذخیره موفق در تلاش {attempt + 1}")
                                return True
                            elif response.status == 409:
                                # Conflict - ادامه به تلاش بعدی
                                logger.warning(f"⚠️ Conflict (409) در تلاش {attempt + 1}/{max_retries}")
                                continue
                            else:
                                logger.error(f"❌ خطای ذخیره در Gist: {response.status}")
                                return False

                except Exception as e:
                    logger.error(f"❌ خطا در ذخیره Gist (تلاش {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        return False

            logger.error(f"❌ ذخیره ناموفق بعد از {max_retries} تلاش")
            return False

    def should_send_alert(self, symbol: str, alert_type: str) -> bool:
        """
        بررسی اینکه آیا باید هشدار ارسال شود یا نه
        این متد sync باقی می‌ماند چون فقط cache محلی رو چک می‌کنه
        """
        if self._cache is None:
            # اولین بار - باید sync load کنیم
            try:
                url = f"{self.api_url}/{self.gist_id}"
                response = requests.get(url, headers=self.headers, timeout=5)
                if response.status_code == 200:
                    gist_data = response.json()
                    content = gist_data["files"].get("alert_cache.json", {}).get("content", "{}")
                    self._cache = json.loads(content)
                    self._cache_time = time.time()
                else:
                    logger.warning(f"⚠️ خطا در load اولیه Gist: status {response.status_code}")
                    self._cache = {}
            except Exception as e:
                logger.warning(f"⚠️ خطا در load اولیه Gist: {e}")
                self._cache = {}
        
        today_alerts = self._cache.get(self.today_jalali, [])
        for alert in today_alerts:
            if alert["symbol"] == symbol and alert["alert_type"] == alert_type:
                return False
        return True

    async def mark_multiple_as_sent(self, alerts: list) -> bool:
        """
        علامت‌گذاری چندین هشدار به صورت یکجا - نسخه async
        
        Args:
            alerts: لیستی از tuple های (symbol, alert_type)
        
        Returns:
            True در صورت موفقیت
        """
        if not alerts:
            return True

        data = await self._load_gist_content(use_cache=False)

        if self.today_jalali not in data:
            data[self.today_jalali] = []

        # جلوگیری از duplicate
        existing_alerts = {(a["symbol"], a["alert_type"]) for a in data[self.today_jalali]}

        new_alerts = [
            {"symbol": symbol, "alert_type": alert_type}
            for symbol, alert_type in alerts
            if (symbol, alert_type) not in existing_alerts
        ]

        if new_alerts:
            data[self.today_jalali].extend(new_alerts)
            # به‌روزرسانی cache محلی
            self._cache = data
            self._cache_time = time.time()
            # ذخیره async
            return await self._save_to_gist(data)

        return True

    async def get_today_stats(self) -> dict:
        """دریافت آمار هشدارهای امروز - نسخه async"""
        data = await self._load_gist_content()
        today_alerts = data.get(self.today_jalali, [])
        alert_counts = {}
        for alert in today_alerts:
            alert_type = alert["alert_type"]
            alert_counts[alert_type] = alert_counts.get(alert_type, 0) + 1
        return {
            "date": self.today_jalali,
            "total_alerts": len(today_alerts),
            "alerts_by_type": alert_counts,
            "gist_id": self.gist_id
        }

    def get_gist_url(self) -> Optional[str]:
        """دریافت آدرس Gist"""
        if self.gist_id:
            return f"https://gist.github.com/{self.gist_id}"
        return None