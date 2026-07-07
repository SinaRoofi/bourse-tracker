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
            alerts: لیست تاپل‌های (symbol, alert_type, value)
                    value می‌تواند None باشد (برای فیلترهایی که value ندارند)
        """
        if not alerts:
            return True

        data = await self._load_gist_content(use_cache=False)
        data.setdefault(self.today_jalali, [])

        # پاکسازی روزهای قدیمی (نگه داشتن فقط ۳ روز اخیر)
        cutoff = (jdatetime.date.today() - jdatetime.timedelta(days=3)).strftime("%Y-%m-%d")
        keys_to_delete = [
            k for k in list(data.keys())
            if k != "_daily_summary_sent" and k < cutoff
        ]
        for k in keys_to_delete:
            del data[k]
            logger.info(f"🗑️ روز قدیمی پاک شد: {k}")

        existing = {(a["symbol"], a["alert_type"]) for a in data[self.today_jalali]}

        new_items = []
        for item in alerts:
            # پشتیبانی از هر دو فرمت: (symbol, alert_type) و (symbol, alert_type, value)
            if len(item) == 3:
                s, t, val = item
            else:
                s, t = item
                val = None

            if (s, t) not in existing:
                entry = {"symbol": s, "alert_type": t}
                if val is not None:
                    entry["value"] = val
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
