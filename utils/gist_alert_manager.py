"""
مدیریت هشدارها با ذخیره در GitHub Gist
"""
import json
import requests
from datetime import datetime, timedelta
import jdatetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class GistAlertManager:
    """مدیریت هشدارها با ذخیره مستقیم در GitHub Gist، ۳ روز اخیر نگه داشته می‌شود"""

    def __init__(self, github_token: str, gist_id: str = None):
        self.github_token = github_token
        self.gist_id = gist_id
        self.api_url = "https://api.github.com/gists"
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.today_jalali = jdatetime.date.today().strftime("%Y-%m-%d")

        if not self.gist_id:
            self._create_new_gist()

    def _load_gist_content(self) -> dict:
        """بارگذاری محتوای Gist"""
        if not self.gist_id:
            return {}
        try:
            url = f"{self.api_url}/{self.gist_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                gist_data = response.json()
                content = gist_data["files"].get("alert_cache.json", {}).get("content", "{}")
                return json.loads(content)
            logger.error(f"❌ خطای دریافت Gist: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ خطا در بارگذاری Gist: {e}")
        return {}

    def _save_to_gist(self, data: dict):
        """ذخیره داده در Gist"""
        if not self.gist_id:
            logger.error("❌ Gist ID موجود نیست")
            return False
        try:
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
            response = requests.patch(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 200:
                return True
            logger.error(f"❌ خطای ذخیره در Gist: {response.status_code}")
        except Exception as e:
            logger.error(f"❌ خطا در ذخیره Gist: {e}")
        return False

    def _create_new_gist(self):
        """ایجاد Gist جدید"""
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

    def should_send_alert(self, symbol: str, alert_type: str) -> bool:
        """بررسی اینکه آیا باید هشدار ارسال شود یا نه"""
        data = self._load_gist_content()
        today_alerts = data.get(self.today_jalali, [])
        for alert in today_alerts:
            if alert["symbol"] == symbol and alert["alert_type"] == alert_type:
                return False
        return True

    def mark_as_sent(self, symbol: str, alert_type: str):
        """علامت‌گذاری هشدار به عنوان ارسال شده"""
        data = self._load_gist_content()
        if self.today_jalali not in data:
            data[self.today_jalali] = []
        data[self.today_jalali].append({"symbol": symbol, "alert_type": alert_type})
        self._save_to_gist(data)

    def get_today_stats(self) -> dict:
        """دریافت آمار هشدارهای امروز"""
        data = self._load_gist_content()
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