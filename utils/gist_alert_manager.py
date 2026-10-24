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

from utils.tehran_time import tehran_today, tehran_today_str

logger = logging.getLogger(__name__)

# پسوند dedup برای کپیِ واچ‌لیست شخصی در کانال دوم — عمداً از filter_name جدا نگه
# داشته می‌شه تا ارسال به کانال اصلی و ارسال به کانال دوم مستقل از هم dedup بشن
# (وگرنه چون GistAlertManager فقط بر اساس (symbol, alert_type) چک می‌کنه، ارسال
# دوم به‌اشتباه «قبلاً ارسال شده» تشخیص داده می‌شد و رد می‌شد).
# این ثابت اینجا تعریف شده (نه در main.py) چون هم main.py و هم
# daily_summary_generator.py بهش نیاز دارن؛ داشتنش در یک‌جا از عدم‌همگامی
# بین دو فایل جلوگیری می‌کنه.
WATCHLIST_COPY_SUFFIX = "__watchlist_copy"

# فایل جدای اسنپ‌شات صف‌ها (فیلتر ۱۷/۱۸ + نمایش آخرین ارزش صف خرید در خلاصه‌ی
# بازار) تو همون Gist؛ عمداً از alert_cache.json جداست تا حجم/منطق هشدارها
# و پاکسازی روزهای قدیمی دست نخوره.
QUEUE_SNAPSHOT_FILE = "queue_snapshot.json"

# تاریخچه‌ی ۱۰ روزه‌ی سرانه خرید (فیلتر ۱۹) تو همون Gist صف‌ها (QUEUE_GIST) ولی تو فایل جدا.
SARANE_HISTORY_FILE = "sarane_history.json"
# فایل تاریخچه چند صد کیلوبایته (تا حدود ۴۰۰KB تو بدترین حالت)؛ timeout پیش‌فرض ۱۰ ثانیه برای
# آپلود/دانلود این حجم تو شبکه‌ی کند کمه.
SARANE_HTTP_TIMEOUT = 30

# GitHub برای نوشتن‌های پشت‌سرهم تو یک Gist (rate limit ثانویه) 403 می‌ده و توصیه
# می‌کنه بین دو درخواست نوشتن حداقل ۱ ثانیه فاصله باشه. اگه نوشتن dedup شکست بخوره،
# اجرای بعدی همون هشدارها رو دوباره می‌فرسته (اسپم)، پس هر نوشتن/خوندن با فاصله و
# retry انجام می‌شه.
MIN_WRITE_GAP_SECONDS = 1.5
HTTP_MAX_ATTEMPTS = 4
HTTP_RETRY_STATUSES = {403, 429, 500, 502, 503, 504}
HTTP_MAX_WAIT_SECONDS = 20


class GistAlertManager:
    """مدیریت هشدارها با ذخیره مستقیم در GitHub Gist - نسخه Async"""

    def __init__(
        self,
        github_token: str,
        gist_id: str = None,
        queue_gist_token: str = None,
        queue_gist_id: str = None,
    ):
        self.github_token = github_token
        self.gist_id = gist_id
        self.api_url = "https://api.github.com/gists"
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        # Gist جدای اسنپ‌شات صف‌ها. فالبک به Gist اصلی عمداً وجود نداره تا Gist اصلی
        # (دیداپ هشدارها) با اسنپ‌شات پر نشه؛ اگه این دو تنظیم نشن، ردیاب صف و
        # ارزش صف تو خلاصه‌ی بازار غیرفعال می‌شن.
        self.queue_gist_id = None
        self.queue_headers = None
        if queue_gist_id and queue_gist_token:
            self.queue_gist_id = queue_gist_id
            self.queue_headers = {
                "Authorization": f"token {queue_gist_token}",
                "Accept": "application/vnd.github.v3+json"
            }

        self.today_jalali = tehran_today_str()

        self._lock = asyncio.Lock()

        self._cache = None
        self._cache_time = 0
        self._cache_duration = 10
        self._last_write_ts = {}  # آخرین زمان نوشتن به‌ازای هر توکن (محدودیت GitHub روی هر اکانته)
        self._load_failed = False  # آخرین خوندن alert_cache.json شکست خورده؟
        # کش کوتاه‌مدت GET گیست صف‌ها: queue_snapshot.json و sarane_history.json تو یک Gist‌ان،
        # پس با یک GET هر دو خونده می‌شن (نصف درخواست‌های خوندن). بعد از هر نوشتن پاک می‌شه.
        self._queue_gist_cache = None
        self._queue_gist_cache_time = 0.0
        self._queue_gist_cache_ttl = 30

        if not self.gist_id:
            # ⚠️ این متد یک requests.post سینک (بلاکینگ) اجرا می‌کنه.
            # این شاخه امروز unreachable هست چون main.py/daily_summary_main.py
            # قبل از ساخت این کلاس، validate_config() رو صدا می‌زنن که بدون
            # GIST_ID با exception متوقف می‌شه. اگه یه‌روز GIST_ID رو اختیاری
            # کردی، این خط دوباره می‌تونه event loop رو بلاک کنه — اون‌موقع
            # باید __init__ رو به یک async factory (classmethod create) تبدیل کنی.
            self._create_new_gist_sync()

    @property
    def queue_gist_enabled(self) -> bool:
        return bool(self.queue_gist_id and self.queue_headers)

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
    async def _http(
        self,
        method: str,
        url: str,
        payload: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: int = 10,
    ):
        """
        درخواست به GitHub API با فاصله‌ی حداقلی بین نوشتن‌ها و retry روی خطاهای
        موقتی (403 rate limit، 429، 5xx). برمی‌گردونه: (status, body) که body برای
        200 دیکشنری JSON و برای خطا متن کوتاه پاسخه.
        """
        status, body = 0, None
        headers = headers or self.headers
        write_key = headers.get("Authorization", "")
        for attempt in range(1, HTTP_MAX_ATTEMPTS + 1):
            if method != "GET":
                gap = MIN_WRITE_GAP_SECONDS - (time.time() - self._last_write_ts.get(write_key, 0.0))
                if gap > 0:
                    await asyncio.sleep(gap)

            retry_after = None
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.request(
                        method, url, headers=headers, json=payload, timeout=timeout
                    ) as r:
                        status = r.status
                        if status == 200:
                            body = await r.json()
                        else:
                            body = (await r.text())[:300]
                            retry_after = r.headers.get("Retry-After")
            except Exception as e:
                status, body = 0, str(e)

            if method != "GET":
                self._last_write_ts[write_key] = time.time()

            if status == 200:
                return status, body

            retryable = status == 0 or status in HTTP_RETRY_STATUSES
            final = (not retryable) or attempt == HTTP_MAX_ATTEMPTS
            # تلاش‌های وسط (که retry می‌شن) فقط warning‌ان؛ error فقط وقتی که واقعاً تموم شد
            (logger.error if final else logger.warning)(
                f"{'❌' if final else '⚠️'} Gist {method} ناموفق: {status} (تلاش {attempt}/{HTTP_MAX_ATTEMPTS}) "
                f"{body if body else ''} retry-after={retry_after}"
            )
            if final:
                break

            # backoff نمایی (۳، ۶، ۱۲ ثانیه)؛ اگه GitHub Retry-After داده باشه همون رعایت می‌شه
            backoff = 3.0 * (2 ** (attempt - 1))
            try:
                wait = float(retry_after) if retry_after else backoff
            except ValueError:
                wait = backoff
            await asyncio.sleep(min(wait, HTTP_MAX_WAIT_SECONDS))

        return status, body

    async def _fetch_raw(self, raw_url: Optional[str]) -> Optional[str]:
        """متن کامل یک فایل Gist از raw_url (برای فایل‌های بزرگ‌تر از ۱MB). شکست -> None."""
        if not raw_url:
            return None
        for attempt in range(1, 3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(raw_url, timeout=20) as r:
                        if r.status == 200:
                            return await r.text()
                        logger.error(f"❌ raw_url ناموفق: {r.status} (تلاش {attempt}/2)")
            except Exception as e:
                logger.error(f"❌ raw_url خطا: {e} (تلاش {attempt}/2)")
            await asyncio.sleep(2)
        return None

    async def _load_gist_content(self, use_cache: bool = True, allow_stale: bool = False) -> dict:
        """
        محتوای alert_cache.json. اگه خوندن Gist شکست بخوره:
          - allow_stale=True و کش قدیمی موجود: همون کش برمی‌گرده.
          - وگرنه {} برمی‌گرده و self._load_failed=True می‌شه تا فراخوان بتونه
            به‌جای «هیچ هشداری ارسال نشده» فرض کردن (که اسپم و بازنویسی خطرناک
            Gist رو می‌سازه) تصمیم درست بگیره.
        """
        if not self.gist_id:
            return {}

        now = time.time()
        if use_cache and self._cache and (now - self._cache_time) < self._cache_duration:
            self._load_failed = False
            return self._cache.copy()

        status, gist = await self._http("GET", f"{self.api_url}/{self.gist_id}")
        if status != 200:
            if allow_stale and self._cache:
                logger.warning("⚠️ خوندن Gist شکست خورد - از کش قدیمی همین اجرا استفاده شد")
                self._load_failed = False
                return self._cache.copy()
            self._load_failed = True
            return {}

        file_info = (gist or {}).get("files", {}).get("alert_cache.json")
        if file_info is None:
            # قبلاً KeyError می‌داد و کل اجرا می‌افتاد. تو این حالت عمداً «خوندن شکست‌خورده» حساب
            # می‌شه (نه دیتای خالی) تا کش هشدارها بازنویسی و هشدارها دوباره ارسال نشن.
            logger.error("❌ فایل alert_cache.json توی Gist نیست - باید یه فایل با همین نام (محتوا: {}) بسازی")
            self._load_failed = True
            return {}
        self._load_failed = False
        content = file_info.get("content") or ""

        # API گیست فقط ۱ مگابایت اول هر فایل رو برمی‌گردونه (truncated=True).
        # JSON ناقص نباید «خراب» حساب بشه و باعث ریست داده بشه؛ متن کامل رو از
        # raw_url (آدرس نسخه‌ی مشخص) می‌خونیم و اگه نشد، خوندن شکست‌خورده حساب می‌شه.
        if file_info.get("truncated"):
            raw = await self._fetch_raw(file_info.get("raw_url"))
            if raw is None:
                if allow_stale and self._cache:
                    logger.warning("⚠️ فایل truncated و raw_url خونده نشد - از کش قدیمی استفاده شد")
                    return self._cache.copy()
                logger.error("❌ alert_cache.json truncated است و raw_url خونده نشد")
                self._load_failed = True
                return {}
            content = raw

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
            status, _ = await self._http("PATCH", f"{self.api_url}/{self.gist_id}", payload)
            if status == 200:
                self._cache = data
                self._cache_time = time.time()
                return True
            logger.error(f"❌ Failed to save gist: {status}")
            return False

    # ------------------------------------------------------------------
    # پاکسازی داده‌ی قدیمی (روزهای هشدار + روزهای داخل _industry_universe)
    # ------------------------------------------------------------------
    @staticmethod
    def _prune_old_days(data: dict, keep_days: int = 1) -> None:
        """
        هر ساختاری که کلیدش تاریخ جلالی روزانه باشه رو به آخرین keep_days
        روز محدود می‌کنه - هم سطح بالای data (روزهای هشدار)، هم داخل
        _industry_universe (که قبلاً هیچ‌وقت پاک نمی‌شد و هر روز فقط
        بزرگ‌تر می‌شد). in-place تغییر می‌ده.
        """
        cutoff = (tehran_today() - jdatetime.timedelta(days=keep_days)).strftime("%Y-%m-%d")

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
        if self._load_failed:
            return True  # نمی‌تونیم مطمئن بشیم؛ از ارسال دوباره‌ی خلاصه جلوگیری می‌کنیم
        return data.get("_daily_summary_sent", {}).get(self.today_jalali, False)

    async def mark_today_summary_sent(self) -> bool:
        data = await self._load_gist_content(use_cache=False)
        if self._load_failed:
            logger.error("❌ قفل خلاصه‌ی روزانه ذخیره نشد: خوندن Gist شکست خورد")
            return False
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
        if self._load_failed:
            return False  # روی داده‌ی خالی بازنویسی نکن
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
        data = await self._load_gist_content(allow_stale=True)
        if self._load_failed:
            # بدون سابقه‌ی dedup نمی‌شه مطمئن بود؛ ارسال نکردن بهتر از اسپم تکراریه
            logger.warning(f"⚠️ {symbol}: سابقه‌ی dedup در دسترس نیست - ارسال رد شد")
            return False
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
                (symbol, alert_type, value, is_fund, industry_name, price_change_percent, sarane_kharid)

                sarane_kharid: سرانه خرید لحظه‌ی هشدار (فقط برای فیلتر ۱۹؛ بقیه None)
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
        if self._load_failed:
            logger.error("❌ علامت‌گذاری هشدارها انجام نشد: خوندن Gist شکست خورد")
            return False
        data.setdefault(self.today_jalali, [])

        # پاکسازی روزهای قدیمی (فقط امروز + دیروز نگه داشته می‌شه) - هم سطح بالا
        # هم داخل _industry_universe
        self._prune_old_days(data)

        existing = {(a["symbol"], a["alert_type"]) for a in data[self.today_jalali]}

        new_items = []
        for item in alerts:
            # پشتیبانی از فرمت‌های ۲ تا ۶ عضوی (ترتیب ثابت، از چپ اضافه می‌شن)
            is_fund = None
            industry_name = None
            price_change_percent = None
            sarane_kharid = None

            n = len(item)
            if n == 7:
                s, t, val, is_fund, industry_name, price_change_percent, sarane_kharid = item
            elif n == 6:
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
                if sarane_kharid is not None:
                    entry["sarane_kharid"] = sarane_kharid
                new_items.append(entry)

        if not new_items:
            return True

        data[self.today_jalali].extend(new_items)
        return await self._save_to_gist(data)

    # ------------------------------------------------------------------
    # Queue Snapshot (فایل جدا: queue_snapshot.json)
    # ------------------------------------------------------------------
    def _empty_queue_snapshot(self) -> dict:
        return {"date": self.today_jalali, "updated": None, "buy": {}, "sell": {}}

    async def _get_queue_gist(self):
        """(status, gist) با کش ۳۰ ثانیه‌ای؛ فقط پاسخ موفق کش می‌شه."""
        now = time.time()
        if self._queue_gist_cache is not None and now - self._queue_gist_cache_time < self._queue_gist_cache_ttl:
            return 200, self._queue_gist_cache
        status, gist = await self._http(
            "GET", f"{self.api_url}/{self.queue_gist_id}", headers=self.queue_headers,
            timeout=SARANE_HTTP_TIMEOUT,
        )
        if status == 200:
            self._queue_gist_cache = gist
            self._queue_gist_cache_time = time.time()
        return status, gist

    def _invalidate_queue_gist_cache(self) -> None:
        self._queue_gist_cache = None

    async def load_queue_snapshot(self) -> Optional[dict]:
        """
        اسنپ‌شات امروز صف‌ها. اگه فایل نباشه یا مال روز قبل باشه، اسنپ‌شات
        خالیِ امروز برمی‌گرده. اگه خوندن Gist شکست بخوره None برمی‌گرده تا
        فراخوان این دور رو بی‌صدا رد کنه (نه اینکه با اسنپ‌شات خالی بازنویسی کنه).
        """
        if not self.queue_gist_enabled:
            return None

        status, gist = await self._get_queue_gist()
        if status != 200:
            logger.error(f"❌ Failed to load queue snapshot: {status}")
            return None

        file_info = (gist.get("files") or {}).get(QUEUE_SNAPSHOT_FILE)
        if not file_info:
            return self._empty_queue_snapshot()

        try:
            data = json.loads(file_info.get("content") or "")
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON خراب در queue_snapshot - ریست می‌شود: {e}")
            return self._empty_queue_snapshot()

        if not isinstance(data, dict) or data.get("date") != self.today_jalali:
            return self._empty_queue_snapshot()

        data.setdefault("buy", {})
        data.setdefault("sell", {})
        return data

    async def save_queue_snapshot(self, snapshot: dict) -> bool:
        if not self.queue_gist_enabled:
            return False
        payload = {
            "files": {
                QUEUE_SNAPSHOT_FILE: {
                    "content": json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
                }
            }
        }
        async with self._lock:
            status, _ = await self._http(
                "PATCH", f"{self.api_url}/{self.queue_gist_id}", payload, headers=self.queue_headers
            )
            self._invalidate_queue_gist_cache()
            if status == 200:
                return True
            logger.error(f"❌ Failed to save queue snapshot: {status}")
            return False

    # ------------------------------------------------------------------
    # Sarane History (فایل جدا تو همون Gist صف‌ها: sarane_history.json)
    # ------------------------------------------------------------------
    async def load_sarane_history(self) -> Optional[dict]:
        """
        تاریخچه‌ی خام سرانه خرید. اگه فایل نباشه یا خالی/خراب باشه، {} برمی‌گرده
        (فراخوان با normalize_history کامل‌ش می‌کنه). اگه خوندن Gist شکست بخوره
        None برمی‌گرده تا فراخوان فیلتر رو رد کنه و روی تاریخچه بازنویسی نکنه.
        """
        if not self.queue_gist_enabled:
            return None

        status, gist = await self._get_queue_gist()
        if status != 200:
            logger.error(f"❌ Failed to load sarane history: {status}")
            return None

        file_info = (gist.get("files") or {}).get(SARANE_HISTORY_FILE)
        if not file_info:
            return {}

        content = file_info.get("content") or ""
        if file_info.get("truncated"):
            raw = await self._fetch_raw(file_info.get("raw_url"))
            if raw is None:
                logger.error("❌ sarane_history truncated است و raw_url خونده نشد")
                return None
            content = raw

        if not content.strip():
            return {}
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            # ریست نمی‌کنیم تا تاریخچه‌ی قابل‌بازیابی بی‌صدا پاک نشه؛ این دور رد می‌شه
            logger.error(f"❌ JSON خراب در sarane_history - این دور رد شد: {e}")
            return None
        return data if isinstance(data, dict) else {}

    async def save_sarane_history(self, history: dict) -> bool:
        if not self.queue_gist_enabled:
            return False
        payload = {
            "files": {
                SARANE_HISTORY_FILE: {
                    "content": json.dumps(history, ensure_ascii=False, separators=(",", ":"))
                }
            }
        }
        async with self._lock:
            status, _ = await self._http(
                "PATCH", f"{self.api_url}/{self.queue_gist_id}", payload, headers=self.queue_headers,
                timeout=SARANE_HTTP_TIMEOUT,
            )
            self._invalidate_queue_gist_cache()
            if status == 200:
                return True
            logger.error(f"❌ Failed to save sarane history: {status}")
            return False

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
