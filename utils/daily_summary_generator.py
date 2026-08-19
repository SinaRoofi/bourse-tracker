"""
ماژول تولید گزارش خلاصه روزانه
تحلیل نمادهای پرتکرار از هشدارهای ثبت شده در Gist
+ ارسال Top-5 نمادهای برتر هر فیلتر
+ ارسال برترین صنایع بر اساس تعداد نمادهای فعال هر صنعت
"""

import logging
from datetime import datetime
import jdatetime
import pytz
from typing import Dict, List, Optional

from utils.gist_alert_manager import WATCHLIST_COPY_SUFFIX

logger = logging.getLogger(__name__)

TEHRAN_TZ = pytz.timezone("Asia/Tehran")

# مدال برای سه رتبه‌ی اول نمادهای پرتکرار (زیباتر از عدد خشک)
RANK_MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}

# عنوان فارسی و واحد هر فیلتر برای نمایش در پیام
FILTER_META = {
    "filter_1_strong_buying": {
        "title": "قدرت خرید قوی",
        "emoji": "💪",
        "unit": "",
        "format": ".2f",
    },
    "filter_2_sarane_cross": {
        "title": "کراس سرانه خرید",
        "emoji": "📈",
        "unit": "M",
        "format": ".0f",
    },
    "filter_5_pol_hagigi_ratio": {
        "title": "ورود پول حقیقی قوی",
        "emoji": "💎",
        "unit": "%",
        "format": ".0f",
        "multiplier": 100,
    },
    "filter_7_suspicious_volume": {
        "title": "حجم مشکوک",
        "emoji": "🔍",
        "unit": "%",
        "format": ".0f",
        "multiplier": 100,
    },
    "filter_10_heavy_buy_queue": {
        "title": "صف خرید با اردر سنگین",
        "emoji": "💰",
        "unit": "B",
        "format": ".2f",
    },
    "filter_11_hoghooghi_haghighi_strong_buy": {
        "title": "خرید حقوقی و حقیقی قوی",
        "emoji": "🏦",
        "unit": "M",
        "format": ".0f",
    },
}


class DailySummaryGenerator:
    """کلاس تولید و ارسال گزارش خلاصه روزانه"""

    def __init__(self, alert_manager, telegram_alert):
        self.alert_manager = alert_manager
        self.telegram = telegram_alert
        self.today_jalali = jdatetime.date.today().strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # نمادهای پرتکرار
    # ------------------------------------------------------------------
    @staticmethod
    def _count_unique_signals(today_alerts: List[dict]) -> Dict[str, int]:
        """
        شمارش تعداد سیگنال‌های یکتای هر نماد در امروز — هر (نماد، فیلتر واقعی)
        فقط یک‌بار حساب می‌شه، حتی اگه به‌خاطر واچ‌لیست شخصی کپی هم داشته
        باشه (پسوند WATCHLIST_COPY_SUFFIX حذف می‌شه). هم برای «نمادهای
        پرتکرار» و هم برای رتبه‌بندی preview نمادهای هر صنعت استفاده می‌شه.
        """
        seen_signals = set()
        symbol_count: Dict[str, int] = {}
        for alert in today_alerts:
            symbol = alert.get("symbol")
            alert_type = alert.get("alert_type", "")
            if not symbol:
                continue
            base_alert_type = alert_type.removesuffix(WATCHLIST_COPY_SUFFIX)
            signal = (symbol, base_alert_type)
            if signal in seen_signals:
                continue
            seen_signals.add(signal)
            symbol_count[symbol] = symbol_count.get(symbol, 0) + 1
        return symbol_count

    def get_frequent_symbols(
        self, data: dict, min_count: int = 3, top_n: int = None
    ) -> Dict[str, int]:
        """
        محاسبه‌ی نمادهای پرتکرار از داده‌ی از قبل لود شده‌ی Gist.
        `data` باید همون دیکشنری کامل خروجی `_load_gist_content` باشه —
        این متد دیگه خودش I/O انجام نمی‌ده (به فراخوان سپرده شده).
        """
        today_alerts = data.get(self.today_jalali, [])

        if not today_alerts:
            logger.warning("⚠️ هیچ هشداری برای امروز یافت نشد")
            return {}

        logger.info(f"✅ {len(today_alerts)} هشدار یافت شد")

        symbol_count = self._count_unique_signals(today_alerts)

        frequent_symbols = {
            symbol: count
            for symbol, count in symbol_count.items()
            if count >= min_count
        }

        if not frequent_symbols:
            logger.info(f"ℹ️ هیچ نمادی بیش از {min_count} بار تکرار نشده")
            return {}

        sorted_symbols = sorted(frequent_symbols.items(), key=lambda x: x[1], reverse=True)

        if top_n is not None:
            sorted_symbols = sorted_symbols[:top_n]

        logger.info(f"🎯 {len(sorted_symbols)} نماد پرتکرار یافت شد")
        return dict(sorted_symbols)

    # ------------------------------------------------------------------
    # Top-N هر فیلتر
    # ------------------------------------------------------------------
    def get_top_symbols_per_filter(self, data: dict, top_n: int = 5) -> Dict[str, List[dict]]:
        """
        دریافت top_n نماد برتر برای هر فیلتر بر اساس value ذخیره‌شده در Gist.

        dedup: برای هر نماد، آخرین رکورد ثبت‌شده‌ش در همون فیلتر نگه داشته
        می‌شه (نه بیشترین value) — چون آخرین alert نشون‌دهنده‌ی جدیدترین
        وضعیت نماده، نه لزوماً بهترین لحظه‌ی روز.

        Returns:
            dict: {filter_name: [{"symbol":..., "value":..., "price_change_percent":...}, ...]}
        """
        today_alerts = data.get(self.today_jalali, [])

        if not today_alerts:
            return {}

        # گروه‌بندی بر اساس filter_name — فقط فیلترهایی که در FILTER_META هستن
        filter_groups: Dict[str, List[dict]] = {}
        for alert in today_alerts:
            filter_name = alert.get("alert_type")
            if filter_name not in FILTER_META:
                continue
            if "value" not in alert or alert["value"] is None:
                continue
            if alert.get("is_fund"):  # فقط سهام - صندوق‌ها حذف می‌شن
                continue
            filter_groups.setdefault(filter_name, []).append(alert)

        result = {}
        for filter_name, items in filter_groups.items():
            # آخرین entry هر نماد (ترتیب Gist = ترتیب زمانی ثبت)
            last_per_symbol = {}
            for item in items:
                last_per_symbol[item["symbol"]] = item
            sorted_items = sorted(last_per_symbol.values(), key=lambda x: x["value"], reverse=True)
            result[filter_name] = sorted_items[:top_n]

        logger.info(f"🏆 Top-{top_n} فیلترها: {list(result.keys())}")
        return result

    # ------------------------------------------------------------------
    # برترین صنایع
    # ------------------------------------------------------------------
    def get_top_industries(
        self,
        data: dict,
        industry_universe: Optional[dict] = None,
        top_n: int = 5,
        symbols_preview: int = 4,
    ) -> List[dict]:
        """
        صنایعی که امروز بیشترین «هشدار» (فعالیت) رو داشتن.

        معیار رتبه‌بندی: total_alert_count = مجموع تعداد کل هشدارهای
        ثبت‌شده امروز برای اون صنعت (نه تعداد نماد یکتا). یعنی نمادی که
        امروز ۵ بار توی فیلترهای مختلف هشدار گرفته، ۵ برابر نمادی که فقط
        ۱ بار هشدار گرفته وزن داره — برخلاف قبل که فقط «حضور حداقل یه‌بار»
        حساب می‌شد و صنایع کوچیک با چندتا نماد کم‌اهمیت هم مصنوعی بالا
        می‌اومدن.

        حداقل ۲ نماد یکتای هشداردهنده (alerted_count >= 2) هم لازمه تا
        صنعت وارد رتبه‌بندی بشه — جلوی صنایعی رو می‌گیره که کل «داغی»شون
        فقط از یه نماد تک‌رقمی میاد.

        preview نمادها (نمادهای داغ صنعت): به‌جای الفبایی، بر اساس تعداد
        سیگنال هر نماد در کل بازار امروز (از _count_unique_signals) مرتب
        می‌شه — یعنی نمادهایی که در چند فیلتر هم‌زمان alert گرفتن (سیگنال
        قوی‌تر) اول preview میان.

        صندوق‌ها (is_fund=True) و رکوردهای بدون industry_name کنار گذاشته
        می‌شن.

        Returns:
            list: [{"industry_name", "symbol_count", "universe_count",
                     "participation_pct", "total_alert_count",
                     "symbols": [...]}]
                  مرتب‌شده نزولی بر اساس total_alert_count
        """
        today_alerts = data.get(self.today_jalali, [])
        if not today_alerts:
            return []

        signal_counts = self._count_unique_signals(today_alerts)

        industry_symbols: Dict[str, set] = {}
        industry_alert_counts: Dict[str, int] = {}
        for alert in today_alerts:
            if alert.get("is_fund"):
                continue
            industry_name = alert.get("industry_name")
            symbol = alert.get("symbol")
            if not industry_name or not symbol:
                continue
            industry_symbols.setdefault(industry_name, set()).add(symbol)
            industry_alert_counts[industry_name] = industry_alert_counts.get(industry_name, 0) + 1

        if not industry_symbols:
            return []

        industry_universe = industry_universe or {}
        MIN_ALERTED_SYMBOLS = 2  # حداقل نماد یکتای هشداردهنده برای ورود به رتبه‌بندی

        rows = []
        for industry_name, symbols in industry_symbols.items():
            alerted_count = len(symbols)
            if alerted_count < MIN_ALERTED_SYMBOLS:
                continue
            universe_count = industry_universe.get(industry_name)
            participation_pct = (
                (alerted_count / universe_count * 100) if universe_count else None
            )
            preview_symbols = sorted(
                symbols, key=lambda s: (-signal_counts.get(s, 0), s)
            )[:symbols_preview]
            rows.append({
                "industry_name": industry_name,
                "symbol_count": alerted_count,
                "universe_count": universe_count,
                "participation_pct": participation_pct,
                "total_alert_count": industry_alert_counts[industry_name],
                "symbols": preview_symbols,
            })

        result = sorted(
            rows, key=lambda r: r["total_alert_count"], reverse=True
        )[:top_n]

        logger.info(f"🏭 برترین صنایع: {[r['industry_name'] for r in result]}")
        return result

    # ------------------------------------------------------------------
    # فرمت پیام نمادهای پرتکرار
    # ------------------------------------------------------------------
    def format_summary_message(
        self,
        frequent_symbols: Dict[str, int],
        total_unique_symbols: int
    ) -> str:
        date_str, time_str = self._get_tehran_datetime()

        message = "📊 <b>خلاصه هشدارهای امروز</b>\n\n"

        if frequent_symbols:
            count_groups = {}
            for symbol, count in frequent_symbols.items():
                count_groups.setdefault(count, []).append(symbol)

            for rank, count in enumerate(sorted(count_groups.keys(), reverse=True), 1):
                symbols_list = sorted(count_groups[count])
                hashtags = " ".join([f"#{self._format_symbol_hashtag(s)}" for s in symbols_list])
                prefix = RANK_MEDALS.get(rank, "▪️")
                message += f"{prefix} <b>({count}×)</b> {hashtags}\n"
        else:
            message += "هیچ نماد پرتکراری نبود\n"

        message += f"\n🎯 {len(frequent_symbols)} نماد پرتکرار از {total_unique_symbols} نماد هشداردهنده\n\n"
        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.telegram.channel_name}"

        return message

    # ------------------------------------------------------------------
    # فرمت پیام Top-N فیلترها
    # ------------------------------------------------------------------
    def format_top_filter_message(self, top_per_filter: Dict[str, List[dict]]) -> str:
        """
        فرمت پیام Top-N نمادهای برتر هر فیلتر، همراه با درصد تغییر قیمت
        پایانی نماد (نه قیمت آخر) کنار هر ردیف — برای زمینه‌ی سریع‌تر.

        Returns:
            str: پیام آماده برای ارسال به تلگرام
        """
        if not top_per_filter:
            return ""

        date_str, time_str = self._get_tehran_datetime()

        message = "🏆 <b>برترین نمادها — امروز</b>\n\n"

        for filter_name, items in top_per_filter.items():
            meta = FILTER_META.get(filter_name, {})
            emoji = meta.get("emoji", "📌")
            title = meta.get("title", filter_name)
            unit = meta.get("unit", "")
            fmt = meta.get("format", ".2f")
            multiplier = meta.get("multiplier", 1)

            message += f"{emoji} <b>#{title.replace(' ', '_')}</b>\n"

            for i, item in enumerate(items, 1):
                symbol = self._format_symbol_hashtag(item["symbol"])
                raw_val = item["value"] * multiplier
                val_str = format(raw_val, fmt)
                unit_str = f" {unit}" if unit else ""
                price_prefix = self._format_price_change_prefix(item.get("price_change_percent"))
                message += f"  {i}. #{symbol} — {price_prefix}{val_str}{unit_str}\n"

            message += "\n"

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.telegram.channel_name}"

        return message

    # ------------------------------------------------------------------
    # فرمت پیام برترین صنایع
    # ------------------------------------------------------------------
    def format_top_industries_message(self, top_industries: List[dict]) -> str:
        """
        فرمت پیام برترین صنایع — هم‌الگو با پیام Top-N فیلترها (لیست
        شماره‌دار)، بدون خط جداکننده، فوتر هم‌شکل با بقیه‌ی پیام‌ها.

        نمایش: «۵۸ هشدار (۳۶/۵۱ نماد، ۷۱٪)» — تعداد کل هشدار (معیار
        رتبه‌بندی) همراه با تعداد نماد یکتا و درصد مشارکت به‌عنوان اطلاعات
        تکمیلی. اگه درصد مشارکت موجود نباشه (fallback، مثلاً روز اول قبل
        از ذخیره‌شدن universe): «۵۸ هشدار (۳۶ نماد)»
        """
        if not top_industries:
            return ""

        date_str, time_str = self._get_tehran_datetime()

        message = "🏭 <b>برترین صنایع امروز</b>\n\n"

        for i, industry in enumerate(top_industries, 1):
            name = industry["industry_name"].replace(" ", "_")
            count = industry["symbol_count"]
            universe_count = industry.get("universe_count")
            participation_pct = industry.get("participation_pct")
            total_alert_count = industry.get("total_alert_count", 0)

            if participation_pct is not None and universe_count:
                count_str = f"{total_alert_count} هشدار ({count}/{universe_count} نماد، {participation_pct:.0f}٪)"
            else:
                count_str = f"{total_alert_count} هشدار ({count} نماد)"

            hashtags = " ".join(
                f"#{self._format_symbol_hashtag(s)}" for s in industry["symbols"]
            )
            message += f"{i}. {name} — {count_str}\n"
            if hashtags:
                message += f"   {hashtags}\n"
            message += "\n"

        message += f"📅 {date_str} | 🕐 {time_str}\n"
        message += f"📢 {self.telegram.channel_name}"

        return message

    # ------------------------------------------------------------------
    # Utils
    # ------------------------------------------------------------------
    @staticmethod
    def _format_symbol_hashtag(symbol: str) -> str:
        if not symbol:
            return ""
        return str(symbol).replace(' ', '_').replace('\u200c', '_').strip()

    @staticmethod
    def _format_price_change_prefix(price_change_percent: Optional[float]) -> str:
        """تبدیل درصد تغییر قیمت پایانی به پیشوند نمایشی '▲2.9% | ' یا '▼1.0% | '،
        که قبل از مقدار خودِ فیلتر میاد (مثلاً '▲2.9% | 1035 M'). درصد قیمت
        عمداً اول میاد چون سیگنال مهم‌تری برای نگاه اول کاربره؛ مقدار فیلتر
        بعدش با یه '|' از هم جدا می‌شه تا با عدد سمت راستش قاطی نشه."""
        if price_change_percent is None:
            return ""
        arrow = "▲" if price_change_percent >= 0 else "▼"
        return f"{arrow}{abs(price_change_percent):.1f}% | "

    @staticmethod
    def _get_tehran_datetime() -> tuple:
        now = datetime.now(TEHRAN_TZ)
        jnow = jdatetime.datetime.fromgregorian(datetime=now.replace(tzinfo=None))
        date_str = jnow.strftime("%Y/%m/%d")
        time_str = now.strftime("%H:%M")
        return date_str, time_str

    # ------------------------------------------------------------------
    # تولید و ارسال — هر سه پیام
    # ------------------------------------------------------------------
    async def generate_and_send(self, min_count: int = 3, top_n: int = None) -> bool:
        """
        تولید و ارسال سه پیام:
          ۱. خلاصه نمادهای پرتکرار
          ۲. Top-N برترین نمادهای هر فیلتر
          ۳. برترین صنایع امروز

        داده‌ی Gist فقط یک‌بار در ابتدا لود می‌شه و بین هر سه محاسبه به
        اشتراک گذاشته می‌شه (قبلاً هر متد جدا لود می‌کرد).

        Returns:
            bool: True اگر همه‌ی پیام‌های قابل‌ارسال موفق باشند
        """
        try:
            data = await self.alert_manager._load_gist_content()
            today_alerts = data.get(self.today_jalali, [])
            total_unique_symbols = len(set(
                alert["symbol"] for alert in today_alerts if alert.get("symbol")
            ))

            # پیام ۱: نمادهای پرتکرار
            frequent_symbols = self.get_frequent_symbols(data, min_count, top_n)
            message1 = self.format_summary_message(frequent_symbols, total_unique_symbols)

            logger.info("📤 ارسال پیام خلاصه نمادهای پرتکرار...")
            success1 = await self.telegram.send_message(message1, parse_mode='HTML')

            if success1:
                logger.info("✅ پیام خلاصه ارسال شد")
            else:
                logger.error("❌ خطا در ارسال پیام خلاصه")

            # پیام ۲: Top-5 هر فیلتر
            top_per_filter = self.get_top_symbols_per_filter(data, top_n=5)
            message2 = self.format_top_filter_message(top_per_filter)

            success2 = True
            if message2:
                logger.info("📤 ارسال پیام Top-5 فیلترها...")
                success2 = await self.telegram.send_message(message2, parse_mode='HTML')
                if success2:
                    logger.info("✅ پیام Top-5 ارسال شد")
                else:
                    logger.error("❌ خطا در ارسال پیام Top-5")
            else:
                logger.info("ℹ️ داده‌ای برای Top-5 موجود نیست")

            # پیام ۳: برترین صنایع
            industry_universe = await self.alert_manager.get_industry_universe()
            top_industries = self.get_top_industries(data, industry_universe, top_n=5)
            message3 = self.format_top_industries_message(top_industries)

            success3 = True
            if message3:
                logger.info("📤 ارسال پیام برترین صنایع...")
                success3 = await self.telegram.send_message(message3, parse_mode='HTML')
                if success3:
                    logger.info("✅ پیام برترین صنایع ارسال شد")
                else:
                    logger.error("❌ خطا در ارسال پیام برترین صنایع")
            else:
                logger.info("ℹ️ داده‌ای برای برترین صنایع موجود نیست")

            return success1 and success2 and success3

        except Exception as e:
            logger.error(f"❌ خطا در تولید گزارش خلاصه: {e}", exc_info=True)
            return False
