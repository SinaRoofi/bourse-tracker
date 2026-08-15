import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class BourseDataProcessor:
    """کلاس پردازش و اعمال فیلترها بر روی داده‌های بورس"""

    def __init__(self):
        self.filters_results = {}
        self.failed_filters: List[str] = []

    # ========================================
    # پردازش داده‌های خام
    # ========================================

    def process_all_data(self, df_raw: pd.DataFrame) -> pd.DataFrame:

        logger.info("شروع پردازش داده‌های خام...")

        if df_raw is not None and not df_raw.empty:
            df = self._clean_and_prepare_api1(df_raw)
            logger.info(f"✅ {len(df)} سهم/صندوق پردازش شد")
        else:
            df = pd.DataFrame()
            logger.warning("⚠️ داده‌ی خام خالی است")

        return df

    def _clean_and_prepare_api1(self, df: pd.DataFrame) -> pd.DataFrame:
        """پاکسازی و آماده‌سازی داده‌های API اول"""
        # حذف ردیف‌های نال
        if "symbol" in df.columns:
            df = df.dropna(subset=["symbol"])

        # تبدیل ستون‌های عددی از string به numeric
        numeric_columns = [
            "volume",
            "value",
            "first_price",
            "first_price_change_percent",
            "high_price",
            "high_price_change_percent",
            "low_price",
            "low_price_change_percent",
            "last_price",
            "last_price_change_percent",
            "final_price",
            "final_price_change_percent",
            "diff_last_final",
            "volatility",
            "sarane_kharid",
            "sarane_forosh",
            "godrat_kharid",
            "pol_hagigi",
            "buy_order_value",
            "sell_order_value",
            "diff_buy_sell_order",
            "avg_5_day_pol_hagigi",
            "avg_20_day_pol_hagigi",
            "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi",
            "20_day_pol_hagigi",
            "60_day_pol_hagigi",
            "5_day_godrat_kharid",
            "20_day_godrat_kharid",
            "avg_monthly_value",
            "value_to_avg_monthly_value",
            "avg_3_month_value",
            "value_to_avg_3_month_value",
            "avg_5_day_value",
            "5_day_return",
            "20_day_return",
            "60_day_return",
            "marketcap",
            "value_to_marketcap",
            # فیلدهای جدید (endpoint snapshot)
            "bubble_percent",
            "avg_1_month_bubble",
            "buy_order",
            "buy_queue_value",
            "ceiling_price",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("✅ تبدیل ستون‌های عددی API اول انجام شد")

        # تقسیم ستون‌ها به 10 میلیون (ریال -> میلیون تومان)
        columns_to_divide = ["sarane_kharid", "sarane_forosh", "buy_order"]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = df[col] / 10_000_000

        logger.info("✅ تقسیم ستون‌ها به 10 میلیون انجام شد")

        # تقسیم ستون‌ها به 10 میلیارد (ریال -> میلیارد تومان)
        columns_to_divide = [
            "value",
            "pol_hagigi",
            "buy_order_value",
            "sell_order_value",
            "diff_buy_sell_order",
            "avg_5_day_pol_hagigi",
            "avg_20_day_pol_hagigi",
            "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi",
            "20_day_pol_hagigi",
            "60_day_pol_hagigi",
            "avg_monthly_value",
            "avg_3_month_value",
            "marketcap",
            "buy_queue_value",
            "avg_5_day_value",
        ]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = df[col] / 10_000_000_000

        logger.info("✅ تقسیم ستون‌ها به 10 میلیارد انجام شد")

        # محاسبه pol_hagigi_to_avg_monthly_value
        if all(col in df.columns for col in ["pol_hagigi", "avg_monthly_value"]):
            df["pol_hagigi_to_avg_monthly_value"] = df.apply(
                lambda row: (
                    row["pol_hagigi"] / row["avg_monthly_value"]
                    if row["avg_monthly_value"] != 0
                    and pd.notna(row["avg_monthly_value"])
                    else 0
                ),
                axis=1,
            )
            logger.info("✅ محاسبه pol_hagigi_to_avg_monthly_value انجام شد")
        else:
            logger.warning(
                "⚠️ ستون‌های pol_hagigi یا avg_monthly_value برای محاسبه نسبت یافت نشد"
            )
            df["pol_hagigi_to_avg_monthly_value"] = 0

        # محاسبه value_5_to_20_ratio (میانگین ارزش معاملات 5 روزه نسبت به 20 روزه)
        if all(col in df.columns for col in ["avg_5_day_value", "avg_monthly_value"]):
            df["value_5_to_20_ratio"] = df.apply(
                lambda row: (
                    row["avg_5_day_value"] / row["avg_monthly_value"]
                    if row["avg_monthly_value"] != 0
                    and pd.notna(row["avg_monthly_value"])
                    else 0
                ),
                axis=1,
            )
            logger.info("✅ محاسبه value_5_to_20_ratio انجام شد")
        else:
            logger.warning(
                "⚠️ ستون‌های avg_5_day_value یا avg_monthly_value برای محاسبه نسبت یافت نشد"
            )
            df["value_5_to_20_ratio"] = 0

        return df

    # ========================================
    # فیلتر 1: قدرت خرید قوی
    # ========================================
    def filter_1_strong_buying_power(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        from config import STRONG_BUYING_CONFIG

        config = STRONG_BUYING_CONFIG
        logger.info("اعمال فیلتر 1: قدرت خرید قوی")

        mask = (
            (df["value_to_avg_monthly_value"] > config["min_value_to_avg_monthly"])
            & (df["sarane_kharid"] > config["min_sarane_kharid"])
            & (df["godrat_kharid"] > config["min_godrat_kharid"])
        )

        if config.get("godrat_greater_than_5day", True):
            multiplier = config.get("godrat_5day_multiplier", 2)
            logger.info(f"  • شرط اضافه: قدرت خرید > {multiplier} × میانگین 5 روزه")
            mask &= df["godrat_kharid"] > multiplier * df["5_day_godrat_kharid"]

        filtered = df[mask].copy()

        filtered = filtered.sort_values("sarane_kharid", ascending=False)
        logger.info(f"✅ فیلتر 1: {len(filtered)} سهم یافت شد")
        return filtered

    # ========================================
    # فیلتر 2: کراس سرانه خرید
    # ========================================
    def filter_2_sarane_kharid_cross(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        from config import SARANE_CROSS_CONFIG

        config = SARANE_CROSS_CONFIG
        logger.info("اعمال فیلتر 2: کراس سرانه خرید")

        filtered = df[
            (df["sarane_kharid"] > df["sarane_forosh"])
            & (df["value_to_avg_monthly_value"] >= config["min_value_to_avg_monthly"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
        ].copy()

        filtered = filtered.sort_values("sarane_kharid", ascending=False)
        logger.info(f"✅ فیلتر 2: {len(filtered)} سهم یافت شد")
        return filtered

    # ========================================
    # فیلتر 3: واچ‌لیست شخصی - عبور از آستانه‌ی واحد
    # ========================================
    def filter_3_watchlist_symbols(
        self, df: pd.DataFrame, watchlist: list = None, threshold: float = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if watchlist is None:
            from config import PERSONAL_WATCHLIST

            watchlist = PERSONAL_WATCHLIST

        if threshold is None:
            from config import PERSONAL_WATCHLIST_THRESHOLD

            threshold = PERSONAL_WATCHLIST_THRESHOLD

        if not watchlist:
            logger.warning("فیلتر 3: واچ‌لیست شخصی خالی است!")
            return pd.DataFrame()

        logger.info(
            f"اعمال فیلتر 3: بررسی {len(watchlist)} نماد واچ‌لیست (آستانه {threshold}%)"
        )

        filtered = df[
            df["symbol"].isin(watchlist)
            & (df["last_price_change_percent"] > threshold)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 3: هیچ نمادی از آستانه عبور نکرد")
            return pd.DataFrame()

        filtered["threshold"] = threshold
        filtered = filtered.sort_values("last_price_change_percent", ascending=False)
        logger.info(f"✅ فیلتر 3: {len(filtered)} نماد از آستانه عبور کرد")
        return filtered

    # ========================================
    # فیلتر 4: رنج مثبت
    # ========================================
    def  filter_4_range_mosbat(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if config is None:
            from config import range_mosbat

            config = range_mosbat

        logger.info("اعمال فیلتر 4: رنج مثبت")

        filtered = df[
            (df["diff_last_final"] >= config["tick_diff_percent"])
            & (df["value_to_avg_monthly_value"] >= config["min_value_to_avg_monthly"])
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 4: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("diff_last_final", ascending=False)
        logger.info(f"✅ فیلتر 4: {len(filtered)} سهم با رنج مثبت ")
        return filtered

    # ========================================
    # فیلتر 5: نسبت پول حقیقی
    # ========================================
    def filter_5_pol_hagigi_ratio(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if config is None:
            from config import POL_HAGIGI_FILTER_CONFIG

            config = POL_HAGIGI_FILTER_CONFIG

        logger.info("اعمال فیلتر 5: نسبت پول حقیقی")

        filtered = df[
            (df["pol_hagigi_to_avg_monthly_value"] >= config["min_pol_to_value_ratio"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
            & (df["godrat_kharid"] >= config["min_godrat_kharid"])
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 5: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values(
            "pol_hagigi_to_avg_monthly_value", ascending=False
        )
        logger.info(f"✅ فیلتر 5: {len(filtered)} سهم با نسبت پول حقیقی بالا")
        return filtered

    # ========================================
    # فیلتر 6: تیک و ساعت
    # ========================================
    def filter_6_tick_and_time(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if config is None:
            from config import TICK_FILTER_CONFIG

            config = TICK_FILTER_CONFIG

        first_to_low_ratio = config.get("first_to_low_ratio", 0.98)
        last_to_first_ratio = config.get("last_to_first_ratio", 0.98)
        tick_diff_percent = config.get("tick_diff_percent", 2.0)

        logger.info("اعمال فیلتر 6: تیک و ساعت")

        df_copy = df.copy()
        df_copy["tick_diff"] = df_copy["diff_last_final"]

        filtered = df_copy[
            (first_to_low_ratio * df_copy["first_price"] > df_copy["low_price"])
            & (last_to_first_ratio * df_copy["last_price"] > df_copy["first_price"])
            & (df_copy["tick_diff"] > tick_diff_percent)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 6: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("tick_diff", ascending=False)
        logger.info(f"✅ فیلتر 6: {len(filtered)} سهم با تیک مثبت در آخر روز")
        return filtered

    # ========================================
    # فیلتر 7: حجم مشکوک
    # ========================================
    def filter_7_suspicious_volume(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if config is None:
            from config import SUSPICIOUS_VOLUME_CONFIG

            config = SUSPICIOUS_VOLUME_CONFIG

        min_ratio = config.get("min_value_to_avg_ratio", 2.0)
        logger.info(f"اعمال فیلتر 7: حجم مشکوک (آستانه: {min_ratio}x)")

        filtered = df[df["value_to_avg_monthly_value"] > min_ratio].copy()

        if filtered.empty:
            logger.info("فیلتر 7: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("value_to_avg_monthly_value", ascending=False)
        logger.info(f"✅ فیلتر 7: {len(filtered)} سهم با حجم مشکوک")
        return filtered

    # ========================================
    # فیلتر 8: نوسان‌گیری
    # ========================================
    def filter_8_swing_trade(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if config is None:
            from config import SWING_TRADE_CONFIG

            config = SWING_TRADE_CONFIG

        logger.info("اعمال فیلتر 8: نوسان‌گیری")

        filtered = df[
            (df["low_price_change_percent"] <= config["min_allowed_price"])
            & (df["last_price_change_percent"] >= config["min_allowed_price"])
            & (df["godrat_kharid"] >= config["min_godrat_kharid"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
            & (df["value_to_avg_monthly_value"] >= config["min_value_to_avg_monthly"])
            & (df["last_price_change_percent"] < config["max_last_change_percent"])
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 8: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("godrat_kharid", ascending=False)
        logger.info(f"✅ فیلتر 8: {len(filtered)} سهم برای نوسان‌گیری")
        return filtered

    # ========================================
    # فیلتر 9: یک ساعت اول
    # ========================================
    def filter_9_first_hour(
        self,
        df: pd.DataFrame,
        config: dict = None,
        current_hour: int = None,
        current_minute: int = None,
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if current_hour is None or current_minute is None:
            from datetime import datetime
            import pytz

            tehran_tz = pytz.timezone("Asia/Tehran")
            now_tehran = datetime.now(tehran_tz)
            current_hour = now_tehran.hour
            current_minute = now_tehran.minute

        if config is None:
            from config import FIRST_HOUR_CONFIG

            config = FIRST_HOUR_CONFIG

        start_hour = config.get("start_hour", 9)
        start_minute = config.get("start_minute", 0)
        end_hour = config.get("end_hour", 9)
        end_minute = config.get("end_minute", 30)
        min_ratio = config.get("min_value_to_avg_ratio", 1.0)

        current_total_minutes = current_hour * 60 + current_minute
        start_total_minutes = start_hour * 60 + start_minute
        end_total_minutes = end_hour * 60 + end_minute

        if not (start_total_minutes <= current_total_minutes < end_total_minutes):
            logger.info(
                f"فیلتر 9: خارج از بازه زمانی ({start_hour:02d}:{start_minute:02d}-"
                f"{end_hour:02d}:{end_minute:02d}). زمان فعلی: {current_hour:02d}:{current_minute:02d}"
            )
            return pd.DataFrame()

        logger.info(
            f"اعمال فیلتر 9: نیم ساعت اول (زمان تهران: {current_hour:02d}:{current_minute:02d})"
        )

        filtered = df[df["value_to_avg_monthly_value"] >= min_ratio].copy()

        if filtered.empty:
            logger.info("فیلتر 9: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("value_to_avg_monthly_value", ascending=False)
        logger.info(f"✅ فیلتر 9: {len(filtered)} سهم در ساعت اول")
        return filtered

    # ========================================
    # فیلتر 10: صف خرید میلیاردی (API دوم + غنی‌سازی با API اول)
    # ========================================
    def filter_10_heavy_buy_queue(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:
        """
        صف خرید میلیاردی - قبلاً از API دوم (BrsApi) + غنی‌سازی از API اول
        می‌اومد. الان buy_order/buy_queue_value/ceiling_price مستقیم از همون
        endpoint یکپارچه (سطح ۱ صف سفارش + سقف قیمت) میان، پس نیازی به
        merge بین دو منبع نیست.
        """
        if df.empty:
            return df

        if config is None:
            from config import HEAVY_BUY_QUEUE_CONFIG

            config = HEAVY_BUY_QUEUE_CONFIG

        logger.info("اعمال فیلتر 10: صف خرید میلیاردی")
        if config.get("price_at_ceiling", True):
            logger.info("  • شرط 1: آخرین قیمت = سقف")
        logger.info(f"  • شرط 2: buy_order >= {config['min_buy_order']} میلیون تومان")
        logger.info(
            f"  • شرط 3: buy_queue_value >= {config['min_buy_queue_value']} میلیارد تومان"
        )

        required_cols = ["last_price", "ceiling_price", "buy_order", "buy_queue_value"]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(f"❌ ستون‌های گمشده برای فیلتر 10: {missing_cols}")
            return pd.DataFrame()

        mask = (df["buy_order"] >= config["min_buy_order"]) & (
            df["buy_queue_value"] >= config["min_buy_queue_value"]
        )

        if config.get("price_at_ceiling", True):
            # ceiling_price فقط برای سهام موجوده (صندوق‌ها سقف قیمت ندارن)؛
            # مقایسه با NaN به‌طور طبیعی False می‌شه و صندوق‌ها از این فیلتر رد می‌شن.
            mask &= df["last_price"] == df["ceiling_price"]

        filtered = df[mask].copy()

        if filtered.empty:
            logger.info("فیلتر 10: هیچ نمادی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values("buy_queue_value", ascending=False)
        logger.info(f"✅ فیلتر 10: {len(filtered)} نماد با صف خرید میلیاردی")
        return filtered

    # ========================================
    # فیلتر 11: خرید حقوقی و حقیقی قوی
    # ========================================
    def filter_11_hoghooghi_haghighi_strong_buy(
        self, df: pd.DataFrame, config: dict = None
    ) -> pd.DataFrame:

        if df.empty:
            return df

        if config is None:
            from config import HOGHOOGHI_HAGHIGHI_STRONG_BUY_CONFIG

            config = HOGHOOGHI_HAGHIGHI_STRONG_BUY_CONFIG

        logger.info("اعمال فیلتر 11: خرید حقوقی و حقیقی قوی")
        logger.info(
            f"  • شرط 1: pol_hagigi_to_avg_monthly_value <= {config['max_pol_hagigi_to_value']} (خروج پول حقیقی)"
        )
        logger.info(
            f"  • شرط 2: last_price_change_percent > {config['min_last_price_change_percent']}% (قیمت مثبت)"
        )
        logger.info(
            f"  • شرط 3: sarane_kharid > {config['min_sarane_kharid']} میلیون تومان"
        )
        logger.info("  • شرط 4: sarane_kharid > sarane_forosh")

        # بررسی وجود ستون‌های لازم
        required_cols = [
            "pol_hagigi_to_avg_monthly_value",
            "last_price_change_percent",
            "sarane_kharid",
            "sarane_forosh",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.error(f"❌ ستون‌های گمشده در فیلتر 11: {missing_cols}")
            return pd.DataFrame()

        # اعمال فیلترها
        filtered = df[
            (
                df["pol_hagigi_to_avg_monthly_value"] <= config["max_pol_hagigi_to_value"]
            )  # خروج پول حقیقی
            & (df["pol_hagigi_to_avg_monthly_value"] < 0)  # فقط منفی (نه مثبت)
            & (
                df["last_price_change_percent"]
                > config["min_last_price_change_percent"]
            )  # قیمت مثبت
            & (df["sarane_kharid"] > config["min_sarane_kharid"])  # سرانه خرید > 70
            & (df["sarane_kharid"] > df["sarane_forosh"])  # سرانه خرید > سرانه فروش
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 11: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        # مرتب‌سازی براساس سرانه خرید (نزولی)
        filtered = filtered.sort_values("sarane_kharid", ascending=False)

        logger.info(
            f"✅ فیلتر 11: {len(filtered)} سهم با خرید حقوقی قوی (در حال خروج پول حقیقی)"
        )
        return filtered

    # ========================================
    # اجرای ایمن یک فیلتر — جلوگیری از سقوط کل pipeline
    # به‌خاطر خطای یک فیلتر (مثلاً ستون گمشده بعد از تغییر schema API)
    # ========================================
    def _run_filter_safe(self, filter_func, *args, **kwargs) -> pd.DataFrame:
        try:
            return filter_func(*args, **kwargs)
        except Exception as e:
            logger.error(
                f"❌ خطای غیرمنتظره در {filter_func.__name__}: {e} — "
                f"این فیلتر رد می‌شود، بقیه فیلترها ادامه پیدا می‌کنند",
                exc_info=True,
            )
            self.failed_filters.append(filter_func.__name__)
            return pd.DataFrame()

    # ========================================
    # اعمال همه فیلترها
    # ========================================
    def apply_all_filters(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        همه‌ی ۱۱ فیلتر رو روی یک دیتافریم یکپارچه اجرا می‌کنه (دیگه تفکیک
        api1/api2 وجود نداره - از وقتی BrsApi حذف شد، فیلتر ۱۰ هم دقیقاً
        مثل بقیه‌ی فیلترها مستقیم روی همین df اجرا می‌شه).
        """
        logger.info("شروع اعمال فیلترها")
        logger.info(f"  • داده: {len(df)} سهم/صندوق")

        self.failed_filters = []
        results: Dict[str, pd.DataFrame] = {}

        if not df.empty:
            results = {
                "filter_1_strong_buying": self._run_filter_safe(self.filter_1_strong_buying_power, df),
                "filter_2_sarane_cross": self._run_filter_safe(self.filter_2_sarane_kharid_cross, df),
                "filter_3_watchlist": self._run_filter_safe(self.filter_3_watchlist_symbols, df),
                "filter_4_range_mosbat": self._run_filter_safe(self.filter_4_range_mosbat, df),
                "filter_5_pol_hagigi_ratio": self._run_filter_safe(self.filter_5_pol_hagigi_ratio, df),
                "filter_6_tick_time": self._run_filter_safe(self.filter_6_tick_and_time, df),
                "filter_7_suspicious_volume": self._run_filter_safe(self.filter_7_suspicious_volume, df),
                "filter_8_swing_trade": self._run_filter_safe(self.filter_8_swing_trade, df),
                "filter_9_first_hour": self._run_filter_safe(self.filter_9_first_hour, df),
                "filter_10_heavy_buy_queue": self._run_filter_safe(self.filter_10_heavy_buy_queue, df),
                "filter_11_hoghooghi_haghighi_strong_buy": self._run_filter_safe(
                    self.filter_11_hoghooghi_haghighi_strong_buy, df
                ),
            }

        total = sum(len(v) for v in results.values())
        logger.info(f"✅ جمع نتایج فیلترها: {total} سهم/صندوق (۱۱ فیلتر)")

        self.filters_results = results
        return results
