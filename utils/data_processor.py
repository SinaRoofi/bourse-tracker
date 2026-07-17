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

    def process_all_data(
        self, df_api1_raw: pd.DataFrame, df_api2_raw: pd.DataFrame
    ) -> tuple:

        logger.info("شروع پردازش داده‌های خام...")

        # پردازش API اول
        if df_api1_raw is not None and not df_api1_raw.empty:
            df_api1 = self._clean_and_prepare_api1(df_api1_raw)
            logger.info(f"✅ API اول: {len(df_api1)} سهم پردازش شد")
        else:
            df_api1 = pd.DataFrame()
            logger.warning("⚠️ API اول خالی است")

        # پردازش API دوم
        if df_api2_raw is not None and not df_api2_raw.empty:
            df_api2 = self._clean_and_prepare_api2(df_api2_raw)
            logger.info(f"✅ API دوم: {len(df_api2)} نماد پردازش شد")
        else:
            df_api2 = pd.DataFrame()
            logger.warning("⚠️ API دوم خالی است")

        return df_api1, df_api2

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
            "5_day_return",
            "20_day_return",
            "60_day_return",
            "marketcap",
            "value_to_marketcap",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("✅ تبدیل ستون‌های عددی API اول انجام شد")

        # تقسیم ستون‌ها به 10 میلیون
        columns_to_divide = ["sarane_kharid", "sarane_forosh"]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = df[col] / 10_000_000

        logger.info("✅ تقسیم ستون‌ها به 10 میلیون انجام شد")

        # تقسیم ستون‌ها به 10 میلیارد
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

        return df

    def _clean_and_prepare_api2(self, df: pd.DataFrame) -> pd.DataFrame:
        """پاکسازی و آماده‌سازی داده‌های API دوم"""
        # تبدیل نام ستون l18 به symbol
        if "l18" in df.columns:
            df = df.rename(columns={"l18": "symbol"})

        # حذف ردیف‌های نال
        if "symbol" in df.columns:
            df = df.dropna(subset=["symbol"])

        # محاسبه buy_order (میلیون تومان)
        if all(col in df.columns for col in ["qd1", "pd1", "zd1"]):
            df["buy_order"] = df.apply(
                lambda row: (
                    (row["qd1"] * row["pd1"] / row["zd1"]) / 10_000_000
                    if row["zd1"] != 0 and pd.notna(row["zd1"])
                    else 0
                ),
                axis=1,
            )
            logger.info("✅ محاسبه buy_order (میلیون تومان) انجام شد")
        else:
            logger.warning("⚠️ ستون‌های qd1, pd1, zd1 برای محاسبه buy_order یافت نشد")
            df["buy_order"] = 0

        # محاسبه buy_queue_value (میلیارد تومان)
        if all(col in df.columns for col in ["qd1", "pd1"]):
            df["buy_queue_value"] = (df["qd1"] * df["pd1"]) / 10_000_000_000
            logger.info("✅ محاسبه buy_queue_value (میلیارد تومان) انجام شد")
        else:
            logger.warning("⚠️ ستون‌های qd1, pd1 برای محاسبه buy_queue_value یافت نشد")
            df["buy_queue_value"] = 0

        # تبدیل نام ستون‌های اضافی
        column_mapping = {
            "pl": "last_price",
            "plp": "last_price_change_percent",
            "tval": "value",
            "tvol": "volume",
            "tmax": "ceiling_price",
        }
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]

        # تقسیم value به 10 میلیارد
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["value"] = df["value"] / 10_000_000_000
            logger.info("✅ تقسیم value به 10 میلیارد انجام شد")

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
    # فیلتر 3: هشدار درصد تغییر نمادهای خاص
    # ========================================
    def filter_3_watchlist_symbols(
        self, df: pd.DataFrame, watchlist: dict = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if watchlist is None:
            from config import WATCHLIST_SYMBOLS

            watchlist = WATCHLIST_SYMBOLS

        if not watchlist:
            logger.warning("فیلتر 3: watchlist خالی است!")
            return pd.DataFrame()

        logger.info(f"اعمال فیلتر 3: بررسی {len(watchlist)} نماد")
        filtered_list = []

        for symbol, threshold in watchlist.items():
            symbol_df = df[df["symbol"] == symbol]
            if symbol_df.empty:
                continue

            symbol_data = symbol_df.iloc[0]
            if symbol_data["last_price_change_percent"] > threshold:
                symbol_row = symbol_data.to_frame().T
                symbol_row["threshold"] = threshold
                filtered_list.append(symbol_row)
                logger.info(
                    f"🔔 {symbol}: {symbol_data['last_price_change_percent']:.2f}% > {threshold}%"
                )

        if not filtered_list:
            logger.info("فیلتر 3: هیچ نمادی از آستانه عبور نکرد")
            return pd.DataFrame()

        filtered = pd.concat(filtered_list, ignore_index=True)
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
        self, df: pd.DataFrame, config: dict = None, current_hour: int = None
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if current_hour is None:
            from datetime import datetime
            import pytz

            tehran_tz = pytz.timezone("Asia/Tehran")
            now_tehran = datetime.now(tehran_tz)
            current_hour = now_tehran.hour

        if config is None:
            from config import FIRST_HOUR_CONFIG

            config = FIRST_HOUR_CONFIG

        start_hour = config.get("start_hour", 9)
        end_hour = config.get("end_hour", 10)
        min_ratio = config.get("min_value_to_avg_ratio", 1.0)

        if not (start_hour <= current_hour < end_hour):
            logger.info(
                f"فیلتر 9: خارج از بازه زمانی ({start_hour}-{end_hour}). ساعت فعلی: {current_hour}"
            )
            return pd.DataFrame()

        logger.info(f"اعمال فیلتر 9: یک ساعت اول (ساعت تهران: {current_hour})")

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
        self, df_api2: pd.DataFrame, df_api1: pd.DataFrame = None, config: dict = None
    ) -> pd.DataFrame:

        if df_api2.empty:
            return df_api2

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

        # بررسی وجود ستون‌های لازم در API دوم
        required_cols = ["last_price", "ceiling_price", "buy_order", "buy_queue_value"]
        missing_cols = [col for col in required_cols if col not in df_api2.columns]

        if missing_cols:
            logger.error(f"❌ ستون‌های گمشده در API دوم: {missing_cols}")
            return pd.DataFrame()

        # اعمال فیلتر روی API دوم
        mask = (df_api2["buy_order"] >= config["min_buy_order"]) & (
            df_api2["buy_queue_value"] >= config["min_buy_queue_value"]
        )

        if config.get("price_at_ceiling", True):
            mask &= df_api2["last_price"] == df_api2["ceiling_price"]

        filtered_api2 = df_api2[mask].copy()

        if filtered_api2.empty:
            logger.info("فیلتر 10: هیچ نمادی یافت نشد")
            return pd.DataFrame()

        logger.info(f"✅ فیلتر 10: {len(filtered_api2)} نماد در API دوم یافت شد")

        # غنی‌سازی با API اول
        if df_api1 is not None and not df_api1.empty:
            logger.info("🔄 غنی‌سازی نمادها با اطلاعات API اول...")

            # ستون‌هایی که می‌خواهیم از API اول بیاوریم
            columns_from_api1 = [
                "symbol",
                "sarane_kharid",
                "pol_hagigi",
                "value_to_avg_monthly_value",
                "godrat_kharid",
                "value",
                "sarane_forosh",
                "marketcap",
                "5_day_return",
            ]

            # فقط ستون‌های موجود را انتخاب کنیم
            available_columns = [
                col for col in columns_from_api1 if col in df_api1.columns
            ]

            if "symbol" in available_columns:
                api1_subset = df_api1[available_columns].copy()

                # پاکسازی symbol (حذف فضای خالی و یکسان‌سازی)
                filtered_api2["symbol_clean"] = (
                    filtered_api2["symbol"].str.strip().str.upper()
                )
                api1_subset["symbol_clean"] = (
                    api1_subset["symbol"].str.strip().str.upper()
                )

                # محاسبه pol_hagigi_to_value
                if all(col in api1_subset.columns for col in ["pol_hagigi", "value"]):
                    api1_subset["pol_hagigi_to_value"] = api1_subset.apply(
                        lambda row: (
                            row["pol_hagigi"] / row["value"]
                            if row["value"] != 0 and pd.notna(row["value"])
                            else 0
                        ),
                        axis=1,
                    )

                # Merge با استفاده از symbol_clean
                enriched = filtered_api2.merge(
                    api1_subset,
                    on="symbol_clean",
                    how="left",
                    suffixes=("_api2", "_api1"),
                )

                # حذف ستون‌های اضافی و کپی
                enriched = enriched.drop(columns=["symbol_clean"], errors="ignore")

                # اولویت دادن به symbol از API دوم
                if "symbol_api1" in enriched.columns:
                    enriched = enriched.drop(columns=["symbol_api1"], errors="ignore")
                if "symbol_api2" in enriched.columns:
                    enriched["symbol"] = enriched["symbol_api2"]
                    enriched = enriched.drop(columns=["symbol_api2"], errors="ignore")

                # اولویت دادن به value از API اول
                if "value_api1" in enriched.columns:
                    enriched["value"] = enriched["value_api1"].fillna(
                        enriched.get("value_api2", 0)
                    )
                    enriched = enriched.drop(
                        columns=["value_api1", "value_api2"], errors="ignore"
                    )

                # لاگ نمادهایی که غنی نشدن
                not_enriched = enriched[enriched["value_to_avg_monthly_value"].isna()]
                if len(not_enriched) > 0:
                    logger.warning(
                        f"⚠️ {len(not_enriched)} نماد از API اول پیدا نشد: {list(not_enriched['symbol'])}"
                    )

                logger.info(
                    f"✅ {len(enriched)} نماد پردازش شد، {len(enriched) - len(not_enriched)} نماد غنی شد"
                )
                enriched = enriched.sort_values("buy_queue_value", ascending=False)
                return enriched
            else:
                logger.warning("⚠️ ستون symbol در API اول یافت نشد")
                filtered_api2 = filtered_api2.sort_values(
                    "buy_queue_value", ascending=False
                )
                return filtered_api2
        else:
            logger.warning("⚠️ API اول خالی است، غنی‌سازی انجام نمی‌شود")
            filtered_api2 = filtered_api2.sort_values(
                "buy_queue_value", ascending=False
            )
            return filtered_api2

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
    def apply_all_filters(
        self, df_api1: pd.DataFrame, df_api2: pd.DataFrame
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        logger.info("شروع اعمال فیلترها")
        logger.info(f"  • API اول: {len(df_api1)} سهم")
        logger.info(f"  • API دوم: {len(df_api2)} نماد")

        self.failed_filters = []
        results = {"api1": {}, "api2": {}}

        # فیلترهای 1 تا 9 و 11 روی API اول
        if not df_api1.empty:
            results["api1"] = {
                "filter_1_strong_buying": self._run_filter_safe(self.filter_1_strong_buying_power, df_api1),
                "filter_2_sarane_cross": self._run_filter_safe(self.filter_2_sarane_kharid_cross, df_api1),
                "filter_3_watchlist": self._run_filter_safe(self.filter_3_watchlist_symbols, df_api1),
                "filter_4_range_mosbat": self._run_filter_safe(self.filter_4_range_mosbat, df_api1),
                "filter_5_pol_hagigi_ratio": self._run_filter_safe(self.filter_5_pol_hagigi_ratio, df_api1),
                "filter_6_tick_time": self._run_filter_safe(self.filter_6_tick_and_time, df_api1),
                "filter_7_suspicious_volume": self._run_filter_safe(self.filter_7_suspicious_volume, df_api1),
                "filter_8_swing_trade": self._run_filter_safe(self.filter_8_swing_trade, df_api1),
                "filter_9_first_hour": self._run_filter_safe(self.filter_9_first_hour, df_api1),
                "filter_11_hoghooghi_haghighi_strong_buy": self._run_filter_safe(
                    self.filter_11_hoghooghi_haghighi_strong_buy, df_api1
                ),
            }

        # فیلتر 10 روی API دوم با غنی‌سازی از API اول
        if not df_api2.empty:
            results["api2"] = {
                "filter_10_heavy_buy_queue": self._run_filter_safe(
                    self.filter_10_heavy_buy_queue, df_api2, df_api1
                ),
            }

        # خلاصه نتایج
        total_api1 = sum(len(v) for v in results["api1"].values())
        total_api2 = sum(len(v) for v in results["api2"].values())

        logger.info("✅ جمع نتایج فیلترها:")
        logger.info(f"  • API اول (فیلتر 1-9, 11): {total_api1} سهم")
        logger.info(f"  • API دوم (فیلتر 10): {total_api2} نماد")

        self.filters_results = results
        return results
