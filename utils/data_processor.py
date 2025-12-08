"""
ماژول پردازش و فیلتر داده‌های بورس
فیلترهای 1 تا 9: روی API اول
فیلتر 10: روی API دوم + غنی‌سازی با API اول
"""

import pandas as pd
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class BourseDataProcessor:
    """کلاس پردازش و اعمال فیلترها بر روی داده‌های بورس"""

    def __init__(self):
        self.filters_results = {}

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
        if "symbol" in df.columns:
            df = df.dropna(subset=["symbol"])

        numeric_columns = [
            "volume", "value", "first_price", "first_price_change_percent",
            "high_price", "high_price_change_percent", "low_price", "low_price_change_percent",
            "last_price", "last_price_change_percent", "final_price", "final_price_change_percent",
            "diff_last_final", "volatility", "sarane_kharid", "sarane_forosh", "godrat_kharid",
            "pol_hagigi", "buy_order_value", "sell_order_value", "diff_buy_sell_order",
            "avg_5_day_pol_hagigi", "avg_20_day_pol_hagigi", "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi", "20_day_pol_hagigi", "60_day_pol_hagigi",
            "5_day_godrat_kharid", "20_day_godrat_kharid", "avg_monthly_value",
            "value_to_avg_monthly_value", "avg_3_month_value", "value_to_avg_3_month_value",
            "5_day_return", "20_day_return", "60_day_return", "marketcap", "value_to_marketcap",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        columns_to_divide = ["sarane_kharid", "sarane_forosh"]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = df[col] / 10_000_000

        columns_to_divide = [
            "value", "pol_hagigi", "buy_order_value", "sell_order_value", "diff_buy_sell_order",
            "avg_5_day_pol_hagigi", "avg_20_day_pol_hagigi", "avg_60_day_pol_hagigi",
            "5_day_pol_hagigi", "20_day_pol_hagigi", "60_day_pol_hagigi",
            "avg_monthly_value", "avg_3_month_value", "marketcap",
        ]
        for col in columns_to_divide:
            if col in df.columns:
                df[col] = df[col] / 10_000_000_000

        if all(col in df.columns for col in ["pol_hagigi", "avg_monthly_value"]):
            df["pol_hagigi_to_avg_monthly_value"] = df.apply(
                lambda row: (
                    row["pol_hagigi"] / row["avg_monthly_value"]
                    if row["avg_monthly_value"] != 0 and pd.notna(row["avg_monthly_value"])
                    else 0
                ),
                axis=1,
            )
        else:
            df["pol_hagigi_to_avg_monthly_value"] = 0

        return df

    def _clean_and_prepare_api2(self, df: pd.DataFrame) -> pd.DataFrame:
        if "l18" in df.columns:
            df = df.rename(columns={"l18": "symbol"})

        if "symbol" in df.columns:
            df = df.dropna(subset=["symbol"])

        if all(col in df.columns for col in ["qd1", "pd1", "zd1"]):
            df["buy_order"] = df.apply(
                lambda row: ((row["qd1"] * row["pd1"] / row["zd1"]) / 10_000_000
                             if row["zd1"] != 0 and pd.notna(row["zd1"])
                             else 0),
                axis=1,
            )
        else:
            df["buy_order"] = 0

        if all(col in df.columns for col in ["qd1", "pd1"]):
            df["buy_queue_value"] = (df["qd1"] * df["pd1"]) / 10_000_000_000
        else:
            df["buy_queue_value"] = 0

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

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["value"] = df["value"] / 10_000_000_000

        return df

    # ========================================
    # فیلتر 1: قدرت خرید قوی
    # ========================================
    def filter_1_strong_buying_power(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        from config import STRONG_BUYING_CONFIG
        config = STRONG_BUYING_CONFIG
        filtered = df[
            (df["value_to_avg_monthly_value"] > config["min_value_to_avg_monthly"])
            & (df["sarane_kharid"] > config["min_sarane_kharid"])
            & (df["godrat_kharid"] > config["min_godrat_kharid"])
            & (df["godrat_kharid"] > df["5_day_godrat_kharid"])
        ].copy()
        return filtered.sort_values("godrat_kharid", ascending=False)

    # ========================================
    # فیلتر 2: کراس سرانه خرید
    # ========================================
    def filter_2_sarane_kharid_cross(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        from config import SARANE_CROSS_CONFIG
        config = SARANE_CROSS_CONFIG
        filtered = df[
            (df["sarane_kharid"] > df["sarane_forosh"])
            & (df["value_to_avg_monthly_value"] >= config["min_value_to_avg_monthly"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
        ].copy()
        return filtered.sort_values("sarane_kharid", ascending=False)

    # ========================================
    # فیلتر 3: هشدار درصد تغییر نمادهای خاص
    # ========================================
    def filter_3_watchlist_symbols(self, df: pd.DataFrame, watchlist: dict = None) -> pd.DataFrame:
        if df.empty:
            return df
        if watchlist is None:
            from config import WATCHLIST_SYMBOLS
            watchlist = WATCHLIST_SYMBOLS
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
        if not filtered_list:
            return pd.DataFrame()
        filtered = pd.concat(filtered_list, ignore_index=True)
        return filtered.sort_values("last_price_change_percent", ascending=False)

    # ========================================
    # فیلتر 4: رزرو شده
    # ========================================
    def filter_4_heavy_buy_queue_at_ceiling(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        return pd.DataFrame()

    # ========================================
    # فیلتر 5: نسبت پول حقیقی
    # ========================================
    def filter_5_pol_hagigi_ratio(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        if df.empty:
            return df
        if config is None:
            from config import POL_HAGIGI_FILTER_CONFIG
            config = POL_HAGIGI_FILTER_CONFIG
        filtered = df[
            (df["pol_hagigi_to_avg_monthly_value"] >= config["min_pol_to_value_ratio"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
            & (df["godrat_kharid"] >= config["min_godrat_kharid"])
        ].copy()
        return filtered.sort_values("pol_hagigi_to_avg_monthly_value", ascending=False)

    # ========================================
    # فیلتر 6: تیک و ساعت
    # ========================================
    def filter_6_tick_and_time(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        if df.empty:
            return df
        if config is None:
            from config import TICK_FILTER_CONFIG
            config = TICK_FILTER_CONFIG
        first_to_low_ratio = config.get("first_to_low_ratio", 0.98)
        last_to_first_ratio = config.get("last_to_first_ratio", 0.98)
        tick_diff_percent = config.get("tick_diff_percent", 2.0)
        df_copy = df.copy()
        df_copy["tick_diff"] = df_copy["last_price_change_percent"] - df_copy["final_price_change_percent"]
        filtered = df_copy[
            (first_to_low_ratio * df_copy["first_price"] > df_copy["low_price"])
            & (last_to_first_ratio * df_copy["last_price"] > df_copy["first_price"])
            & (df_copy["tick_diff"] > tick_diff_percent)
        ].copy()
        return filtered.sort_values("tick_diff", ascending=False)

    # ========================================
    # فیلتر 7: حجم مشکوک
    # ========================================
    def filter_7_suspicious_volume(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        if df.empty:
            return df
        if config is None:
            from config import SUSPICIOUS_VOLUME_CONFIG
            config = SUSPICIOUS_VOLUME_CONFIG
        min_ratio = config.get("min_value_to_avg_ratio", 2.0)
        filtered = df[df["value_to_avg_monthly_value"] > min_ratio].copy()
        return filtered.sort_values("value_to_avg_monthly_value", ascending=False)

    # ========================================
    # فیلتر 8: نوسان‌گیری
    # ========================================
    def filter_8_swing_trade(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        if df.empty:
            return df
        if config is None:
            from config import SWING_TRADE_CONFIG
            config = SWING_TRADE_CONFIG
        filtered = df[
            (df["low_price_change_percent"] == config["min_allowed_price"])
            & (df["last_price_change_percent"] > config["min_allowed_price"])
            & (df["godrat_kharid"] >= config["min_godrat_kharid"])
            & (df["sarane_kharid"] >= config["min_sarane_kharid"])
            & (df["value_to_avg_monthly_value"] >= config["min_value_to_avg_monthly"])
            & (df["last_price_change_percent"] < config["max_last_change_percent"])
        ].copy()
        return filtered.sort_values("godrat_kharid", ascending=False)

    # ========================================
    # فیلتر 9: یک ساعت اول
    # ========================================
    def filter_9_first_hour(self, df: pd.DataFrame, config: dict = None, current_hour: int = None) -> pd.DataFrame:
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
            return pd.DataFrame()
        filtered = df[df["value_to_avg_monthly_value"] >= min_ratio].copy()
        return filtered.sort_values("value_to_avg_monthly_value", ascending=False)

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
        required_cols = ["last_price", "ceiling_price", "buy_order", "buy_queue_value"]
        missing_cols = [col for col in required_cols if col not in df_api2.columns]
        if missing_cols:
            return pd.DataFrame()
        filtered_api2 = df_api2[
            (df_api2["last_price"] == df_api2["ceiling_price"])
            & (df_api2["buy_order"] >= config["min_buy_order"])
            & (df_api2["buy_queue_value"] >= config["min_buy_queue_value"])
        ].copy()
        if filtered_api2.empty:
            return pd.DataFrame()
        if df_api1 is not None and not df_api1.empty:
            columns_from_api1 = [
                'symbol', 'sarane_kharid', 'pol_hagigi', 'value_to_avg_monthly_value',
                'godrat_kharid', 'value', 'sarane_forosh'
            ]
            available_columns = [col for col in columns_from_api1 if col in df_api1.columns]
            if 'symbol' in available_columns:
                api1_subset = df_api1[available_columns].copy()
                filtered_api2['symbol_clean'] = filtered_api2['symbol'].str.strip().str.upper()
                api1_subset['symbol_clean'] = api1_subset['symbol'].str.strip().str.upper()
                if all(col in api1_subset.columns for col in ['pol_hagigi', 'value']):
                    api1_subset['pol_hagigi_to_value'] = api1_subset.apply(
                        lambda row: (
                            row['pol_hagigi'] / row['value']
                            if row['value'] != 0 and pd.notna(row['value'])
                            else 0
                        ),
                        axis=1,
                    )
                enriched = filtered_api2.merge(
                    api1_subset, 
                    on='symbol_clean', 
                    how='left', 
                    suffixes=('_api2', '_api1')
                )
                enriched = enriched.drop(columns=['symbol_clean'], errors='ignore')
                return enriched
        return filtered_api2