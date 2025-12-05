"""
ماژول پردازش و فیلتر داده‌های بورس
فیلترهای 1 تا 9: روی API اول
فیلتر 10: روی API دوم
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

    def process_all_data(self, df_api1_raw: pd.DataFrame, df_api2_raw: pd.DataFrame) -> tuple:
        """
        پردازش داده‌های خام از هر دو API
        
        Args:
            df_api1_raw: DataFrame خام از API اول (برای فیلترهای 1-9)
            df_api2_raw: DataFrame خام از API دوم (برای فیلتر 10)
            
        Returns:
            tuple: (df_api1_processed, df_api2_processed)
        """
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
        if 'symbol' in df.columns:
            df = df.dropna(subset=['symbol'])

        # تبدیل ستون‌های عددی از string به numeric
        numeric_columns = [
            'volume', 'value', 
            'first_price', 'first_price_change_percent',
            'high_price', 'high_price_change_percent',
            'low_price', 'low_price_change_percent',
            'last_price', 'last_price_change_percent',
            'final_price', 'final_price_change_percent',
            'diff_last_final', 'volatility',
            'sarane_kharid', 'sarane_forosh', 'godrat_kharid',
            'pol_hagigi', 'buy_order_value', 'sell_order_value',
            'diff_buy_sell_order',
            'avg_5_day_pol_hagigi', 'avg_20_day_pol_hagigi', 'avg_60_day_pol_hagigi',
            '5_day_pol_hagigi', '20_day_pol_hagigi', '60_day_pol_hagigi',
            '5_day_godrat_kharid', '20_day_godrat_kharid',
            'avg_monthly_value', 'value_to_avg_monthly_value',
            'avg_3_month_value', 'value_to_avg_3_month_value',
            '5_day_return', '20_day_return', '60_day_return',
            'marketcap', 'value_to_marketcap'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        logger.info("✅ تبدیل ستون‌های عددی API اول انجام شد")

        # محاسبه pol_hagigi_to_avg_monthly_value
        if all(col in df.columns for col in ['pol_hagigi', 'avg_monthly_value']):
            df['pol_hagigi_to_avg_monthly_value'] = df.apply(
                lambda row: row['pol_hagigi'] / row['avg_monthly_value']
                if row['avg_monthly_value'] != 0 and pd.notna(row['avg_monthly_value'])
                else 0,
                axis=1
            )
            logger.info("✅ محاسبه pol_hagigi_to_avg_monthly_value انجام شد")
        else:
            logger.warning("⚠️ ستون‌های pol_hagigi یا avg_monthly_value برای محاسبه نسبت یافت نشد")
            df['pol_hagigi_to_avg_monthly_value'] = 0

        return df

    def _clean_and_prepare_api2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        پاکسازی و آماده‌سازی داده‌های API دوم
        محاسبه buy_order و buy_queue_value برای فیلتر 10
        """
        # تبدیل نام ستون l18 به symbol
        if 'l18' in df.columns:
            df = df.rename(columns={'l18': 'symbol'})

        # حذف ردیف‌های نال
        if 'symbol' in df.columns:
            df = df.dropna(subset=['symbol'])

        # محاسبه buy_order (میلیون تومان) برای فیلتر
        # buy_order = (qd1 * pd1 / zd1) / 10,000,000
        if all(col in df.columns for col in ['qd1', 'pd1', 'zd1']):
            df['buy_order'] = df.apply(
                lambda row: (row['qd1'] * row['pd1'] / row['zd1']) / 10_000_000 
                if row['zd1'] != 0 and pd.notna(row['zd1']) 
                else 0, 
                axis=1
            )
            logger.info("✅ محاسبه buy_order (میلیون تومان) انجام شد")
        else:
            logger.warning("⚠️ ستون‌های qd1, pd1, zd1 برای محاسبه buy_order یافت نشد")
            df['buy_order'] = 0

        # محاسبه buy_queue_value (میلیارد تومان) برای نمایش
        # buy_queue_value = (qd1 * pd1) / 10,000,000,000
        if all(col in df.columns for col in ['qd1', 'pd1']):
            df['buy_queue_value'] = (df['qd1'] * df['pd1']) / 10_000_000_000
            logger.info("✅ محاسبه buy_queue_value (میلیارد تومان) انجام شد")
        else:
            logger.warning("⚠️ ستون‌های qd1, pd1 برای محاسبه buy_queue_value یافت نشد")
            df['buy_queue_value'] = 0

        # تبدیل نام ستون‌های اضافی برای سازگاری
        column_mapping = {
            'pl': 'last_price',
            'plp': 'last_price_change_percent',
            'tval': 'value',
            'tvol': 'volume',
            'tmax': 'ceiling_price',  # آستانه مجاز بالا
        }

        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]

        return df

    # ========================================
    # فیلتر 1: قدرت خرید قوی (API اول)
    # ========================================
    def filter_1_strong_buying_power(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        فیلتر 1: سهام با قدرت خرید قوی
        
        شرایط:
        - ارزش امروز به میانگین ماهانه > 1
        - سرانه خرید > 50 میلیون تومان (واحد: میلیون)
        - قدرت خرید > 1
        - قدرت خرید امروز > میانگین 5 روز
        
        Args:
            df: DataFrame کل سهام از API اول
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        logger.info("اعمال فیلتر 1: قدرت خرید قوی")

        filtered = df[
            (df['value_to_avg_monthly_value'] > 1) &
            (df['sarane_kharid'] > 5.0) &
            (df['godrat_kharid'] > 1) &
            (df['godrat_kharid'] > df['5_day_godrat_kharid'])
        ].copy()

        filtered = filtered.sort_values('godrat_kharid', ascending=False)
        logger.info(f"✅ فیلتر 1: {len(filtered)} سهم یافت شد")

        return filtered

    # ========================================
    # فیلتر 2: کراس سرانه خرید (API اول)
    # ========================================
    def filter_2_sarane_kharid_cross(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        فیلتر 2: کراس سرانه خرید
        
        شرایط:
        - سرانه خرید > سرانه فروش
        
        Args:
            df: DataFrame کل سهام از API اول
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        logger.info("اعمال فیلتر 2: کراس سرانه خرید")

        filtered = df[
            df['sarane_kharid'] > df['sarane_forosh']
        ].copy()

        filtered = filtered.sort_values('sarane_kharid', ascending=False)
        logger.info(f"✅ فیلتر 2: {len(filtered)} سهم یافت شد")

        return filtered

    # ========================================
    # فیلتر 3: هشدار درصد تغییر نمادهای خاص (API اول)
    # ========================================
    def filter_3_watchlist_symbols(self, df: pd.DataFrame, watchlist: dict = None) -> pd.DataFrame:
        """
        فیلتر 3: هشدار درصد تغییر برای نمادهای خاص
        
        شرایط:
        - نماد در watchlist باشد
        - درصد تغییر آخرین > آستانه تعریف شده
        
        Args:
            df: DataFrame کل سهام از API اول
            watchlist: دیکشنری {نماد: آستانه_درصد}
            
        Returns:
            DataFrame سهام فیلتر شده
        """
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
            symbol_df = df[df['symbol'] == symbol]

            if symbol_df.empty:
                continue

            symbol_data = symbol_df.iloc[0]
            if symbol_data['last_price_change_percent'] > threshold:
                symbol_row = symbol_data.to_frame().T
                symbol_row['threshold'] = threshold
                filtered_list.append(symbol_row)
                logger.info(f"🔔 {symbol}: {symbol_data['last_price_change_percent']:.2f}% > {threshold}%")

        if not filtered_list:
            logger.info("فیلتر 3: هیچ نمادی از آستانه عبور نکرد")
            return pd.DataFrame()

        filtered = pd.concat(filtered_list, ignore_index=True)
        filtered = filtered.sort_values('last_price_change_percent', ascending=False)
        logger.info(f"✅ فیلتر 3: {len(filtered)} نماد از آستانه عبور کرد")

        return filtered

    # ========================================
    # فیلتر 4: رزرو شده برای آینده
    # ========================================
    def filter_4_heavy_buy_queue_at_ceiling(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 4: غیرفعال (رزرو شده برای فیلتر جدید)
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame خالی
        """
        logger.info("فیلتر 4: غیرفعال است")
        return pd.DataFrame()

    # ========================================
    # فیلتر 5: نسبت پول حقیقی (API اول)
    # ========================================
    def filter_5_pol_hagigi_ratio(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 5: نسبت پول حقیقی به ارزش معاملات
        
        شرایط:
        - نسبت پول حقیقی به میانگین ماهانه >= 0.5
        - سرانه خرید >= 50 میلیون تومان
        - قدرت خرید >= 1.5
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        if config is None:
            from config import POL_HAGIGI_FILTER_CONFIG
            config = POL_HAGIGI_FILTER_CONFIG

        min_ratio = config.get('min_pol_to_value_ratio', 0.5)
        min_sarane = config.get('min_sarane_kharid', 5.0)
        min_godrat = config.get('min_godrat_kharid', 1.5)

        logger.info(f"اعمال فیلتر 5: نسبت پول حقیقی (آستانه: {min_ratio})")

        filtered = df[
            (df['pol_hagigi_to_avg_monthly_value'] >= min_ratio) &
            (df['sarane_kharid'] >= min_sarane) &
            (df['godrat_kharid'] >= min_godrat)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 5: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('pol_hagigi_to_avg_monthly_value', ascending=False)
        logger.info(f"✅ فیلتر 5: {len(filtered)} سهم با نسبت پول حقیقی بالا")

        return filtered

    # ========================================
    # فیلتر 6: تیک و ساعت (API اول)
    # ========================================
    def filter_6_tick_and_time(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 6: تیک و ساعت - رشد قیمتی در آخر معاملات
        
        شرایط:
        - 0.98 × قیمت اولین > قیمت کف
        - 0.98 × قیمت آخرین > قیمت اولین
        - درصد تغییر آخرین - درصد تغییر پایانی > 2%
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        if config is None:
            from config import TICK_FILTER_CONFIG
            config = TICK_FILTER_CONFIG

        first_to_low_ratio = config.get('first_to_low_ratio', 0.98)
        last_to_first_ratio = config.get('last_to_first_ratio', 0.98)
        tick_diff_percent = config.get('tick_diff_percent', 2.0)

        logger.info(f"اعمال فیلتر 6: تیک و ساعت")

        df_copy = df.copy()
        df_copy['tick_diff'] = (
            df_copy['last_price_change_percent'] - df_copy['final_price_change_percent']
        )

        filtered = df_copy[
            (first_to_low_ratio * df_copy['first_price'] > df_copy['low_price']) &
            (last_to_first_ratio * df_copy['last_price'] > df_copy['first_price']) &
            (df_copy['tick_diff'] > tick_diff_percent)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 6: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('tick_diff', ascending=False)
        logger.info(f"✅ فیلتر 6: {len(filtered)} سهم با تیک مثبت در آخر روز")

        return filtered

    # ========================================
    # فیلتر 7: حجم مشکوک (API اول)
    # ========================================
    def filter_7_suspicious_volume(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 7: حجم مشکوک - ارزش معاملات بسیار بالاتر از میانگین
        
        شرایط:
        - ارزش معاملات به میانگین ماهانه > 2
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        if config is None:
            from config import SUSPICIOUS_VOLUME_CONFIG
            config = SUSPICIOUS_VOLUME_CONFIG

        min_ratio = config.get('min_value_to_avg_ratio', 2.0)

        logger.info(f"اعمال فیلتر 7: حجم مشکوک (آستانه: {min_ratio}x)")

        filtered = df[
            df['value_to_avg_monthly_value'] > min_ratio
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 7: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('value_to_avg_monthly_value', ascending=False)
        logger.info(f"✅ فیلتر 7: {len(filtered)} سهم با حجم مشکوک")

        return filtered

    # ========================================
    # فیلتر 8: نوسان‌گیری (API اول)
    # ========================================
    def filter_8_swing_trade(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 8: نوسان‌گیری - خرید در کف
        
        شرایط:
        - پایین‌ترین قیمت = حداقل آستانه مجاز
        - آخرین قیمت > حداقل آستانه مجاز
        - قدرت خرید >= 2.0
        - سرانه خرید >= 50 میلیون
        - ارزش معاملات >= میانگین ماهانه
        - درصد آخرین < -2%
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df

        if config is None:
            from config import SWING_TRADE_CONFIG
            config = SWING_TRADE_CONFIG

        min_allowed = config.get('min_allowed_price', -5.0)
        max_last_change = config.get('max_last_change_percent', -2.0)
        min_godrat = config.get('min_godrat_kharid', 2.0)
        min_sarane = config.get('min_sarane_kharid', 5.0)

        logger.info(f"اعمال فیلتر 8: نوسان‌گیری")

        filtered = df[
            (df['low_price_change_percent'] == min_allowed) &
            (df['last_price_change_percent'] > min_allowed) &
            (df['godrat_kharid'] >= min_godrat) &
            (df['sarane_kharid'] >= min_sarane) &
            (df['value_to_avg_monthly_value'] >= 1.0) &
            (df['last_price_change_percent'] < max_last_change)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 8: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('godrat_kharid', ascending=False)
        logger.info(f"✅ فیلتر 8: {len(filtered)} سهم برای نوسان‌گیری")

        return filtered

    # ========================================
    # فیلتر 9: یک ساعت اول (API اول)
    # ========================================
    def filter_9_first_hour(self, df: pd.DataFrame, config: dict = None, current_hour: int = None) -> pd.DataFrame:
        """
        فیلتر 9: یک ساعت اول - فقط در ساعت اول بازار (9:00 - 10:00)
        
        شرایط:
        - ساعت فعلی بین 9 تا 10 (به وقت تهران)
        - ارزش معاملات به میانگین ماهانه >= 1
        
        Args:
            df: DataFrame کل سهام از API اول
            config: تنظیمات فیلتر
            current_hour: ساعت فعلی (برای تست)
            
        Returns:
            DataFrame سهام فیلتر شده یا خالی
        """
        if df.empty:
            return df

        if current_hour is None:
            from datetime import datetime
            import pytz
            tehran_tz = pytz.timezone('Asia/Tehran')
            now_tehran = datetime.now(tehran_tz)
            current_hour = now_tehran.hour

        if config is None:
            from config import FIRST_HOUR_CONFIG
            config = FIRST_HOUR_CONFIG

        start_hour = config.get('start_hour', 9)
        end_hour = config.get('end_hour', 10)
        min_ratio = config.get('min_value_to_avg_ratio', 1.0)

        if not (start_hour <= current_hour < end_hour):
            logger.info(f"فیلتر 9: خارج از بازه زمانی ({start_hour}-{end_hour}). ساعت فعلی: {current_hour}")
            return pd.DataFrame()

        logger.info(f"اعمال فیلتر 9: یک ساعت اول (ساعت تهران: {current_hour})")

        filtered = df[
            df['value_to_avg_monthly_value'] >= min_ratio
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 9: هیچ سهمی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('value_to_avg_monthly_value', ascending=False)
        logger.info(f"✅ فیلتر 9: {len(filtered)} سهم در ساعت اول")

        return filtered

    # ========================================
    # فیلتر 10: صف خرید میلیاردی (API دوم)
    # ========================================
    def filter_10_heavy_buy_queue(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 10: صف خرید میلیاردی
        استفاده از API دوم (BrsApi)
        
        شرایط:
        - آخرین قیمت = آستانه مجاز بالا (سقف)
        - buy_order >= 70 میلیون تومان
        
        Args:
            df: DataFrame کل نمادها از API دوم
            config: تنظیمات فیلتر
            
        Returns:
            DataFrame نمادهای فیلتر شده
        """
        if df.empty:
            return df

        if config is None:
            from config import HEAVY_BUY_QUEUE_CONFIG
            config = HEAVY_BUY_QUEUE_CONFIG

        min_buy_order = config.get('min_buy_order', 70)

        logger.info(f"اعمال فیلتر 10: صف خرید میلیاردی")
        logger.info(f"  • شرط 1: آخرین قیمت = سقف")
        logger.info(f"  • شرط 2: buy_order >= {min_buy_order} میلیون تومان")

        # بررسی وجود ستون‌های لازم
        required_cols = ['last_price', 'ceiling_price', 'buy_order']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"❌ ستون‌های گمشده در API دوم: {missing_cols}")
            return pd.DataFrame()

        # اعمال فیلتر
        filtered = df[
            (df['last_price'] == df['ceiling_price']) &
            (df['buy_order'] >= min_buy_order)
        ].copy()

        if filtered.empty:
            logger.info("فیلتر 10: هیچ نمادی یافت نشد")
            return pd.DataFrame()

        filtered = filtered.sort_values('buy_queue_value', ascending=False)
        logger.info(f"✅ فیلتر 10: {len(filtered)} نماد با صف خرید میلیاردی در سقف")

        return filtered

    # ========================================
    # اعمال همه فیلترها
    # ========================================
    def apply_all_filters(self, df_api1: pd.DataFrame, df_api2: pd.DataFrame) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        اعمال همه فیلترها
        فیلترهای 1-9 روی API اول
        فیلتر 10 روی API دوم
        
        Args:
            df_api1: DataFrame از API اول
            df_api2: DataFrame از API دوم
            
        Returns:
            دیکشنری شامل نتایج هر فیلتر تفکیک شده به api1 و api2
        """
        logger.info(f"شروع اعمال فیلترها")
        logger.info(f"  • API اول: {len(df_api1)} سهم")
        logger.info(f"  • API دوم: {len(df_api2)} نماد")

        results = {
            'api1': {},
            'api2': {}
        }

        # فیلترهای 1 تا 9 روی API اول
        if not df_api1.empty:
            results['api1'] = {
                'filter_1_strong_buying': self.filter_1_strong_buying_power(df_api1),
                'filter_2_sarane_cross': self.filter_2_sarane_kharid_cross(df_api1),
                'filter_3_watchlist': self.filter_3_watchlist_symbols(df_api1),
                'filter_4_ceiling_queue': self.filter_4_heavy_buy_queue_at_ceiling(df_api1),
                'filter_5_pol_hagigi_ratio': self.filter_5_pol_hagigi_ratio(df_api1),
                'filter_6_tick_time': self.filter_6_tick_and_time(df_api1),
                'filter_7_suspicious_volume': self.filter_7_suspicious_volume(df_api1),
                'filter_8_swing_trade': self.filter_8_swing_trade(df_api1),
                'filter_9_first_hour': self.filter_9_first_hour(df_api1),
            }

        # فیلتر 10 روی API دوم
        if not df_api2.empty:
            results['api2'] = {
                'filter_10_heavy_buy_queue': self.filter_10_heavy_buy_queue(df_api2),
            }

        # خلاصه نتایج
        total_api1 = sum(len(v) for v in results['api1'].values())
        total_api2 = sum(len(v) for v in results['api2'].values())

        logger.info(f"✅ جمع نتایج فیلترها:")
        logger.info(f"  • API اول (فیلتر 1-9): {total_api1} سهم")
        logger.info(f"  • API دوم (فیلتر 10): {total_api2} نماد")

        self.filters_results = results
        return results