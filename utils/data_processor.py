"""
ماژول پردازش و فیلتر داده‌های بورس
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
    # فیلتر 1: قدرت خرید قوی
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
            df: DataFrame کل سهام
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        logger.info("اعمال فیلتر 1: قدرت خرید قوی")
        
        filtered = df[
            (df['value_to_avg_monthly_value'] > 1) &
            (df['sarane_kharid'] > 5.0) &  # 50 میلیون تومان = 5 واحد (میلیون)
            (df['godrat_kharid'] > 1) &
            (df['godrat_kharid'] > df['5_day_godrat_kharid'])
        ].copy()
        
        # مرتب‌سازی بر اساس قدرت خرید (نزولی)
        filtered = filtered.sort_values('godrat_kharid', ascending=False)
        
        logger.info(f"✅ فیلتر 1: {len(filtered)} سهم یافت شد")
        
        return filtered
    
    # ========================================
    # فیلتر 2: کراس سرانه خرید
    # ========================================
    def filter_2_sarane_kharid_cross(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        فیلتر 2: کراس سرانه خرید (سرانه خرید از سرانه فروش بالاتر رفت)
        
        شرایط:
        - سرانه خرید > سرانه فروش
        
        Args:
            df: DataFrame کل سهام
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        logger.info("اعمال فیلتر 2: کراس سرانه خرید")
        
        filtered = df[
            df['sarane_kharid'] > df['sarane_forosh']
        ].copy()
        
        # مرتب‌سازی بر اساس سرانه خرید (نزولی)
        filtered = filtered.sort_values('sarane_kharid', ascending=False)
        
        logger.info(f"✅ فیلتر 2: {len(filtered)} سهم یافت شد")
        
        return filtered
    
    # ========================================
    # فیلتر 3: هشدار درصد تغییر نمادهای خاص
    # ========================================
    def filter_3_watchlist_symbols(self, df: pd.DataFrame, watchlist: dict = None) -> pd.DataFrame:
        """
        فیلتر 3: هشدار درصد تغییر برای نمادهای خاص
        
        شرایط:
        - نماد در watchlist باشد
        - درصد تغییر آخرین > آستانه تعریف شده
        
        Args:
            df: DataFrame کل سهام
            watchlist: دیکشنری {نماد: آستانه_درصد}
                      اگر None باشد، از config استفاده می‌شود
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        # اگه watchlist داده نشده، از config استفاده کن
        if watchlist is None:
            from config import WATCHLIST_SYMBOLS
            watchlist = WATCHLIST_SYMBOLS
        
        if not watchlist:
            logger.warning("فیلتر 3: watchlist خالی است!")
            return pd.DataFrame()
        
        logger.info(f"اعمال فیلتر 3: بررسی {len(watchlist)} نماد")
        
        filtered_list = []
        
        for symbol, threshold in watchlist.items():
            # پیدا کردن نماد در دیتافریم
            symbol_df = df[df['symbol'] == symbol]
            
            if symbol_df.empty:
                continue
            
            # بررسی آستانه
            symbol_data = symbol_df.iloc[0]
            if symbol_data['last_price_change_percent'] > threshold:
                # اضافه کردن ستون آستانه برای نمایش
                symbol_row = symbol_data.to_frame().T
                symbol_row['threshold'] = threshold
                filtered_list.append(symbol_row)
                
                logger.info(f"🔔 {symbol}: {symbol_data['last_price_change_percent']:.2f}% > {threshold}%")
        
        if not filtered_list:
            logger.info("فیلتر 3: هیچ نمادی از آستانه عبور نکرد")
            return pd.DataFrame()
        
        filtered = pd.concat(filtered_list, ignore_index=True)
        
        # مرتب‌سازی بر اساس درصد تغییر (نزولی)
        filtered = filtered.sort_values('last_price_change_percent', ascending=False)
        
        logger.info(f"✅ فیلتر 3: {len(filtered)} نماد از آستانه عبور کرد")
        
        return filtered
    
    # ========================================
    # فیلتر 4: صف خرید سنگین در سقف قیمت
    # ========================================
    def filter_4_heavy_buy_queue_at_ceiling(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 4: صف خرید سنگین در سقف دامنه نوسان
        
        شرایط:
        - درصد تغییر قیمت پایانی >= دامنه نوسان (مثلاً 5%)
          یعنی سهم در سقف مثبت قیمت
        - صف فروش صفر یا خیلی کم
        - صف خرید سنگین (بالای 1 میلیارد تومان)
        
        Args:
            df: DataFrame کل سهام
            config: تنظیمات فیلتر (اگر None باشد از config استفاده می‌شود)
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        # بارگذاری تنظیمات
        if config is None:
            from config import CEILING_FILTER_CONFIG
            config = CEILING_FILTER_CONFIG
        
        price_range = config.get('price_range_percent', 5.0)
        min_buy_value = config.get('min_buy_queue_value', 1_000_000_000)
        max_sell_value = config.get('max_sell_queue_value', 10_000_000)
        
        logger.info(f"اعمال فیلتر 4: صف خرید سنگین در سقف (دامنه نوسان: {price_range}%)")
        
        # اعمال فیلتر
        filtered = df[
            (df['final_price_change_percent'] >= price_range) &    # در سقف مثبت دامنه
            (df['sell_order_value'] <= max_sell_value) &           # صف فروش کم/صفر
            (df['buy_order_value'] >= min_buy_value)               # صف خرید سنگین
        ].copy()
        
        if filtered.empty:
            logger.info("فیلتر 4: هیچ سهمی یافت نشد")
            return pd.DataFrame()
        
        # مرتب‌سازی بر اساس ارزش صف خرید (نزولی)
        filtered = filtered.sort_values('buy_order_value', ascending=False)
        
        logger.info(f"✅ فیلتر 4: {len(filtered)} سهم با صف خرید سنگین در سقف")
        
        return filtered
    
    # ========================================
    # فیلتر 5: نسبت پول حقیقی به ارزش معاملات
    # ========================================
    def filter_5_pol_hagigi_ratio(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 5: نسبت پول حقیقی به میانگین ارزش معاملات ماهانه
        
        شرایط:
        - نسبت پول حقیقی به میانگین ماهانه >= 0.5
        - سرانه خرید >= 50 میلیون تومان (واحد: میلیون)
        - قدرت خرید >= 1.5
        
        Args:
            df: DataFrame کل سهام
            config: تنظیمات فیلتر (اگر None باشد از config استفاده می‌شود)
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        # بارگذاری تنظیمات
        if config is None:
            from config import POL_HAGIGI_FILTER_CONFIG
            config = POL_HAGIGI_FILTER_CONFIG
        
        min_ratio = config.get('min_pol_to_value_ratio', 0.5)
        min_sarane = config.get('min_sarane_kharid', 5.0)
        min_godrat = config.get('min_godrat_kharid', 1.5)
        
        logger.info(f"اعمال فیلتر 5: نسبت پول حقیقی (آستانه: {min_ratio})")
        
        # اعمال فیلتر
        filtered = df[
            (df['pol_hagigi_to_avg_monthly_value'] >= min_ratio) &
            (df['sarane_kharid'] >= min_sarane) &
            (df['godrat_kharid'] >= min_godrat)
        ].copy()
        
        if filtered.empty:
            logger.info("فیلتر 5: هیچ سهمی یافت نشد")
            return pd.DataFrame()
        
        # مرتب‌سازی بر اساس نسبت پول حقیقی (نزولی)
        filtered = filtered.sort_values('pol_hagigi_to_avg_monthly_value', ascending=False)
        
        logger.info(f"✅ فیلتر 5: {len(filtered)} سهم با نسبت پول حقیقی بالا")
        
        return filtered
    
    # ========================================
    # فیلتر 6: تیک و ساعت (رشد قیمتی در آخر معاملات)
    # ========================================
    def filter_6_tick_and_time(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 6: تیک و ساعت - شناسایی سهامی که در آخر روز تیک مثبت خوردند
        
        شرایط:
        - 0.98 × قیمت اولین > قیمت کف (کف بالاتر از 98% اولین)
        - 0.98 × قیمت آخرین > قیمت اولین (آخرین حداقل 2% بالاتر از اولین)
        - درصد تغییر آخرین - درصد تغییر پایانی > 2% (تیک مثبت)
        
        Args:
            df: DataFrame کل سهام
            config: تنظیمات فیلتر (اگر None باشد از config استفاده می‌شود)
            
        Returns:
            DataFrame سهام فیلتر شده
        """
        if df.empty:
            return df
        
        # بارگذاری تنظیمات
        if config is None:
            from config import TICK_FILTER_CONFIG
            config = TICK_FILTER_CONFIG
        
        first_to_low_ratio = config.get('first_to_low_ratio', 0.98)
        last_to_first_ratio = config.get('last_to_first_ratio', 0.98)
        tick_diff_percent = config.get('tick_diff_percent', 2.0)
        
        logger.info(f"اعمال فیلتر 6: تیک و ساعت")
        
        # محاسبه اختلاف درصد تغییر آخرین و پایانی
        df_copy = df.copy()
        df_copy['tick_diff'] = (
            df_copy['last_price_change_percent'] - df_copy['final_price_change_percent']
        )
        
        # اعمال فیلتر
        filtered = df_copy[
            (first_to_low_ratio * df_copy['first_price'] > df_copy['low_price']) &     # کف بالاتر از 98% اولین
            (last_to_first_ratio * df_copy['last_price'] > df_copy['first_price']) &    # آخرین بالاتر از اولین
            (df_copy['tick_diff'] > tick_diff_percent)                                   # تیک مثبت بیش از 2%
        ].copy()
        
        if filtered.empty:
            logger.info("فیلتر 6: هیچ سهمی یافت نشد")
            return pd.DataFrame()
        
        # مرتب‌سازی بر اساس اختلاف تیک (نزولی)
        filtered = filtered.sort_values('tick_diff', ascending=False)
        
        logger.info(f"✅ فیلتر 6: {len(filtered)} سهم با تیک مثبت در آخر روز")
        
        return filtered
    
    # ========================================
    # فیلتر 7: حجم مشکوک
    # ========================================
    def filter_7_suspicious_volume(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 7: حجم مشکوک - ارزش معاملات بسیار بالاتر از میانگین
        
        شرایط:
        - ارزش معاملات به میانگین ماهانه > 2
        
        Args:
            df: DataFrame کل سهام
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
        
        # مرتب‌سازی بر اساس نسبت ارزش (نزولی)
        filtered = filtered.sort_values('value_to_avg_monthly_value', ascending=False)
        
        logger.info(f"✅ فیلتر 7: {len(filtered)} سهم با حجم مشکوک")
        
        return filtered
    
    # ========================================
    # فیلتر 8: نوسان‌گیری (خرید در کف)
    # ========================================
    def filter_8_swing_trade(self, df: pd.DataFrame, config: dict = None) -> pd.DataFrame:
        """
        فیلتر 8: نوسان‌گیری - شناسایی فرصت خرید در کف
        
        شرایط:
        - پایین‌ترین قیمت = حداقل آستانه مجاز (مثلاً -5% دامنه)
        - آخرین قیمت > حداقل آستانه مجاز
        - قدرت خرید >= 2.0
        - سرانه خرید >= 50 میلیون
        - ارزش معاملات >= میانگین ماهانه
        - درصد آخرین < -2%
        
        Args:
            df: DataFrame کل سهام
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
            (df['low_price_change_percent'] == min_allowed) &              # کف در آستانه منفی
            (df['last_price_change_percent'] > min_allowed) &              # آخرین بالاتر از کف
            (df['godrat_kharid'] >= min_godrat) &                          # قدرت خرید قوی
            (df['sarane_kharid'] >= min_sarane) &                          # سرانه بالا
            (df['value_to_avg_monthly_value'] >= 1.0) &                    # ارزش >= میانگین
            (df['last_price_change_percent'] < max_last_change)            # هنوز منفی
        ].copy()
        
        if filtered.empty:
            logger.info("فیلتر 8: هیچ سهمی یافت نشد")
            return pd.DataFrame()
        
        # مرتب‌سازی بر اساس قدرت خرید (نزولی)
        filtered = filtered.sort_values('godrat_kharid', ascending=False)
        
        logger.info(f"✅ فیلتر 8: {len(filtered)} سهم برای نوسان‌گیری")
        
        return filtered
    
    # ========================================
    # فیلتر 9: یک ساعت اول (فقط 9:00 - 10:00)
    # ========================================
    def filter_9_first_hour(self, df: pd.DataFrame, config: dict = None, current_hour: int = None) -> pd.DataFrame:
        """
        فیلتر 9: یک ساعت اول - فقط در ساعت اول بازار (9:00 - 10:00)
        
        شرایط:
        - ساعت فعلی بین 9 تا 10 (به وقت تهران)
        - ارزش معاملات به میانگین ماهانه >= 1
        
        Args:
            df: DataFrame کل سهام
            config: تنظیمات فیلتر
            current_hour: ساعت فعلی (برای تست، اگر None باشد از سیستم می‌گیره)
            
        Returns:
            DataFrame سهام فیلتر شده یا خالی (اگه خارج از ساعت کاری)
        """
        if df.empty:
            return df
        
        # بررسی ساعت (با timezone تهران)
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
        
        # اگه خارج از بازه زمانی، خالی برگردون
        if not (start_hour <= current_hour < end_hour):
            logger.info(f"فیلتر 9: خارج از بازه زمانی ({start_hour}-{end_hour}). ساعت فعلی تهران: {current_hour}")
            return pd.DataFrame()
        
        logger.info(f"اعمال فیلتر 9: یک ساعت اول (ساعت تهران: {current_hour})")
        
        filtered = df[
            df['value_to_avg_monthly_value'] >= min_ratio
        ].copy()
        
        if filtered.empty:
            logger.info("فیلتر 9: هیچ سهمی یافت نشد")
            return pd.DataFrame()
        
        # مرتب‌سازی بر اساس نسبت ارزش (نزولی)
        filtered = filtered.sort_values('value_to_avg_monthly_value', ascending=False)
        
        logger.info(f"✅ فیلتر 9: {len(filtered)} سهم در ساعت اول")
        
        return filtered
    
    # ========================================
    # فیلتر 10: (منتظر اطلاعات)
    # ========================================
    def filter_10_placeholder(self, df: pd.DataFrame) -> pd.DataFrame:
        """فیلتر 10: در انتظار تعریف"""
        return pd.DataFrame()
    
    # ========================================
    # اعمال همه فیلترها
    # ========================================
    def apply_all_filters(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        اعمال همه فیلترها بر روی DataFrame
        
        Args:
            df: DataFrame کل سهام
            
        Returns:
            دیکشنری شامل نتایج هر فیلتر
        """
        logger.info(f"شروع اعمال فیلترها بر روی {len(df)} سهم")
        
        results = {
            'filter_1_strong_buying': self.filter_1_strong_buying_power(df),
            'filter_2_sarane_cross': self.filter_2_sarane_kharid_cross(df),
            'filter_3_watchlist': self.filter_3_watchlist_symbols(df),
            'filter_4_ceiling_queue': self.filter_4_heavy_buy_queue_at_ceiling(df),
            'filter_5_pol_hagigi_ratio': self.filter_5_pol_hagigi_ratio(df),
            'filter_6_tick_time': self.filter_6_tick_and_time(df),
            'filter_7_suspicious_volume': self.filter_7_suspicious_volume(df),
            'filter_8_swing_trade': self.filter_8_swing_trade(df),
            'filter_9_first_hour': self.filter_9_first_hour(df),
            # فیلتر 10 اینجا اضافه می‌شود
        }
        
        # خلاصه نتایج
        total_filtered = sum(len(v) for v in results.values())
        logger.info(f"✅ جمع {total_filtered} سهم از {len(results)} فیلتر یافت شد")
        
        self.filters_results = results
        return results
    
    # ========================================
    # توابع کمکی برای آمار
    # ========================================
    def get_market_summary(self, df: pd.DataFrame) -> Dict:
        """
        تولید خلاصه کلی بازار
        
        Args:
            df: DataFrame کل سهام
            
        Returns:
            دیکشنری حاوی آمار کلی
        """
        if df.empty:
            return {}
        
        summary = {
            'total_stocks': len(df),
            'positive_stocks': len(df[df['final_price_change_percent'] > 0]),
            'negative_stocks': len(df[df['final_price_change_percent'] < 0]),
            'neutral_stocks': len(df[df['final_price_change_percent'] == 0]),
            'avg_change_percent': df['final_price_change_percent'].mean(),
            'total_value': df['value'].sum(),
            'total_volume': df['volume'].sum(),
            'total_pol_hagigi': df['pol_hagigi'].sum(),
        }
        
        return summary
    
    def get_top_industries(self, df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
        """
        برترین صنایع بر اساس ارزش معاملات و ورود پول
        
        Args:
            df: DataFrame کل سهام
            n: تعداد صنایع برتر
            
        Returns:
            DataFrame حاوی آمار صنایع برتر
        """
        if df.empty:
            return pd.DataFrame()
        
        industry_stats = df.groupby('industry_name').agg({
            'symbol': 'count',
            'value': 'sum',
            'pol_hagigi': 'sum',
            'final_price_change_percent': lambda x: (x > 0).sum()
        }).reset_index()
        
        industry_stats.columns = [
            'industry_name', 
            'stock_count', 
            'total_value', 
            'total_pol_hagigi',
            'positive_count'
        ]
        
        # محاسبه تعداد منفی
        negative_counts = df.groupby('industry_name').apply(
            lambda x: (x['final_price_change_percent'] < 0).sum()
        ).reset_index(name='negative_count')
        
        industry_stats = industry_stats.merge(negative_counts, on='industry_name')
        
        # مرتب‌سازی بر اساس ارزش معاملات
        industry_stats = industry_stats.sort_values('total_value', ascending=False).head(n)
        
        return industry_stats
    
    def get_top_stocks(self, df: pd.DataFrame, by: str = 'final_price_change_percent', n: int = 10) -> pd.DataFrame:
        """
        برترین سهام بر اساس معیار مشخص
        
        Args:
            df: DataFrame کل سهام
            by: ستون مورد نظر برای مرتب‌سازی
            n: تعداد سهام برتر
            
        Returns:
            DataFrame حاوی سهام برتر
        """
        if df.empty:
            return pd.DataFrame()
        
        return df.nlargest(n, by)
    
    def format_number(self, num: float) -> str:
        """
        فرمت‌بندی اعداد به فارسی
        
        توجه: اعداد مالی از قبل به میلیارد تبدیل شده‌اند
        
        Args:
            num: عدد ورودی
            
        Returns:
            رشته فرمت شده
        """
        if pd.isna(num):
            return "0"
        
        # اگه عدد بزرگ‌تر از 1000 میلیارد
        if abs(num) >= 1000:
            return f"{num:.1f} هزار میلیارد"
        # اگه عدد به میلیارد
        elif abs(num) >= 1:
            return f"{num:.2f} میلیارد"
        # اگه عدد خیلی کوچیک (کمتر از 1 میلیارد)
        elif abs(num) >= 0.001:
            return f"{num*1000:.1f} میلیون"
        else:
            return f"{num:.3f}"