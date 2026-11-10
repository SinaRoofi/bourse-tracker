"""
ساخت خلاصه‌ی روزانه به‌صورت یک Rich Message (فیلد html در Bot API 10.1+).

این ماژول فقط HTML می‌سازه و هیچ ارسالی انجام نمی‌ده؛ ارسال با
TelegramAlert.send_rich_message انجام می‌شه. فرمت‌کننده‌های قدیمی
DailySummaryGenerator دست‌نخورده می‌مونن و fallback هستن.

نکته: عمداً از حالت html استفاده شده (نه markdown)، چون تو Markdown ریچ هر
خطی که با # شروع بشه هدینگ حساب می‌شه و هشتگ نمادها خراب می‌شد.
"""

import html as _html
import re
from typing import Callable, Dict, List, Optional

from utils.industry_market_fetcher import RIAL_TO_MILLION_TOMAN, RIAL_TO_TRILLION_TOMAN

# سقف‌های مستندشده‌ی تلگرام: ۳۲۷۶۸ کاراکتر متن و ۵۰۰ بلاک (ردیف جدول و آیتم لیست هم بلاک حساب می‌شن).
# کمی پایین‌تر از سقف نگه می‌داریم تا اگه پیام بزرگ شد، به‌جای رد شدن، fallback بشه.
MAX_TEXT_CHARS = 30000
MAX_BLOCKS = 450

_LATIN_ISOLATE_START = "\u2068"
_LATIN_ISOLATE_END = "\u2069"
MA20 = f"{_LATIN_ISOLATE_START}MA20{_LATIN_ISOLATE_END}"

_BLOCK_TAGS = re.compile(r"<(?:tr|li|p|h[1-6]|details|table|footer|hr|blockquote)\b", re.I)
_ANY_TAG = re.compile(r"<[^>]+>")


def _e(value) -> str:
    return _html.escape(str(value), quote=False)


def _hashtag(symbol: str) -> str:
    if not symbol:
        return ""
    return str(symbol).replace(" ", "_").replace("\u200c", "_").strip()


def _td(content: str) -> str:
    return f'<td align="center">{content}</td>'


def _th(content: str) -> str:
    return f'<th align="center">{content}</th>'


def _table(headers: List[str], rows: List[List[str]]) -> str:
    """جدول ساده؛ همه‌ی ستون‌ها وسط‌چین. محتوای سلول‌ها باید از قبل escape شده باشه."""
    head = "<tr>" + "".join(_th(h) for h in headers) + "</tr>"
    body = "".join("<tr>" + "".join(_td(c) for c in row) + "</tr>" for row in rows)
    return f"<table bordered compact>{head}{body}</table>"


def _price_cell(price_change_percent: Optional[float]) -> str:
    if price_change_percent is None:
        return "—"
    arrow = "▲" if price_change_percent >= 0 else "▼"
    return f"{arrow}{abs(price_change_percent):.1f}%"


def _sarane_heavy_value(item: dict) -> str:
    ratio = f"{item['value']:.1f}x"
    sarane = item.get("sarane_kharid")
    detail = f"M {sarane:.0f} ({ratio})" if sarane is not None else f"({ratio})"
    return f"{_LATIN_ISOLATE_START}{_e(detail)}{_LATIN_ISOLATE_END}"


def _format_queue_value(value: Optional[float]) -> str:
    if value is None:
        return "؟"
    if value <= 0:
        return "خارج از سقف"
    if value < 1:
        return f"{value * 1000:.0f}M"
    return f"{value:.1f}B" if value < 10 else f"{value:.0f}B"


def within_limits(rich_html: str) -> bool:
    text_len = len(_ANY_TAG.sub("", rich_html))
    blocks = len(_BLOCK_TAGS.findall(rich_html))
    return text_len <= MAX_TEXT_CHARS and blocks <= MAX_BLOCKS


# ----------------------------------------------------------------------
# بخش‌های خلاصه‌ی بازار / صندوق‌ها
# ----------------------------------------------------------------------
def _combined_totals_table(combined_totals: Dict) -> str:
    total_value = combined_totals.get("total_value", 0.0)
    total_value_hemat = total_value / RIAL_TO_TRILLION_TOMAN
    avg_month = combined_totals.get("total_value_avg_month", 0.0)
    value_ratio_pct = total_value / avg_month * 100 if avg_month > 0 else 0.0

    pol_hemat = combined_totals.get("total_pol_hagigi", 0.0) / RIAL_TO_TRILLION_TOMAN
    pol_pct = combined_totals.get("market_pol_to_avg_month_pct", 0.0)
    pol_emoji = "🟢" if pol_hemat >= 0 else "🔴"

    buy_m = combined_totals.get("market_sarane_kharid", 0.0) / RIAL_TO_MILLION_TOMAN
    buy_ratio = combined_totals.get("market_sarane_kharid_ratio", 0.0)
    buy_pct = buy_ratio * 100 if buy_ratio > 0 else 0.0

    sell_m = combined_totals.get("market_sarane_forosh", 0.0) / RIAL_TO_MILLION_TOMAN
    sell_ratio = combined_totals.get("market_sarane_forosh_ratio", 0.0)
    sell_pct = sell_ratio * 100 if sell_ratio > 0 else 0.0

    rows = [
        ["ارزش معاملات", f"{total_value_hemat:,.2f} همت", f"{value_ratio_pct:.0f}٪"],
        [f"{pol_emoji} ورود پول حقیقی", f"{abs(pol_hemat):,.2f} همت", f"{pol_pct:+.0f}٪"],
        ["سرانه خرید", f"{buy_m:,.0f} میلیون تومان", f"{buy_pct:.0f}٪"],
        ["سرانه فروش", f"{sell_m:,.0f} میلیون تومان", f"{sell_pct:.0f}٪"],
    ]
    return _table(["شاخص", "مقدار", f"نسبت به {MA20}"], rows)


def _market_blocks(
    analysis: Dict,
    unit_label: str,
    combined_totals: Optional[Dict],
    show_breadth: bool,
    buy_queue_industries: Optional[List[dict]],
    top_n: int,
) -> str:
    """بدنه‌ی پیام خلاصه‌ی بازار؛ هر بخشِ خالی کلاً حذف می‌شه (مثل نسخه‌ی قبلی)."""
    out = []
    breadth = analysis.get("breadth", {})

    if combined_totals:
        out.append("<h4>📌 کل بازار</h4>" + _combined_totals_table(combined_totals))

    total_active = breadth.get("total_active", 0)
    if show_breadth and total_active:
        out.append(
            f"<h4>🌡️ نبض بازار (از {total_active} {_e(unit_label)} فعال)</h4>"
            "<ul>"
            f"<li>ورود پول مثبت: {breadth.get('pol_positive_count', 0)} {_e(unit_label)}</li>"
            f"<li>سرانه خرید بالای ماهانه: {breadth.get('sarane_above_count', 0)} {_e(unit_label)}</li>"
            f"<li>بازدهی مثبت: {breadth.get('return_positive_count', 0)} {_e(unit_label)}</li>"
            "</ul>"
        )

    value_above = analysis.get("value_above_avg", [])[:top_n]
    if value_above:
        rows = [
            [str(i), _e(r["name"]), f"+{r['pct_week']:.0f}٪", f"+{r['pct_month']:.0f}٪"]
            for i, r in enumerate(value_above, 1)
        ]
        out.append(
            "<h4>📈 ارزش معاملات نسبی بالا</h4>"
            + _table(["#", _e(unit_label), "هفتگی", "ماهانه"], rows)
        )

    sarane_above = analysis.get("sarane_above_month", [])[:top_n]
    if sarane_above:
        rows = [
            [str(i), _e(r["name"]), f"{r['sarane_ratio']:.2f}×"]
            for i, r in enumerate(sarane_above, 1)
        ]
        out.append(
            "<h4>🛒 سرانه خرید بیشتر از میانگین ماهانه</h4>"
            + _table(["#", _e(unit_label), "نسبت"], rows)
        )

    pol_top = analysis.get("pol_to_avg_month", [])[:top_n]
    if pol_top:
        rows = [
            [str(i), _e(r["name"]), f"{r['pol_to_avg_month_pct']:+.0f}٪"]
            for i, r in enumerate(pol_top, 1)
        ]
        out.append(
            f"<h4>⚡ قدرت پول {_e(unit_label)}‌ها</h4>"
            + _table(["#", _e(unit_label), "ورود پول / میانگین ماهانه"], rows)
        )

    return_top = analysis.get("return_ranked", [])[:top_n]
    if return_top:
        rows = [
            [str(i), _e(r["name"]), f"{r['group_return_equal_weight']:+.2f}٪"]
            for i, r in enumerate(return_top, 1)
        ]
        out.append(
            "<h4>🏆 بیشترین بازدهی</h4>" + _table(["#", _e(unit_label), "بازدهی"], rows)
        )

    if buy_queue_industries:
        items = []
        for ind in buy_queue_industries[:top_n]:
            count = ind["symbol_count"]
            pct = ind.get("participation_pct")
            universe = ind.get("universe_count")
            count_str = (
                f"{count}/{universe} نماد ({pct:.0f}٪)" if pct is not None and universe else f"{count} نماد"
            )
            line = f"<b>{_e(ind['industry_name'])}</b> — {count_str}"
            symbols = ind.get("symbols", [])
            if symbols:
                values = ind.get("symbol_values", {})
                tags = []
                for sym in symbols:
                    tag = f"#{_e(_hashtag(sym))}"
                    if sym in values:
                        tag += f" ({_e(_format_queue_value(values[sym]))})"
                    tags.append(tag)
                line += "<br>" + " ".join(tags)
            items.append(f"<li>{line}</li>")
        out.append("<h4>🎯 صنایع پیشرو صف خرید</h4><ol>" + "".join(items) + "</ol>")

    return "".join(out)


# ----------------------------------------------------------------------
# برترین صنایع / برترین نمادها / پرتکرارها
# ----------------------------------------------------------------------
def _top_industries_block(top_industries: List[dict]) -> str:
    if not top_industries:
        return ""
    items = []
    for industry in top_industries:
        count = industry["symbol_count"]
        universe = industry.get("universe_count")
        pct = industry.get("participation_pct")
        total_alerts = industry.get("total_alert_count", 0)
        if pct is not None and universe:
            count_str = f"{total_alerts} هشدار ({count}/{universe} نماد، {pct:.0f}٪)"
        else:
            count_str = f"{total_alerts} هشدار ({count} نماد)"
        tags = " ".join(f"#{_e(_hashtag(s))}" for s in industry["symbols"])
        line = f"<b>{_e(industry['industry_name'])}</b> — {count_str}"
        if tags:
            line += f"<br>{tags}"
        items.append(f"<li>{line}</li>")
    return "<h3>🏭 برترین صنایع</h3><ol>" + "".join(items) + "</ol>"


def _top_filters_block(top_per_filter: Dict[str, List[dict]], filter_meta: Dict[str, dict]) -> str:
    if not top_per_filter:
        return ""
    sections = []
    for filter_name, items in top_per_filter.items():
        meta = filter_meta.get(filter_name, {})
        emoji = meta.get("emoji", "📌")
        title = meta.get("title", filter_name)
        unit = meta.get("unit", "")
        fmt = meta.get("format", ".2f")
        multiplier = meta.get("multiplier", 1)
        heavy = meta.get("row_style") == "sarane_heavy"

        rows = []
        for i, item in enumerate(items, 1):
            if heavy:
                value_cell = _sarane_heavy_value(item)
            else:
                value_cell = _e(format(item["value"] * multiplier, fmt) + (f" {unit}" if unit else ""))
            rows.append([
                str(i),
                f"#{_e(_hashtag(item['symbol']))}",
                _price_cell(item.get("price_change_percent")),
                value_cell,
            ])
        sections.append(
            f"<details><summary>{emoji} {_e(title)}</summary>"
            + _table(["#", "نماد", "قیمت پایانی", "مقدار"], rows)
            + "</details>"
        )
    return "<h3>🏆 برترین نمادها</h3>" + "".join(sections)


def _frequent_symbols_block(
    frequent_symbols: Dict[str, int],
    total_unique_symbols: int,
    medals: Dict[int, str],
) -> str:
    out = ["<h3>📊 خلاصه هشدارها</h3>"]
    if frequent_symbols:
        groups: Dict[int, List[str]] = {}
        for symbol, count in frequent_symbols.items():
            groups.setdefault(count, []).append(symbol)
        lines = []
        for rank, count in enumerate(sorted(groups, reverse=True), 1):
            tags = " ".join(f"#{_e(_hashtag(s))}" for s in sorted(groups[count]))
            lines.append(f"{medals.get(rank, '▪️')} <b>({count}×)</b> {tags}")
        out.append("<p>" + "<br>".join(lines) + "</p>")
    else:
        out.append("<p>هیچ نماد پرتکراری نبود</p>")
    out.append(
        f"<p>🎯 {len(frequent_symbols)} نماد پرتکرار از {total_unique_symbols} نماد هشداردهنده</p>"
    )
    return "".join(out)


# ----------------------------------------------------------------------
# ورودی اصلی
# ----------------------------------------------------------------------
def build_daily_summary_html(
    *,
    industries_analysis: Optional[Dict],
    funds_analysis: Optional[Dict],
    combined_totals: Optional[Dict],
    buy_queue_industries: Optional[List[dict]],
    top_industries: List[dict],
    top_per_filter: Dict[str, List[dict]],
    frequent_symbols: Dict[str, int],
    total_unique_symbols: int,
    filter_meta: Dict[str, dict],
    medals: Dict[int, str],
    date_str: str,
    time_str: str,
    channel_name: str,
    top_n: int = 8,
) -> str:
    """
    خلاصه‌ی روزانه رو در یک پیام Rich برمی‌گردونه. صندوق‌ها و هر فیلتر در
    بلاک جمع‌شونده‌ان تا پیام کوتاه بمونه؛ بقیه باز نمایش داده می‌شن.
    """
    parts = [f"<h2>📅 خلاصه روزانه بازار — {_e(date_str)}</h2>"]

    if industries_analysis:
        body = _market_blocks(
            industries_analysis, "صنعت", combined_totals, True, buy_queue_industries, top_n
        )
        if body:
            parts.append("<h3>📊 خلاصه معاملات بازار</h3>" + body)

    if funds_analysis:
        body = _market_blocks(funds_analysis, "صندوق", None, False, None, top_n)
        if body:
            parts.append("<details><summary>🏦 خلاصه معاملات صندوق‌ها</summary>" + body + "</details>")

    parts.append(_top_industries_block(top_industries))
    parts.append(_top_filters_block(top_per_filter, filter_meta))
    parts.append(_frequent_symbols_block(frequent_symbols, total_unique_symbols, medals))
    parts.append(f"<footer>📅 {_e(date_str)} | 🕐 {_e(time_str)}<br>📢 {_e(channel_name)}</footer>")

    return "".join(p for p in parts if p)
