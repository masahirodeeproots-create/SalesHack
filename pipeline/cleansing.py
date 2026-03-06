"""
クレンジングロジック（型別）
===========================
DATABASE_DESIGN.md Section 7 に定義されたルールを実装。

型一覧:
  長テキスト / 短テキスト・タグ / 数値（円） / 数値（人） / 数値（率）
  日付 / 住所 / 電話番号 / 郵便番号 / URL / JSON
"""

import json
import re
import unicodedata


# ---------------------------------------------------------------------------
# 共通ユーティリティ
# ---------------------------------------------------------------------------

_ZEN_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
_ZEN_TO_HAN = str.maketrans(
    "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    "０１２３４５６７８９　",
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789 ",
)


def _to_str(v) -> str:
    """None / NaN → 空文字列。それ以外は文字列化。"""
    if v is None:
        return ""
    s = str(v)
    if s in ("None", "nan", "NaN", "NaT"):
        return ""
    return s


# ---------------------------------------------------------------------------
# 型別クレンジング関数
# ---------------------------------------------------------------------------


def clean_long_text(v) -> str:
    """長テキスト: trim、連続空白 → 単一スペース"""
    s = _to_str(v).strip()
    if not s:
        return ""
    return re.sub(r"\s+", " ", s)


def clean_short_text(v) -> str:
    """短テキスト/タグ: 全角→半角、前後スペース除去"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_TO_HAN).strip()
    return s


def clean_money(v) -> str:
    """
    数値（円）: 全ての金額表記を「円」単位の純粋な整数文字列に変換する。

    対応パターン:
      - 「5億9,414万円」→ 594140000
      - 「1,234百万円」→ 1234000000
      - 「100,000,000円」→ 100000000
      - 「1兆387億」→ 10387000000000
      - 「103億」→ 10300000000
      - 「25699百万円」→ 25699000000
      - 「653万円」→ 6530000
      - 「14億円」→ 1400000000
      - 「1億円（株主：...）」→ 100000000
      - みんかぶ百万円単位の裸数値「1,672,377」→ 1672377（そのまま）

    注釈（括弧内テキスト・期間情報）は除去する。
    複数金額が含まれる場合は最初の1つのみ抽出する。
    変換不能な場合は空文字列を返す。
    """
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace(",", "").replace("，", "")

    # 括弧内の注釈を除去（「（2024年3月期）」「（連結）」等）
    s = re.sub(r"[（(][^）)]*[）)]", "", s)
    s = s.strip()
    if not s:
        return ""

    return _parse_japanese_money(s)


def _parse_japanese_money(s: str) -> str:
    """
    日本語金額表記を円単位の整数文字列に変換する。
    複数金額が含まれる場合は最初の1つのみ抽出する。
    変換不能な場合は空文字列を返す。
    """
    # 複数金額が空白で並ぶ場合は最初の金額ブロックだけ使う
    # 「100億7千万円 93億6千万円」→「100億7千万円」
    # 金額ブロック: 数字+日本語単位が連続する部分 + 任意の「円」
    first_match = re.match(
        r"([\d.]+\s*(?:兆|億|百万|千万|万|千)[\d.兆億百万千\s]*円?)", s
    )
    if first_match:
        s = first_match.group(1)
    else:
        # 「100000000円」等の純数値+円パターン
        first_match = re.match(r"(\d+)\s*円?", s)
        if first_match:
            s = first_match.group(0)

    # 「円」を除去
    s = s.rstrip("円").strip()

    # 「百万」を含む場合: 例 "25699百万" → 25699 * 10^6
    m = re.match(r"^(\d+(?:\.\d+)?)\s*百万$", s)
    if m:
        return str(int(float(m.group(1)) * 10**6))

    # 複合単位パターンを累積加算で解析
    # 「千万」を先に処理（「7千万」= 7 * 10^7）
    # 順序重要: 兆 > 億 > 千万 > 百万 > 万 > 千
    UNITS = [
        ("兆", 10**12),
        ("億", 10**8),
        ("千万", 10**7),
        ("百万", 10**6),
        ("万", 10**4),
        ("千", 10**3),
    ]

    total = 0
    found_unit = False
    remaining = s

    for unit_name, multiplier in UNITS:
        pattern = rf"(\d+(?:\.\d+)?)\s*{unit_name}"
        m = re.search(pattern, remaining)
        if m:
            total += int(float(m.group(1)) * multiplier)
            remaining = remaining[m.end():]
            found_unit = True

    if found_unit:
        # 残りに端数の数字があれば加算（「5億9414万3000」の「3000」部分）
        m = re.match(r"(\d+)", remaining.strip())
        if m:
            total += int(m.group(1))
        return str(total)

    # 単位なし純数値: 「100000000」
    m = re.match(r"^(\d+)$", s)
    if m:
        return m.group(1)

    # どれにもマッチしない場合
    return ""


def clean_money_million(v) -> str:
    """
    みんかぶ専用: 百万円単位のカンマ付き数値を円単位の整数文字列に変換する。
    例: 「1,672,377」→ 「1672377000000」... ではなく、
    みんかぶの数値はそのまま百万円単位として扱い、円に変換する。
    例: 「1,672,377」(百万円) → 「1672377000000」

    ※実データ確認: みんかぶの売上高・営業利益等は百万円単位。
    """
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace(",", "").replace("，", "")
    # 符号付き数値に対応（フリーCF等でマイナスあり）
    m = re.match(r"^(-?\d+)$", s)
    if m:
        return str(int(m.group(1)) * 1_000_000)
    return ""


def clean_salary(v) -> str:
    """
    年収・給与: 万円単位の表記を円単位の整数文字列に変換する。
    例: 「653万円」→ 「6530000」
    """
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace(",", "").replace("，", "")
    # 「XXX万円」パターン
    m = re.match(r"^(\d+)\s*万円?$", s)
    if m:
        return str(int(m.group(1)) * 10_000)
    # 純数値（円単位）
    m = re.match(r"^(\d+)$", s)
    if m:
        return m.group(1)
    return ""


def clean_people_count(v) -> str:
    """
    数値（人数）: 単体優先で数値のみ抽出する。単位・注釈は除去。

    単体/単独の明示がある場合はその数値を採用。
    なければ先頭の数値を採用（通常は単体）。

    例:
      「1,042名（2025年3月現在）」→ 「1042」
      「単独　2,568名　連結　112,551名」→ 「2568」
      「単体：8,981人 連結：112,551名」→ 「8981」
      「277名（2024年12月現在）（連結480名）」→ 「277」
      「100名」→ 「100」
      「15人」→ 「15」
      「10名以下」→ 「10」
      「11～15名」→ 「11」（範囲の場合は下限を採用）
    """
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace(",", "").replace("，", "")
    # 「単独」「単体」の後の数値を優先
    m = re.search(r"(?:単独|単体)[：:\s]*(\d+)", s)
    if m:
        return m.group(1)
    # それ以外は先頭の数値を抽出
    m = re.match(r"(\d+)", s)
    if m:
        return m.group(1)
    return ""


def clean_ratio(v) -> str:
    """数値（率）: % を除去して数値のみ保持。テキスト型のまま。"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace("%", "").replace("％", "").strip()
    return s


def clean_numeric_value(v) -> str:
    """
    数値＋単位テキスト: 先頭の数値部分のみ抽出する。
    注釈（括弧内・後続テキスト）は除去する。

    例:
      「10.0時間 （2023年度実績）」→ 「10.0」
      「16.1日 （2023年度実績）」→ 「16.1」
      「10.5年 （2024年10月時点）」→ 「10.5」
      「35.5歳 （2023年4月時点）」→ 「35.5」
      「20.0時間 （2022年度実績） 残業はほとんどありません！」→ 「20.0」
    """
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    # 先頭の数値（整数 or 小数）を抽出
    m = re.match(r"(\d+(?:\.\d+)?)", s)
    if m:
        return m.group(1)
    return ""


def parse_female_ratio(v) -> dict[str, str]:
    """
    女性管理職比率テキストを分解する。

    入力例: 「項目 女性 役員 40.0% 管理職 7.7% （2024年10月時点）」
    出力: {"女性役員比率": "40.0", "女性管理職比率": "7.7"}

    パースできない場合（「－」等）は空辞書を返す。
    """
    s = _to_str(v).strip()
    if not s or s == "－":
        return {}
    s = s.translate(_ZEN_DIGITS)
    m = re.search(r"役員\s*([\d.]+)\s*%.*管理職\s*([\d.]+)\s*%", s)
    if m:
        return {
            "女性役員比率": m.group(1),
            "女性管理職比率": m.group(2),
        }
    return {}


def parse_childcare_leave(v) -> dict[str, str]:
    """
    育児休業取得者数テキストを分解する。

    入力例: 「項目 男性 女性 育休取得者数 0人 1人 取得対象者数 1人 1人
              育休取得率 0.0% 100.0% （2023年度実績）」
    出力: {
        "育休取得者数_男性": "0", "育休取得者数_女性": "1",
        "育休対象者数_男性": "1", "育休対象者数_女性": "1",
        "育休取得率_男性": "0.0", "育休取得率_女性": "100.0",
    }
    """
    s = _to_str(v).strip()
    if not s or s in ("－", "実績なし"):
        return {}
    s = s.translate(_ZEN_DIGITS)
    result = {}
    # 育休取得者数 X人 X人
    m = re.search(r"育休取得者数\s*(\d+)\s*人\s*(\d+)\s*人", s)
    if m:
        result["育休取得者数_男性"] = m.group(1)
        result["育休取得者数_女性"] = m.group(2)
    # 取得対象者数 X人 X人
    m = re.search(r"取得対象者数\s*(\d+)\s*人\s*(\d+)\s*人", s)
    if m:
        result["育休対象者数_男性"] = m.group(1)
        result["育休対象者数_女性"] = m.group(2)
    # 育休取得率 XX.X% XX.X%（「－」も許容）
    m = re.search(r"育休取得率\s*([\d.]+|－)\s*%?\s*([\d.]+|－)\s*%?", s)
    if m:
        male = m.group(1) if m.group(1) != "－" else ""
        female = m.group(2) if m.group(2) != "－" else ""
        if male:
            result["育休取得率_男性"] = male
        if female:
            result["育休取得率_女性"] = female
    return result


def parse_retention(v) -> dict[str, str]:
    """
    過去3年間採用実績テキストから直近年度のデータを抽出する。

    入力例: 「年度 採用者数 離職者数 定着率 2023年度 37人 0人 100.0%
              2022年度 28人 2人 92.8% 2021年度 29人 9人 68.9%」
    出力: {"直近採用者数": "37", "直近離職者数": "0", "直近定着率": "100.0"}

    最も新しい年度のデータを採用する。
    """
    s = _to_str(v).strip()
    if not s or s == "－":
        return {}
    s = s.translate(_ZEN_DIGITS)
    # 「YYYY年度 X人 X人 XX.X%」パターンを全て抽出
    matches = re.findall(
        r"(\d{4})\s*年度\s*(\d+)\s*人\s*(\d+)\s*人\s*([\d.]+)\s*%", s
    )
    if not matches:
        # 「採用実績なし」等のパターン
        m = re.search(r"(\d{4})\s*年度\s*採用実績なし", s)
        if m:
            return {"直近採用者数": "0", "直近離職者数": "0", "直近定着率": ""}
        return {}
    # 最も新しい年度を採用
    latest = max(matches, key=lambda x: int(x[0]))
    return {
        "直近採用者数": latest[1],
        "直近離職者数": latest[2],
        "直近定着率": latest[3],
    }


def clean_date(v) -> str:
    """日付: YYYY年M月 → YYYY-MM。YYYY年 → YYYY"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    # YYYY年MM月DD日 → YYYY-MM
    m = re.match(r"(\d{4})\s*年\s*(\d{1,2})\s*月", s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}"
    # YYYY年 → YYYY
    m = re.match(r"(\d{4})\s*年", s)
    if m:
        return m.group(1)
    return s


def clean_address(v) -> str:
    """住所: 全角→半角数字、郵便番号プレフィックス除去、ダッシュ統一"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    # 先頭の郵便番号プレフィックスを除去（〒XXX-XXXX）
    s = re.sub(r"^〒?\s*\d{3}[-－‐ー]\d{4}\s*", "", s).strip()
    # 全角ダッシュ系を半角に統一（ー U+30FC カタカナ長音符は除外）
    s = s.replace("－", "-").replace("‐", "-").replace("−", "-")
    # カタカナ長音符 ー (U+30FC) は数字間のみハイフンに変換（タワー等は保護）
    s = re.sub(r"(\d)ー(?=\d)", r"\1-", s)
    return s


def clean_phone(v) -> str:
    """電話番号: 全角→半角、XX-XXXX-XXXX 形式に統一"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    s = s.replace("（", "(").replace("）", ")").replace("ー", "-").replace("‐", "-")
    # 括弧表記を除去: (03)1234-5678 → 03-1234-5678
    s = re.sub(r"\((\d+)\)", r"\1-", s)
    # 数字のみ抽出してハイフン付きに整形
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) == 11:
        # 090-XXXX-XXXX or 03-XXXX-XXXX (携帯 or 市外局番3桁)
        return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    elif len(digits) == 10:
        # 03-XXXX-XXXX (市外局番2桁)
        return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    elif len(digits) == 12 and digits.startswith("0120"):
        # フリーダイヤル
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
    # パターンに合わなければ元の値を返す
    return s


def clean_email(v) -> str:
    """メールアドレス: 全角→半角、小文字化、形式バリデーション（不正なら空文字）"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).lower().strip()
    # 基本的なメールアドレス形式チェック
    if not re.match(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$", s):
        return ""
    return s


def clean_zipcode(v) -> str:
    """郵便番号: XXX-XXXX 形式に統一"""
    s = _to_str(v).strip()
    if not s:
        return ""
    s = s.translate(_ZEN_DIGITS)
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) == 7:
        return f"{digits[:3]}-{digits[3:]}"
    return s


def clean_url(v) -> str:
    """URL: http→https、末尾スラッシュ統一"""
    s = _to_str(v).strip()
    if not s:
        return ""
    if s.startswith("http://"):
        s = "https://" + s[7:]
    if not s.endswith("/"):
        s += "/"
    return s


def clean_json(v) -> str:
    """JSON: バリデーションのみ（壊れていれば空配列 []）"""
    s = _to_str(v).strip()
    if not s:
        return "[]"
    try:
        parsed = json.loads(s)
        return json.dumps(parsed, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        return "[]"
