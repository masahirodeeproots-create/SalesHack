"""
regex_extractor.py
==================
スニペットから電話番号・メールアドレスを正規表現で抽出する。
Geminiに渡す前の前処理として、機械的に候補を絞り込む。

抽出対象:
  - 電話番号: 日本国内形式 (03-xxxx-xxxx / 0xx-xxx-xxxx / 0120-xxx-xxx 等)
  - メールアドレス: RFC準拠の基本形式
  - キーワード: 「代表」「直通」「採用」等の文脈ヒント
"""

import re
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# 電話番号パターン
# ---------------------------------------------------------------------------

# 国内電話番号 (ハイフンあり・なし両対応、全角ハイフン対応)
# グループ:  市外局番-市内局番-加入者番号
_TEL_PATTERN = re.compile(
    r"""
    (?:TEL|Tel|tel|電話|℡|☎)?               # 任意のラベル
    [:\s：　]*                                # 区切り
    (
        (?:0\d{1,4})                         # 市外局番 (0始まり)
        [\-－‐‑–—\s・]                       # 区切り文字（全角も）
        (?:\d{2,4})                          # 市内局番
        [\-－‐‑–—\s・]                       # 区切り文字
        (?:\d{3,4})                          # 加入者番号
    )
    """,
    re.VERBOSE,
)

# 括弧あり形式: (03)1234-5678
_TEL_PAREN_PATTERN = re.compile(
    r"(?:TEL|Tel|電話)?[:\s：　]*"
    r"(\(0\d{1,4}\)[\s\-－]?\d{2,4}[\-－]\d{3,4})"
)

# フリーダイヤル: 0120-xxx-xxx
_FREEDIAL_PATTERN = re.compile(
    r"(0120[\-－]?\d{2,3}[\-－]?\d{3,4})"
)


# ---------------------------------------------------------------------------
# メールアドレスパターン
# ---------------------------------------------------------------------------

_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
)


# ---------------------------------------------------------------------------
# コンテキストキーワード（電話番号ラベル判定用）
# ---------------------------------------------------------------------------

# これらのキーワードが電話番号の前後100文字以内にあれば context として返す
PHONE_KEYWORDS = [
    "代表", "直通", "採用", "人事", "HR", "hr",
    "総務", "受付", "窓口", "問い合わせ", "お問い合わせ",
    "新卒", "中途", "本社", "支社", "支店", "営業所",
    "東京", "大阪", "名古屋", "福岡", "札幌", "仙台",
    "採用担当", "人事部",
]

EMAIL_KEYWORDS = {
    "recruit": ["recruit", "saiyou", "採用", "新卒", "saiyo"],
    "hr": ["hr", "jinji", "人事", "career", "キャリア"],
    "info": ["info", "information", "contact", "お問い合わせ", "inquiry"],
}


# ---------------------------------------------------------------------------
# データクラス
# ---------------------------------------------------------------------------

@dataclass
class PhoneCandidate:
    raw: str               # 正規化前の文字列
    normalized: str        # ハイフン統一後
    context_keywords: list[str] = field(default_factory=list)  # 周辺キーワード
    char_position: int = 0  # スニペット内の文字位置


@dataclass
class EmailCandidate:
    address: str
    keyword_hint: str = ""  # "recruit" / "hr" / "info" / "other"
    char_position: int = 0


# ---------------------------------------------------------------------------
# 電話番号の正規化
# ---------------------------------------------------------------------------

def _normalize_phone(raw: str) -> str:
    """全角・空白・全角ハイフンを半角ハイフンに統一する。"""
    s = raw
    # 全角数字→半角
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    # 全角ハイフン類→半角
    s = re.sub(r"[－‐‑–—・]", "-", s)
    # 空白除去
    s = re.sub(r"\s", "", s)
    # 括弧除去
    s = s.replace("(", "").replace(")", "")
    return s


def _extract_context_keywords(text: str, pos: int, window: int = 100) -> list[str]:
    """電話番号の周辺テキストからキーワードを抽出する。"""
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    surrounding = text[start:end]
    return [kw for kw in PHONE_KEYWORDS if kw in surrounding]


def _guess_email_type(email: str) -> str:
    """メールアドレスのローカルパートからタイプを推定する。"""
    local = email.split("@")[0].lower()
    for etype, keywords in EMAIL_KEYWORDS.items():
        if any(kw in local for kw in keywords):
            return etype
    return "other"


# ---------------------------------------------------------------------------
# メイン抽出関数
# ---------------------------------------------------------------------------

def extract_phones(text: str) -> list[PhoneCandidate]:
    """テキストから電話番号候補を抽出する。"""
    found: dict[str, PhoneCandidate] = {}

    for pattern in [_TEL_PATTERN, _TEL_PAREN_PATTERN, _FREEDIAL_PATTERN]:
        for m in pattern.finditer(text):
            raw = m.group(1)
            normalized = _normalize_phone(raw)
            # 桁数チェック（10〜12桁）
            digits = re.sub(r"\D", "", normalized)
            if len(digits) < 10 or len(digits) > 12:
                continue
            if normalized in found:
                continue
            keywords = _extract_context_keywords(text, m.start())
            found[normalized] = PhoneCandidate(
                raw=raw,
                normalized=normalized,
                context_keywords=keywords,
                char_position=m.start(),
            )

    return list(found.values())


def extract_emails(text: str) -> list[EmailCandidate]:
    """テキストからメールアドレス候補を抽出する。"""
    found: dict[str, EmailCandidate] = {}
    for m in _EMAIL_PATTERN.finditer(text):
        addr = m.group(0).lower()
        if addr in found:
            continue
        found[addr] = EmailCandidate(
            address=addr,
            keyword_hint=_guess_email_type(addr),
            char_position=m.start(),
        )
    return list(found.values())


def has_contact_signals(text: str) -> bool:
    """スニペットに連絡先情報が含まれている可能性を判定する（Gemini呼び出し前の絞り込み）。"""
    has_phone = bool(_TEL_PATTERN.search(text) or _TEL_PAREN_PATTERN.search(text))
    has_email = bool(_EMAIL_PATTERN.search(text))
    has_keywords = any(kw in text for kw in ["電話", "TEL", "メール", "e-mail", "採用", "人事", "担当"])
    return has_phone or has_email or has_keywords
