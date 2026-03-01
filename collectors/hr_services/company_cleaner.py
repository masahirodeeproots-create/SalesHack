"""企業名正規化・ファジーマッチによる名寄せ"""

import re
import sys
import csv
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

# HR services ルートを sys.path に追加
_HR_ROOT = str(Path(__file__).resolve().parent)
if _HR_ROOT not in sys.path:
    sys.path.insert(0, _HR_ROOT)

from config import OUTPUT_DIR, CSV_ENCODING


# 除去する法人格パターン（前方・後方いずれも対応）
_LEGAL_ENTITIES = [
    r"株式会社",
    r"\(株\)",
    r"（株）",
    r"有限会社",
    r"\(有\)",
    r"（有）",
    r"合同会社",
    r"\(同\)",
    r"（同）",
    r"合名会社",
    r"合資会社",
    r"一般社団法人",
    r"一般財団法人",
    r"公益社団法人",
    r"公益財団法人",
    r"医療法人",
    r"社会福祉法人",
    r"学校法人",
    r"宗教法人",
    r"NPO法人",
    r"特定非営利活動法人",
    r"独立行政法人",
]
_LEGAL_PATTERN = re.compile("|".join(_LEGAL_ENTITIES))

# 末尾の括弧注釈パターン（例: (東京), [上場], 【本社】）
_TRAILING_ANNOTATION = re.compile(r"\s*[（(\[【][^）)\]】]*[）)\]】]\s*$")


def normalize_company_name(raw: str) -> str:
    """
    企業名を正規化する。

    1. Unicode NFKC正規化（全角→半角、合字分解等）
    2. 法人格除去（株式会社, (株), 有限会社 等）
    3. 空白正規化（全角スペース含む）
    4. 引用符・括弧の除去
    5. 末尾注釈の除去
    """
    if not raw:
        return ""

    name = raw.strip()

    # 1. NFKC正規化
    name = unicodedata.normalize("NFKC", name)

    # 2. 法人格除去
    name = _LEGAL_PATTERN.sub("", name)

    # 3. 空白正規化
    name = re.sub(r"[\s\u3000]+", " ", name).strip()

    # 4. 引用符・括弧除去
    name = name.strip("\"'「」『』【】")

    # 5. 末尾注釈除去
    name = _TRAILING_ANNOTATION.sub("", name).strip()

    return name


def find_fuzzy_clusters(
    names: list[str], threshold: float = 0.85
) -> list[list[str]]:
    """
    類似企業名をクラスタリングする（Union-Find + SequenceMatcher）。

    Args:
        names: 正規化済み企業名リスト
        threshold: 類似度の閾値（0.0〜1.0）

    Returns:
        2件以上の類似名を含むクラスタのリスト
    """
    parent = {n: n for n in names}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            if SequenceMatcher(None, a, b).ratio() >= threshold:
                union(a, b)

    clusters: dict[str, list[str]] = {}
    for name in names:
        root = find(name)
        clusters.setdefault(root, []).append(name)

    return [c for c in clusters.values() if len(c) > 1]


def save_fuzzy_review(clusters: list[list[str]], output_path: str | Path):
    """ファジーマッチ結果をレビュー用CSVに保存"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding=CSV_ENCODING) as f:
        writer = csv.writer(f)
        writer.writerow(["クラスタID", "企業名", "採用名（空欄=要判断）"])
        for i, cluster in enumerate(clusters, 1):
            for name in sorted(cluster):
                writer.writerow([i, name, ""])
