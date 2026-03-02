"""
field_mapper.py
===============
スクレイピングで取得した生フィールド名を master_fields.json のcanonical名にマップするモジュール。

基本方針:
  1. Unicodeを正規化（全角→半角、空白除去、小文字化）して完全一致
  2. 括弧・注釈除去後に再試行
  3. 部分一致（3文字以上のaliasがラベルに含まれる、または逆）
  4. ルールベースで解決できない場合のみ Gemini API にフォールバック

複数媒体のマージ:
  - 各媒体の raw_fields を map_fields() で個別にcanonicalへ変換
  - merge_multi_source() で優先順位に従い1つの値に統合
  - 優先順位は master_fields.json の source_priority で定義（先頭が最高優先）

使い方:
  from field_mapper import map_fields, merge_multi_source

  # 各媒体ごとにマップ
  sources = {
      "リクルートエージェント": map_fields(ra_raw, "リクルートエージェント")["mapped"],
      "キャリタス":             map_fields(ca_raw, "キャリタス")["mapped"],
      "リクナビ":               map_fields(rn_raw, "リクナビ")["mapped"],
  }

  # 優先順位に従い統合
  merged = merge_multi_source(sources)
  # → 例: "本社所在地" はリクルートエージェントの値（郵便番号付き）が採用される
"""

import json
import os
import sys
import re
import unicodedata
import logging
from pathlib import Path
from typing import Optional

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import SCHEMAS_DIR

logger = logging.getLogger(__name__)

# master_fields.json をロード
_SCHEMA_PATH = str(SCHEMAS_DIR / "master_fields.json")
with open(_SCHEMA_PATH, encoding="utf-8") as _f:
    _MASTER_SCHEMA: dict = json.load(_f)

# ルックアップテーブルを構築
# normalized_alias → canonical
_ALIAS_TO_CANONICAL: dict[str, str] = {}
# canonical の集合（Gemini結果の検証用）
_CANONICAL_SET: set[str] = set()
# canonical → source_priority リスト
_SOURCE_PRIORITY: dict[str, list[str]] = {}

for _field in _MASTER_SCHEMA["fields"]:
    _can = _field["canonical"]
    _CANONICAL_SET.add(_can)
    for _alias in _field.get("aliases", []):
        _key = _alias.strip()
        _ALIAS_TO_CANONICAL[_key] = _can
    _SOURCE_PRIORITY[_can] = _field.get("source_priority", [])

# カテゴリ別canonical一覧（参照用）
FIELDS_BY_CATEGORY: dict[str, list[str]] = {}
for _field in _MASTER_SCHEMA["fields"]:
    _cat = _field.get("category", "その他")
    FIELDS_BY_CATEGORY.setdefault(_cat, []).append(_field["canonical"])


# ---------------------------------------------------------------------------
# 内部ユーティリティ
# ---------------------------------------------------------------------------

def _normalize(label: str) -> str:
    """
    ラベルを正規化する。
      - Unicode NFKC（全角→半角、合字展開など）
      - 前後の空白・全角スペース除去
      - 内部の連続空白を除去
      - 小文字化
    """
    label = unicodedata.normalize("NFKC", label)
    label = label.strip()
    label = re.sub(r"[\s\u3000]+", "", label)  # 全スペース除去
    return label.lower()


def _strip_annotations(label_norm: str) -> str:
    """括弧・記号で囲まれた注釈部分を除去する。"""
    # （MAP）, (MAP), 【任意】 etc.
    stripped = re.sub(r"[（(\[【「][^）)\]】」]*[）)\]】」]", "", label_norm)
    # ※ 以降
    stripped = re.sub(r"[※*].+$", "", stripped)
    return stripped.strip()


def _build_alias_lookup() -> dict[str, str]:
    """正規化済み alias → canonical のルックアップを構築して返す。"""
    lookup: dict[str, str] = {}
    for key, canonical in _ALIAS_TO_CANONICAL.items():
        lookup[_normalize(key)] = canonical
    return lookup


_NORM_ALIAS_MAP: dict[str, str] = _build_alias_lookup()


# ---------------------------------------------------------------------------
# メイン API
# ---------------------------------------------------------------------------

def map_label(raw_label: str) -> Optional[str]:
    """
    ラベル1件を canonical 名にマップする。

    Returns:
        canonical 名（str）、解決できない場合は None。
    """
    norm = _normalize(raw_label)

    # 1. 完全一致
    if norm in _NORM_ALIAS_MAP:
        return _NORM_ALIAS_MAP[norm]

    # 2. 括弧・注釈除去後に再試行
    stripped = _strip_annotations(norm)
    if stripped and stripped != norm and stripped in _NORM_ALIAS_MAP:
        return _NORM_ALIAS_MAP[stripped]

    # 3. 部分一致（長さ3以上のaliasがラベルに含まれる、またはラベルがaliasに含まれる）
    #    短いaliasは誤マッチしやすいため除外
    best_match: Optional[str] = None
    best_len = 0
    for alias_norm, canonical in _NORM_ALIAS_MAP.items():
        if len(alias_norm) < 3:
            continue
        if alias_norm in norm or norm in alias_norm:
            # より長い（具体的な）aliasを優先
            if len(alias_norm) > best_len:
                best_match = canonical
                best_len = len(alias_norm)
    if best_match:
        return best_match

    return None


def map_fields(
    raw_fields: dict[str, str],
    media_name: str = "",
) -> dict:
    """
    {raw_label: value} の辞書を canonical フィールドにマップする。

    Args:
        raw_fields: スクレイピングで取得した {ラベル: 値} の辞書
        media_name: 媒体名（ログ出力用）

    Returns:
        {
          "mapped":   {canonical_name: value},  # マップ成功
          "unmapped": {raw_label: value},        # 未解決
        }
    """
    mapped: dict[str, str] = {}
    unmapped: dict[str, str] = {}

    for label, value in raw_fields.items():
        canonical = map_label(label)
        if canonical:
            # 同一canonicalへの複数マップは先着優先
            if canonical not in mapped:
                mapped[canonical] = value
            else:
                logger.debug(
                    f"[{media_name}] canonical '{canonical}' が重複 "
                    f"(既存: '{mapped[canonical]}', 無視: '{value}')"
                )
        else:
            unmapped[label] = value
            logger.info(f"[{media_name}] 未解決ラベル: '{label}' = '{value[:30]}'")

    return {"mapped": mapped, "unmapped": unmapped}


def map_fields_with_gemini_fallback(
    raw_fields: dict[str, str],
    media_name: str = "",
    gemini_model=None,
) -> dict[str, str]:
    """
    ルールベースでマップ後、未解決項目を Gemini API にフォールバックしてマップする。

    Args:
        raw_fields:    {raw_label: value}
        media_name:    媒体名（プロンプトのコンテキストとして使用）
        gemini_model:  google.generativeai.GenerativeModel インスタンス
                       None の場合は Gemini フォールバックをスキップ

    Returns:
        {canonical_name: value} — unmapped は含まれない
    """
    result = map_fields(raw_fields, media_name)
    mapped = result["mapped"]
    unmapped = result["unmapped"]

    if not unmapped:
        return mapped

    # ── Gemini fallback 前にナビゲーション/UIラベルをスキップ ──
    _GEMINI_SKIP_LABELS: set[str] = {
        # リクナビ ナビゲーション・会員登録誘導
        "会員の方はこちら", "まだ会員でない方は", "ログイン / 新規会員登録",
        "OpenES", "内々定",
        # リクナビ 給与形態選択肢（ラジオボタン等）
        "時給制、日給制、週給制、月給制", "年俸制、半期年俸制",
        # リクナビ フォーム注釈・操作案内
        "その他のポイント", "注意事項", "応募方法",
        "セミナー／説明会", "直近の説明会・面接",
        "お知らせ・イベント",
        # エントリー管理
        "エントリー完了企業", "エントリーできなかった企業",
        # マイナビ 年度見出し系（例: "2026年卒採用情報" 等）
        # ※ 年度数値は文字列に含めず prefix で判定
    }

    def _should_skip(label: str) -> bool:
        if label in _GEMINI_SKIP_LABELS:
            return True
        # 「セミナー／説明会（全N件）」「セミナー・説明会」等のパターン
        if label.startswith("セミナー") or label.startswith("説明会"):
            return True
        # 年度見出し（例: "2025年卒採用情報", "2026年卒情報" 等）
        if "年卒" in label and ("採用情報" in label or "情報" in label):
            return True
        return False

    unmapped = {k: v for k, v in unmapped.items() if not _should_skip(k)}
    if unmapped:
        logger.debug(
            f"[{media_name}] スキップリスト適用後: {len(unmapped)}件がGemini fallbackへ"
        )
    if not unmapped:
        return mapped

    if gemini_model is None:
        logger.info(
            f"[{media_name}] Gemini未設定のため {len(unmapped)}件の未解決ラベルをスキップ"
        )
        return mapped

    logger.info(f"[{media_name}] Gemini フォールバック: {len(unmapped)}件")

    canonical_list = [f["canonical"] for f in _MASTER_SCHEMA["fields"]]
    prompt = f"""あなたはデータ正規化の専門家です。
以下は「{media_name}」の企業ページから取得したフィールド名と値のペアです。
各フィールドを、下記の「正規化フィールド名リスト」の中から最も意味が近いものにマッピングしてください。
該当するものがなければ "unknown" としてください。

## 正規化フィールド名リスト
{json.dumps(canonical_list, ensure_ascii=False, indent=2)}

## マッピング対象（フィールド名: 値）
{json.dumps(unmapped, ensure_ascii=False, indent=2)}

## 出力形式
{{"<元のフィールド名>": "<canonical名 or unknown>"}} の形式でJSONのみを返してください。
"""

    try:
        import google.generativeai as genai

        response = gemini_model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(
                response_mime_type="application/json"
            ),
        )
        gemini_result: dict = json.loads(response.text)
        for raw_label, canonical in gemini_result.items():
            if canonical == "unknown":
                continue
            if canonical not in _CANONICAL_SET:
                logger.warning(
                    f"Gemini が未知のcanonicalを返しました: '{canonical}' (ラベル: '{raw_label}')"
                )
                continue
            if canonical not in mapped:
                mapped[canonical] = unmapped.get(raw_label, "")
                logger.debug(f"[Gemini] '{raw_label}' → '{canonical}'")
    except Exception as e:
        logger.error(f"Gemini フォールバック失敗: {e}")

    return mapped


# ---------------------------------------------------------------------------
# 複数媒体マージ
# ---------------------------------------------------------------------------

def merge_multi_source(
    sources: dict[str, dict[str, str]],
) -> dict[str, str]:
    """
    複数媒体のマップ済みデータを source_priority に従い1つに統合する。

    Args:
        sources: {媒体名: {canonical: value}} の辞書
                 各媒体の map_fields()["mapped"] の結果を渡す

    Returns:
        {canonical: value} — 各フィールドで最優先の媒体の値を採用。
        優先順位外の媒体にのみ存在するフィールドもフォールバックとして含める。

    優先ルール:
        1. source_priority リストの順に値を確認し、空文字・"-"・None でない最初の値を採用
        2. source_priority に載っていない媒体（将来追加分など）はフォールバックとして末尾扱い
        3. 全媒体で空値の場合はそのフィールドを結果に含めない
    """
    merged: dict[str, str] = {}

    # 全canonicalを収集（sources内に存在するものすべて）
    all_canonicals: set[str] = set()
    for media_data in sources.values():
        all_canonicals.update(media_data.keys())

    for canonical in all_canonicals:
        priority = _SOURCE_PRIORITY.get(canonical, [])

        # source_priority 順に試みる
        chosen_value: Optional[str] = None
        chosen_source: str = ""

        for media in priority:
            if media not in sources:
                continue
            val = sources[media].get(canonical, "")
            if val and val.strip() not in ("", "-", "null", "なし"):
                chosen_value = val
                chosen_source = media
                break

        # source_priority に載っていない媒体のフォールバック
        if chosen_value is None:
            for media, media_data in sources.items():
                if media in priority:
                    continue  # 既にチェック済み
                val = media_data.get(canonical, "")
                if val and val.strip() not in ("", "-", "null", "なし"):
                    chosen_value = val
                    chosen_source = media
                    break

        if chosen_value is not None:
            merged[canonical] = chosen_value
            logger.debug(f"  '{canonical}' ← [{chosen_source}] {chosen_value[:40]}")

    return merged


# ---------------------------------------------------------------------------
# PR TIMES プレスリリース専用パーサー
# ---------------------------------------------------------------------------

def parse_prtimes_press_releases(text: str) -> list[dict]:
    """
    PR TIMES の [PR: タイトル | 公開日] 形式のテキストから
    プレスリリース一覧を [{title, date}] 形式でパースする。
    """
    pattern = re.compile(r"\[PR:\s*(.+?)\s*\|\s*(.+?)\s*\]")
    results = []
    for m in pattern.finditer(text):
        results.append({"title": m.group(1).strip(), "date": m.group(2).strip()})
    return results


# ---------------------------------------------------------------------------
# デバッグ・確認用
# ---------------------------------------------------------------------------

def list_unmapped_stats(
    all_raw_fields: list[dict[str, str]],
    media_name: str = "",
) -> dict[str, int]:
    """
    複数企業の raw_fields を一括チェックし、未解決ラベルの出現頻度を返す。
    大量データでの検証・alias追加判断に使用する。

    Returns:
        {raw_label: count} — 出現回数の多い順にソート済み
    """
    freq: dict[str, int] = {}
    for raw_fields in all_raw_fields:
        result = map_fields(raw_fields, media_name)
        for label in result["unmapped"]:
            freq[label] = freq.get(label, 0) + 1
    return dict(sorted(freq.items(), key=lambda x: -x[1]))


if __name__ == "__main__":
    # 動作確認用サンプル
    logging.basicConfig(level=logging.DEBUG)

    test_cases = [
        # リクナビ
        {"本社": "愛知、東京", "業種": "自動車", "設立": "1937年"},
        # キャリタス
        {"代表者": "代表取締役社長　佐藤恒治", "上場区分": "国内上場", "創業/設立": "1937年（昭和12年）8月"},
        # リクルートエージェント
        {"本社所在地": "〒471-8571 愛知県豊田市", "資本金": "635,401百万円"},
        # PR TIMES
        {"業種": "情報通信", "代表者名": "德永 俊昭", "上場": "東証1部", "URL": "http://www.hitachi.co.jp/"},
        # 未解決の例
        {"謎のフィールド": "謎の値"},
    ]

    for i, raw in enumerate(test_cases):
        res = map_fields(raw)
        print(f"\n--- test {i + 1} ---")
        print(f"  mapped:   {res['mapped']}")
        if res["unmapped"]:
            print(f"  unmapped: {res['unmapped']}")
