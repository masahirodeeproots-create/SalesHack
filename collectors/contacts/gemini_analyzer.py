"""
gemini_analyzer.py
==================
スニペットテキストと正規表現抽出結果を Gemini に渡し、
担当者名・拠点名・事業部名・電話番号との紐付け・priority・メール種別を抽出する。

出力形式 (JSON):
  {
    "phone_db": [
      {
        "phone_number": "03-1234-5678",
        "office_name": "東京本社",          // null可
        "department_name": "人事部",         // null可
        "priority": 1,                       // 1〜4
        "source_snippet_id": 5,
        "confidence_score": 0.95
      }
    ],
    "person_db": [
      {
        "person_name": "佐藤太郎",
        "office_name": "東京本社",           // null可
        "department_name": "人事部",         // null可
        "relation_phone_number": "03-1234-5678",  // null可
        "source_snippet_id": 5,
        "confidence_score": 0.9
      }
    ],
    "email_db": [
      {
        "email_address": "recruit@example.com",
        "type": "recruit",                   // recruit/hr/info/other
        "source_snippet_id": 5,
        "confidence_score": 0.9
      }
    ]
  }

priority の定義:
  1: 採用/人事直通 (最優先)
  2: 採用部署代表
  3: 会社代表
  4: その他
"""

import json
import logging
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Optional

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import GEMINI_API_KEY
from collectors.contacts.page_fetcher import Snippet
from collectors.contacts.regex_extractor import PhoneCandidate, EmailCandidate

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# APIキープール（複数キーのラウンドロビン配布）
# ---------------------------------------------------------------------------

class GeminiKeyPool:
    """複数APIキーをラウンドロビンで配布するスレッドセーフなプール。
    有効なキー（空文字でないもの）のみを管理する。キーが1つだけでも動作する。
    """

    def __init__(self, keys: list[str]):
        self._keys = [k for k in keys if k]
        if not self._keys:
            from config.settings import GEMINI_API_KEY
            self._keys = [GEMINI_API_KEY] if GEMINI_API_KEY else []
        self._idx = 0
        self._lock = threading.Lock()

    def get_key(self) -> str:
        with self._lock:
            if not self._keys:
                return ""
            key = self._keys[self._idx % len(self._keys)]
            self._idx += 1
            return key

    @property
    def count(self) -> int:
        return len(self._keys)


# ---------------------------------------------------------------------------
# 電話番号正規化
# ---------------------------------------------------------------------------

def _normalize_digits(phone: str) -> Optional[str]:
    """数字のみに正規化。10〜11桁以外は None を返す（時刻・業務時間等の誤検出除外）。"""
    digits = re.sub(r"\D", "", phone)
    return digits if 10 <= len(digits) <= 11 else None


_SYSTEM_PROMPT = """\
あなたは企業の連絡先情報を構造化するアシスタントです。
日本の企業のウェブページから取得したテキストスニペットを解析し、
電話番号・担当者・メールアドレスに関する情報を指定のJSON形式で出力します。

priority の定義:
  1 = 採用担当・人事部直通 (最優先)
  2 = 採用部署・人事部の代表番号
  3 = 会社代表番号
  4 = その他（支社・別部署・フリーダイヤル等）

出力は必ず以下のJSON構造のみ返してください（説明文不要）:
{
  "phone_db": [...],
  "person_db": [...],
  "email_db": [...]
}
"""

_USER_PROMPT_TEMPLATE = """\
【スニペットID】{snippet_id}
【テキスト】
{text}

【正規表現で検出された電話番号候補】
{phone_candidates}

【正規表現で検出されたメールアドレス候補】
{email_candidates}

上記情報を解析し、JSON形式で出力してください。
- 検出できなかった項目は空リスト [] にしてください
- office_name / department_name / relation_phone_number は不明な場合 null にしてください
- confidence_score は 0.0〜1.0 の推定確信度を設定してください
- 担当者名は日本人名の可能性が高いものだけ抽出してください（役職名のみは不可）
"""


def _build_prompt(
    snippet: Snippet,
    phones: list[PhoneCandidate],
    emails: list[EmailCandidate],
) -> str:
    phone_text = "\n".join(
        f"  - {p.normalized} (コンテキスト: {', '.join(p.context_keywords) or 'なし'})"
        for p in phones
    ) or "  (なし)"

    email_text = "\n".join(
        f"  - {e.address} (タイプヒント: {e.keyword_hint})"
        for e in emails
    ) or "  (なし)"

    return _USER_PROMPT_TEMPLATE.format(
        snippet_id=snippet.snippet_id,
        text=snippet.text[:1500],  # Gemini の入力制限を考慮
        phone_candidates=phone_text,
        email_candidates=email_text,
    )


def _parse_gemini_response(raw: str, snippet_id: int) -> dict:
    """Geminiの応答テキストからJSONを抽出・パースする。"""
    # コードブロック除去
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1]
        raw = raw.rsplit("```", 1)[0]
    raw = raw.strip()

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"JSONパース失敗 (snippet_id={snippet_id}): {raw[:200]}")
        return {"phone_db": [], "person_db": [], "email_db": []}

    # source_snippet_id を上書き保証
    for item in result.get("phone_db", []):
        item["source_snippet_id"] = snippet_id
    for item in result.get("person_db", []):
        item["source_snippet_id"] = snippet_id
    for item in result.get("email_db", []):
        item["source_snippet_id"] = snippet_id

    return result


def analyze_snippet(
    snippet: Snippet,
    phones: list[PhoneCandidate],
    emails: list[EmailCandidate],
    model_name: str = "gemini-2.5-flash-lite",
    retry: int = 2,
) -> dict:
    """
    スニペットをGeminiで解析し、構造化された連絡先情報を返す。

    Returns:
        {"phone_db": [...], "person_db": [...], "email_db": [...]}
    """
    try:
        import google.generativeai as genai
    except ImportError:
        logger.error("google-generativeai がインストールされていません: pip install google-generativeai")
        return {"phone_db": [], "person_db": [], "email_db": []}

    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY が設定されていません")
        return {"phone_db": [], "person_db": [], "email_db": []}

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=_SYSTEM_PROMPT,
    )

    prompt = _build_prompt(snippet, phones, emails)

    for attempt in range(retry + 1):
        try:
            response = model.generate_content(prompt)
            return _parse_gemini_response(response.text, snippet.snippet_id)
        except Exception as e:
            logger.warning(f"Gemini API エラー (attempt {attempt + 1}): {e}")
            if attempt < retry:
                time.sleep(2 ** attempt)

    return {"phone_db": [], "person_db": [], "email_db": []}


def analyze_snippets_batch(
    snippets: list[Snippet],
    phones_list: list[list[PhoneCandidate]],
    emails_list: list[list[EmailCandidate]],
    api_key: Optional[str] = None,
    model_name: str = "gemini-2.5-flash-lite",
    retry: int = 2,
) -> dict:
    """
    複数スニペット（最大3件）を1回のGemini呼び出しで解析する。

    Args:
        snippets:     解析対象のSnippetリスト（最大3件推奨）
        phones_list:  各スニペットに対応する電話番号候補リスト
        emails_list:  各スニペットに対応するメール候補リスト
        api_key:      使用するGemini APIキー（None の場合は設定から取得）
        model_name:   Geminiモデル名
        retry:        リトライ回数

    Returns:
        {"phone_db": [...], "person_db": [...], "email_db": [...]} — 全スニペット統合済み
    """
    if not snippets:
        return {"phone_db": [], "person_db": [], "email_db": []}

    try:
        import google.generativeai as genai
    except ImportError:
        logger.error("google-generativeai がインストールされていません: pip install google-generativeai")
        return {"phone_db": [], "person_db": [], "email_db": []}

    _api_key = api_key or GEMINI_API_KEY
    if not _api_key:
        logger.error("GEMINI_API_KEY が設定されていません")
        return {"phone_db": [], "person_db": [], "email_db": []}

    genai.configure(api_key=_api_key)
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=_SYSTEM_PROMPT,
    )

    # バッチプロンプト構築
    parts = []
    for i, (snippet, phones, emails) in enumerate(zip(snippets, phones_list, emails_list)):
        phone_text = "\n".join(
            f"  - {p.normalized} (コンテキスト: {', '.join(p.context_keywords) or 'なし'})"
            for p in phones
        ) or "  (なし)"
        email_text = "\n".join(
            f"  - {e.address} (タイプヒント: {e.keyword_hint})"
            for e in emails
        ) or "  (なし)"
        parts.append(
            f"[スニペット{i}] id={snippet.snippet_id}\n"
            f"テキスト: {snippet.text[:800]}\n"
            f"電話番号候補:\n{phone_text}\n"
            f"メール候補:\n{email_text}"
        )

    n = len(snippets)
    prompt = (
        f"以下{n}件のスニペットを独立して解析し、JSON配列で返してください。\n\n"
        + "\n\n".join(parts)
        + "\n\n出力形式（必ず配列）:\n"
        + '[{"snippet_id": <id>, "phone_db": [...], "person_db": [...], "email_db": [...]}, ...]\n'
        + "- 検出できなかった項目は空リスト [] にしてください\n"
        + "- office_name / department_name / relation_phone_number は不明な場合 null にしてください\n"
        + "- confidence_score は 0.0〜1.0 の推定確信度を設定してください\n"
        + "- 担当者名は日本人名の可能性が高いものだけ抽出してください（役職名のみは不可）\n"
    )

    for attempt in range(retry + 1):
        try:
            response = model.generate_content(prompt)
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[-1]
                raw = raw.rsplit("```", 1)[0]
            raw = raw.strip()

            batch_results = json.loads(raw)
            if not isinstance(batch_results, list):
                batch_results = [batch_results]

            # source_snippet_id を上書き保証
            for item_result, snippet in zip(batch_results, snippets):
                for item in item_result.get("phone_db", []):
                    item["source_snippet_id"] = snippet.snippet_id
                for item in item_result.get("person_db", []):
                    item["source_snippet_id"] = snippet.snippet_id
                for item in item_result.get("email_db", []):
                    item["source_snippet_id"] = snippet.snippet_id

            return merge_results(batch_results)

        except Exception as e:
            logger.warning(f"Gemini バッチAPI エラー (attempt {attempt + 1}): {e}")
            if attempt < retry:
                time.sleep(2 ** attempt)

    return {"phone_db": [], "person_db": [], "email_db": []}


def merge_results(results: list[dict]) -> dict:
    """
    複数スニペットの解析結果をマージする。
    同一電話番号は正規化済み数字列（10〜11桁）で重複除去する。
    """
    merged: dict = {"phone_db": [], "person_db": [], "email_db": []}
    seen_phones: set[str] = set()  # 正規化済み数字列で管理
    seen_emails: set[str] = set()
    seen_persons: set[str] = set()

    for result in results:
        for item in result.get("phone_db", []):
            num = item.get("phone_number", "")
            normalized = _normalize_digits(num)
            if normalized and normalized not in seen_phones:
                seen_phones.add(normalized)
                merged["phone_db"].append(item)

        for item in result.get("email_db", []):
            addr = item.get("email_address", "").lower()
            if addr and addr not in seen_emails:
                seen_emails.add(addr)
                merged["email_db"].append(item)

        for item in result.get("person_db", []):
            name = item.get("person_name", "")
            dept = item.get("department_name", "")
            key = f"{name}_{dept}"
            if name and key not in seen_persons:
                seen_persons.add(key)
                merged["person_db"].append(item)

    return merged
