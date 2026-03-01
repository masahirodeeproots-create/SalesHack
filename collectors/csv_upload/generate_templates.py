"""
generate_templates.py
=====================
各種CSVテンプレートファイルを data/templates/ に生成する。

生成されるテンプレート:
  1. companies_template.csv       - 企業データ一括アップロード用
  2. phone_numbers_template.csv   - 電話番号データアップロード用
  3. call_logs_template.csv       - 架電データアップロード用
  4. persons_template.csv         - 担当者データアップロード用

使い方:
  python -m collectors.csv_upload.generate_templates
"""

import csv
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from config.settings import DATA_DIR

TEMPLATES_DIR = DATA_DIR / "templates"


# ---------------------------------------------------------------------------
# テンプレート定義
# ---------------------------------------------------------------------------

TEMPLATES: dict[str, dict] = {

    # ① 企業データ
    "companies_template.csv": {
        "description": "企業マスタ一括アップロード（13万件規模対応）",
        "delimiter": "|",
        "columns": [
            "企業名",
            "本社都道府県",
            "代表者名",
            "従業員数",
            "企業規模",
            "業種",
            "業種詳細",
            "代表電話番号",
        ],
        "sample_rows": [
            ["株式会社サンプル", "東京都", "山田太郎", "500", "中堅企業", "IT・通信", "ソフトウェア開発", "03-1234-5678"],
            ["テスト株式会社", "大阪府", "鈴木次郎", "120", "中小企業", "製造業", "電子部品製造", "06-9876-5432"],
        ],
        "notes": [
            "# 区切り文字: | (パイプ)",
            "# 文字コード: UTF-8 (BOMあり推奨)",
            "# 企業名は必須。他の列は空欄でも可。",
            "# 代表電話番号は phone_numbers テーブルに「代表」ラベルで登録される。",
            "# 既存企業名は重複登録されない（スキップ）。",
        ],
    },

    # ② 電話番号データ
    "phone_numbers_template.csv": {
        "description": "電話番号データアップロード",
        "delimiter": ",",
        "columns": [
            "company_name",        # 企業名（name_normalized と突合）
            "phone_number",        # 電話番号（必須）
            "label",               # ラベル（代表/人事部直通/大阪拠点 等）
            "status",              # ステータス（未確認/該当/使われてない/AI対応/別会社・別拠点・別事業部）
            "source",              # 入手元（Web収集/架電判明/名刺 等）
            "status_detail",       # ステータス詳細（別会社等の場合）
        ],
        "sample_rows": [
            ["株式会社サンプル", "03-1234-5678", "代表", "未確認", "Web収集", ""],
            ["株式会社サンプル", "03-1234-9999", "人事部直通", "未確認", "Web収集", ""],
            ["テスト株式会社", "06-9876-0000", "大阪本社", "該当", "架電判明", ""],
        ],
        "notes": [
            "# 文字コード: UTF-8 BOM付き",
            "# company_name は companies.name_normalized と完全一致で照合。",
            "# status の許容値: 未確認 / 該当 / 使われてない / AI対応 / 別会社・別拠点・別事業部",
            "# 同じ企業・同じ電話番号は重複登録されない。",
        ],
    },

    # ③ 架電ログ
    "call_logs_template.csv": {
        "description": "架電データアップロード（営業マン記入用）",
        "delimiter": ",",
        "columns": [
            "company_name",              # 企業名（必須）
            "sales_rep_name",            # 営業担当者名（必須）
            "called_at",                 # 架電日時 YYYY-MM-DD HH:MM（必須）
            "phone_number",              # 架電した電話番号（必須）
            "phone_status",              # 番号ステータス（必須）
            "product_name",              # 営業商品名（任意）
            "phone_status_memo",         # 番号ステータス詳細（任意）
            "discovered_number",         # 新規発見した電話番号（任意）
            "discovered_number_memo",    # 新規番号のメモ（任意）
            "call_result",               # 通話結果（任意）
            "spoke_with",                # 話した相手（任意）
            "discovered_person_chuto",   # 判明した中途採用担当者名（任意）
            "discovered_person_shinsotsu",  # 判明した新卒採用担当者名（任意）
            "notes",                     # メモ（任意）
        ],
        "sample_rows": [
            [
                "株式会社サンプル", "田中営業", "2025-04-01 10:30",
                "03-1234-5678", "該当", "HRシステム", "",
                "", "", "不在", "", "", "", "担当者不在、折り返し希望",
            ],
            [
                "テスト株式会社", "田中営業", "2025-04-01 11:00",
                "06-9876-5432", "該当", "HRシステム", "",
                "06-9876-0001", "人事部直通と言われた", "アポ",
                "鈴木さん", "鈴木花子", "", "来週月曜14時アポ",
            ],
            [
                "架電例株式会社", "佐藤営業", "2025-04-01 14:00",
                "03-0000-0000", "使われてない", "", "",
                "", "", "", "", "", "", "",
            ],
        ],
        "notes": [
            "# 文字コード: UTF-8 BOM付き",
            "# called_at の形式: YYYY-MM-DD HH:MM (例: 2025-04-01 10:30)",
            "# phone_status の許容値: 該当 / 使われてない / AI対応 / 別会社・別拠点・別事業部",
            "# call_result の許容値: 不在 / 受付ブロック / 着電NG / 獲得見込み / 資料請求 / アポ / 架電NG",
            "# company_name が未登録の場合はスキップされる（先に企業データをインポートすること）。",
            "# sales_rep_name は未登録でも自動作成される。",
        ],
    },

    # ④ 担当者データ
    "persons_template.csv": {
        "description": "担当者データアップロード",
        "delimiter": ",",
        "columns": [
            "company_name",       # 企業名（必須）
            "person_name",        # 担当者名（必須）
            "department",         # 部署名（任意）
            "role",               # 役割（新卒担当/中途担当/人事部長 等）
            "email",              # メールアドレス（任意）
            "notes",              # 備考（任意）
            "source",             # 情報源（架電判明/Web/名刺 等）
        ],
        "sample_rows": [
            ["株式会社サンプル", "佐藤太郎", "人事部", "新卒採用担当", "sato@example.com", "", "架電判明"],
            ["株式会社サンプル", "鈴木花子", "人事部", "中途採用担当", "", "決裁者", "架電判明"],
        ],
        "notes": [
            "# 文字コード: UTF-8 BOM付き",
            "# company_name と person_name は必須。",
            "# 同じ企業・同じ担当者名は重複登録されない。",
        ],
    },
}


# ---------------------------------------------------------------------------
# テンプレート生成
# ---------------------------------------------------------------------------

def generate_all_templates(output_dir: Path = TEMPLATES_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for filename, tpl in TEMPLATES.items():
        output_path = output_dir / filename
        delimiter = tpl["delimiter"]

        with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
            # コメント行（CSVには直接書けないが参考のために先頭に入れる）
            # ただし一般的なCSVツールはコメント行を無視しないので
            # 別のREADMEテキストとして同名の.txtを生成する

            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(tpl["columns"])
            for row in tpl["sample_rows"]:
                writer.writerow(row)

        # ノートを .txt に出力
        note_path = output_dir / (filename.replace(".csv", "_notes.txt"))
        note_path.write_text(
            "\n".join([f"【{tpl['description']}】", ""] + tpl["notes"]),
            encoding="utf-8",
        )

        print(f"  生成: {output_path}")
        print(f"  メモ: {note_path}")

    print(f"\nテンプレート生成完了: {output_dir}")


def main() -> None:
    generate_all_templates()


if __name__ == "__main__":
    main()
