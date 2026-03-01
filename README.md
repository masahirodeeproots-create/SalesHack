# 企業情報収集

複数の求人媒体・企業情報サイトからデータをスクレイピングし、正規化・統合して1つのCSVに出力するパイプライン。

## アーキテクチャ

```
collect_media_urls.py     ─ Phase 1: Google検索でURL発見
        ↓ company_media_urls.csv
collect_company_data.py   ─ Phase 2: 各URLのスクレイピング → 正規化 → 統合
        ↓
  field_mapper.py         ─ ラベル正規化（alias完全一致 → 注釈除去 → 部分一致 → Gemini AI）
  master_fields.json      ─ 55+フィールド定義（canonical名・aliases・source_priority）
        ↓
  company_data_master.csv ─ 最終出力（企業×フィールドのマトリクス）
```

## 処理フロー

### Phase 1: URL収集 (`collect_media_urls.py`)

ScrapingDog Google Search API で各企業×媒体の組み合わせを検索し、URLプレフィックスの前方一致でターゲットURLを特定する。

**対象媒体（5媒体）:**

| 媒体 | URLプレフィックス | レンダリング |
|------|------------------|-------------|
| マイナビ | `job.mynavi.jp/2{6,7}/pc/search/` | ScrapingDog (JS) |
| リクナビ | `job.rikunabi.com/202{6,7}/company` | 静的HTML |
| キャリタス | `job.career-tasu.jp/corp/` | 静的HTML |
| リクルートエージェント | `r-agent.com/kensaku/companydetail/` | ScrapingDog (JS) |
| PR TIMES | `prtimes.jp/main/html/searchrlp/` | Playwright (React SPA) |

**出力:** `company_media_urls.csv`（企業名・媒体名・URL・status）

### Phase 2: データ収集・統合 (`collect_company_data.py`)

1. `company_media_urls.csv` の `found` URLを読み込み
2. 媒体ごとに適切な方法でHTML取得（静的HTTP / ScrapingDog JS / Playwright SPA）
3. `dl/dt/dd` ペアおよび `th/td` テーブルから構造化フィールドを抽出
4. `field_mapper` で canonical 名にマッピング（3段階ルール + Gemini AIフォールバック）
5. `merge_multi_source()` で `source_priority` に従い複数媒体の値を1つに統合
6. みんかぶ財務データ（売上成長率・営業CF・フリーCF・自己資本率・ROE）を追加
7. `company_data_master.csv` に出力（BOM付きUTF-8、Excel対応）

**チェックポイント機構:** `checkpoints/` に JSON を保存し、中断しても再開可能。

## フィールド正規化 (`field_mapper.py`)

各媒体で表記が異なるラベル（例: 「本社」「本社所在地」「本社所在地1」）を統一する。

| 段階 | 方式 | 例 |
|------|------|-----|
| 1 | Unicode NFKC正規化 + alias完全一致 | `本社所在地` → `本社所在地` |
| 2 | 括弧・注釈除去後に再試行 | `本社（MAP）` → `本社所在地` |
| 3 | 部分一致（3文字以上） | `創業/設立` → `設立` |
| 4 | Gemini AI フォールバック | 上記で解決不能なラベルをAIが判定 |

**`master_fields.json`:** 各フィールドの canonical名・aliases・category・source_priority を定義。

## みんかぶ財務データ

上場企業は証券コードベースで `minkabu.jp/stock/{code}/settlement` から財務指標を取得:

- 売上高・営業利益（複数期分）
- 売上成長率（自動算出）
- 自己資本率
- ROE
- 営業CF・フリーCF

## セットアップ

```bash
# 依存パッケージ
pip install -r requirements.txt
# または
pip install -e .

# Playwright ブラウザ（PR TIMES用）
python -m playwright install chromium

# 環境変数
cp .env.example .env
# .env に SCRAPINGDOG_API_KEY と GEMINI_API_KEY を設定
```

## 実行方法

```bash
# Phase 1: URL収集
python collect_media_urls.py

# Phase 2: データ収集・統合
python collect_company_data.py
```

**出力ファイル:**
- `company_media_urls.csv` — 企業×媒体のURL一覧
- `company_data_master.csv` — 企業×フィールドの統合データ（最終成果物）
- `collect_data.log` — 実行ログ
- `checkpoints/` — 中断再開用チェックポイント

## 対象企業の変更

`collect_media_urls.py` の `COMPANIES` リストを編集する。みんかぶ対象企業は `collect_company_data.py` の `MINKABU_STOCK_CODES` に証券コードを追加する。

## ディレクトリ構成

```
企業情報収集/
├── collect_media_urls.py        # Phase 1: URL収集
├── collect_company_data.py      # Phase 2: データ収集・統合パイプライン
├── field_mapper.py              # ラベル正規化・マージロジック
├── master_fields.json           # フィールド定義（canonical, aliases, priority）
├── analyze_media_structure.py   # 媒体ページ構造のテンプレート分析
├── .env                         # API キー（git管理外）
├── .env.example                 # 環境変数テンプレート
├── pyproject.toml               # プロジェクト設定・依存関係
├── requirements.txt             # 依存パッケージ（簡易版）
├── company_media_urls.csv       # Phase 1 出力
├── company_data_master.csv      # Phase 2 出力（最終成果物）
├── collect_data.log             # 実行ログ
├── checkpoints/                 # 中断再開用チェックポイント
├── config/                      # DB設定
├── db/                          # DBモデル・マイグレーション
└── HRサービス使用状況調査/       # HRサービス利用状況の別パイプライン（別README参照）
```

## 技術スタック

| 用途 | ライブラリ |
|------|-----------|
| 静的HTML取得 | requests |
| JSレンダリング（マイナビ等） | ScrapingDog API (`dynamic: true`) |
| React SPA レンダリング（PR TIMES） | Playwright (Chromium headless) |
| HTML解析 | BeautifulSoup4 |
| AIフォールバック | Google Gemini API (`gemini-2.5-flash`) |
| DB | PostgreSQL + SQLAlchemy + Alembic |
