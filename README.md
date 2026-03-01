# 企業情報収集

複数の求人媒体・企業情報サイトからデータをスクレイピングし、正規化・統合して CSV および BigQuery に出力するパイプライン。

## アーキテクチャ概要

```
collectors/
├── company_info/     Phase 1-2: 企業基本情報（媒体URL収集 → スクレイピング → 統合 CSV）
├── hr_services/      HRサービス各社の利用状況スクレイピング（14サービス対応）
├── contacts/         企業コンタクト情報（電話番号）収集 → PostgreSQL
├── call_data/        架電ログ CSV → PostgreSQL インポート
├── csv_upload/       汎用 CSV → PostgreSQL インポート
├── en_hyouban/       en 転職口コミサイト情報同期
└── gemini_enrichment/ Gemini AI による企業情報補完

db/
├── models.py         SQLAlchemy モデル
├── bigquery.py       BigQuery アップロード関数（全コレクター対応）
└── migrations/       Alembic マイグレーション

config/
└── settings.py       共通設定（パス・API設定・BQフラグ）

schemas/
└── master_fields.json  フィールド定義（canonical名・aliases・source_priority）

scripts/
└── run_experiment.py   実験・検証スクリプト

analytics/
└── csv_exporter.py     分析用 CSV エクスポート
```

---

## コレクター詳細

### 1. company_info — 企業基本情報収集

**Phase 1: URL収集 (`collect_media_urls.py`)**

ScrapingDog Google Search API で各企業×媒体の組み合わせを検索し、URLを特定する。

| 媒体 | URLプレフィックス | レンダリング |
|------|------------------|-------------|
| マイナビ | `job.mynavi.jp/2{6,7}/pc/search/` | ScrapingDog (JS) |
| リクナビ | `job.rikunabi.com/202{6,7}/company` | 静的HTML |
| キャリタス | `job.career-tasu.jp/corp/` | 静的HTML |
| リクルートエージェント | `r-agent.com/kensaku/companydetail/` | ScrapingDog (JS) |
| PR TIMES | `prtimes.jp/main/html/searchrlp/` | Playwright (React SPA) |

**Phase 2: データ収集・統合 (`collect_company_data.py`)**

1. `company_media_urls.csv` の `found` URLを読み込み
2. 媒体ごとに適切な方法で HTML 取得
3. `dl/dt/dd` および `th/td` から構造化フィールドを抽出
4. `field_mapper` で canonical 名にマッピング（3段階ルール + Gemini AI フォールバック）
5. `merge_multi_source()` で複数媒体の値を統合
6. みんかぶ財務データ（売上成長率・営業CF・自己資本率・ROE）を追加
7. **既存 CSV とマージして** `company_data_master.csv` に出力（全実行分を累積保持）

> **チェックポイント:** `data/checkpoints/company_data_checkpoint.json` に収集済み状態を保存。中断しても再開可能。複数回に分けて実行しても既存データは上書きされず保持される。

**出力:**
- `data/output/company_data_master.csv` — 企業×フィールドの統合データ
- `data/output/company_media_urls.csv` — 企業×媒体のURL一覧
- BigQuery `company_data` テーブル（`UPLOAD_TO_BIGQUERY=true` 時）

---

### 2. hr_services — HRサービス利用状況調査

14のHRサービスをスクレイピングし、各サービスに掲載している企業一覧を収集する。

**実行方法:**
```bash
# 全サービス実行
python -m collectors.hr_services.run_all

# 特定サービスのみ
python -m collectors.hr_services.run_all labbase career_ticket

# スクレイピング後にマスターDB構築
python -m collectors.hr_services.run_all --build-master

# 利用可能サービス一覧
python -m collectors.hr_services.run_all --list
```

**対応サービス（14サービス）:**

| キー | サービス名 | カテゴリ |
|-----|-----------|---------|
| labbase | LabBase | 新卒理系 |
| talentbook | Talentbook | 採用広報 |
| type_shinsotsu | type 新卒 | 新卒 |
| onecareer | One Career | 新卒 |
| levtech_rookie | レバテックルーキー | 新卒エンジニア |
| bizreach_campus | Bizreach Campus | 新卒 |
| offerbox | OfferBox | 新卒逆求人 |
| en_tenshoku | エン転職 | 中途 |
| kimisuka | キミスカ | 新卒 |
| caritasu | キャリタス就活 | 新卒 |
| career_ticket | Career Ticket | 新卒 |
| bizreach | Bizreach | 中途 |
| en_ambi | en ambi | 中途 |
| type_chuto | type 中途 | 中途 |

**出力:**
- `data/output/hr_services/{service_key}.csv` — サービス別 CSV
- BigQuery `hr_service_usages` テーブル（`UPLOAD_TO_BIGQUERY=true` 時）

> **BigQuery 書き込み戦略:** `run_all.py` 実行開始時に BQ テーブルを1回 TRUNCATE し、各サービスが WRITE_APPEND で追記する。HRサービス情報は「現在の掲載状況」のため、実行ごとに最新状態に更新される。

---

### 3. contacts — コンタクト情報収集

DBに登録された企業の公式サイトから電話番号を収集し PostgreSQL に保存する。

```bash
python -m collectors.contacts.run
```

**BigQuery 書き込み:** `UPLOAD_TO_BIGQUERY=true` の場合、PostgreSQL の全 `phone_numbers` レコードを `contacts` テーブルに WRITE_TRUNCATE でアップロード。

---

### 4. call_data — 架電ログインポート

営業担当者から収集した架電ログ CSV を PostgreSQL にインポートする。

```bash
# CSVインポート
python -m collectors.call_data.run --csv path/to/call_logs.csv

# バリデーションのみ
python -m collectors.call_data.run --csv path/to/call_logs.csv --dry-run

# エラー行を別ファイルに出力
python -m collectors.call_data.run --csv path/to/call_logs.csv --error-out errors.csv
```

**BigQuery 書き込み:** `UPLOAD_TO_BIGQUERY=true` の場合、インポートした架電ログを `call_logs` テーブルに WRITE_APPEND で追記（歴史的ログのため累積保持）。

---

## BigQuery テーブル構成

`UPLOAD_TO_BIGQUERY=true` で全コレクターが BigQuery に書き込む。

| テーブル | コレクター | 書き込み方式 | 備考 |
|---------|-----------|------------|------|
| `company_data` | company_info | WRITE_TRUNCATE | 毎回最新のマスター全体を反映 |
| `hr_service_usages` | hr_services | WRITE_APPEND + 事前TRUNCATE | 実行ごとに最新掲載状況を反映 |
| `contacts` | contacts | WRITE_TRUNCATE | PostgreSQL の現状をそのまま反映 |
| `call_logs` | call_data | WRITE_APPEND | 歴史的ログを累積保持 |

---

## セットアップ

```bash
# 依存パッケージ
pip install -e .
# BigQuery を使う場合
pip install -e ".[gcp]"

# Playwright ブラウザ（PR TIMES / OfferBox 等のSPA用）
python -m playwright install chromium

# PostgreSQL のマイグレーション
alembic upgrade head

# 環境変数
cp .env.example .env
# .env に必要な値を設定（下記参照）
```

**必須環境変数（`.env`）:**

```ini
# PostgreSQL
DATABASE_URL=postgresql://localhost:5432/company_db

# スクレイピング
SCRAPINGDOG_API_KEY=your_key

# Gemini AI
GEMINI_API_KEY=your_key

# 認証が必要なHRサービス
OFFERBOX_EMAIL=
OFFERBOX_PASSWORD=
BIZREACH_EMAIL=
BIZREACH_PASSWORD=

# GCP / BigQuery（オプション）
GCP_PROJECT_ID=your-gcp-project-id
UPLOAD_TO_BIGQUERY=false   # true にすると全コレクターが BQ に書き込む
```

---

## フィールド正規化 (company_info)

各媒体で表記が異なるラベルを統一する 4 段階パイプライン。

| 段階 | 方式 | 例 |
|------|------|-----|
| 1 | Unicode NFKC 正規化 + alias 完全一致 | `本社所在地` → `本社所在地` |
| 2 | 括弧・注釈除去後に再試行 | `本社（MAP）` → `本社所在地` |
| 3 | 部分一致（3文字以上） | `創業/設立` → `設立` |
| 4 | Gemini AI フォールバック | 上記で解決不能なラベルを AI が判定 |

`schemas/master_fields.json` に各フィールドの canonical 名・aliases・category・source_priority を定義。

---

## ディレクトリ構成

```
企業情報収集/
├── .env.example                    # 環境変数テンプレート
├── pyproject.toml                  # プロジェクト設定・依存関係
├── requirements.txt
├── Dockerfile                      # Cloud Run 用コンテナ
├── config/
│   └── settings.py                 # 共通設定（パス・API・BQ フラグ）
├── db/
│   ├── models.py                   # SQLAlchemy モデル
│   ├── bigquery.py                 # BQ アップロード関数（全コレクター対応）
│   ├── connection.py               # PostgreSQL 接続
│   └── migrations/                 # Alembic マイグレーション
├── collectors/
│   ├── company_info/
│   │   ├── collect_media_urls.py   # Phase 1: URL収集
│   │   ├── collect_company_data.py # Phase 2: スクレイピング・統合・CSV出力
│   │   ├── field_mapper.py         # ラベル正規化・マージロジック
│   │   └── analyze_media_structure.py
│   ├── hr_services/
│   │   ├── run_all.py              # 全スクレイパー実行オーケストレーター
│   │   ├── config.py               # HRサービス設定（SERVICE_REGISTRY 等）
│   │   ├── build_master.py         # マスターDB構築
│   │   └── scrapers/               # 14サービス分のスクレイパー
│   ├── contacts/
│   │   └── run.py                  # 電話番号収集 → PostgreSQL
│   ├── call_data/
│   │   └── run.py                  # 架電ログ CSV → PostgreSQL
│   ├── csv_upload/                 # 汎用 CSV インポート
│   ├── en_hyouban/                 # en 転職口コミ同期
│   └── gemini_enrichment/          # Gemini AI による情報補完
├── schemas/
│   └── master_fields.json          # フィールド定義
├── analytics/
│   └── csv_exporter.py             # 分析用 CSV エクスポート
├── scripts/
│   └── run_experiment.py           # 実験・検証スクリプト
└── data/
    ├── output/                     # CSV 出力ファイル
    │   ├── company_data_master.csv
    │   ├── company_media_urls.csv
    │   └── hr_services/            # HRサービス別 CSV
    ├── checkpoints/                # 中断再開用チェックポイント
    ├── templates/                  # CSV インポート用テンプレート
    └── logs/                       # 実行ログ
```

---

## 技術スタック

| 用途 | ライブラリ |
|------|-----------|
| 静的 HTML 取得 | requests |
| JS レンダリング（マイナビ等） | ScrapingDog API (`dynamic: true`) |
| React SPA レンダリング（PR TIMES / OfferBox） | Playwright (Chromium headless) |
| HTML 解析 | BeautifulSoup4 |
| AI フォールバック・情報補完 | Google Gemini API (`gemini-2.5-flash`) |
| RDB | PostgreSQL + SQLAlchemy + Alembic |
| データウェアハウス | Google BigQuery |
| シークレット管理 | GCP Secret Manager（`USE_SECRET_MANAGER=true` 時） |
