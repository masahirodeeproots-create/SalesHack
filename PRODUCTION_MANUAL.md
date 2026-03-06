# 企業情報収集 本番実行マニュアル

> **対象**: 100社の全情報収集（企業情報 + 連絡先 + 口コミ + 競合・類似企業 + HRサービス）
> **前提**: 13万社の企業情報CSVは別途用意済み

---

## 全体フロー概要

```
[Phase 0] 環境準備
[Phase 1] データリセット（PostgreSQL + BigQuery）
[Phase 2] 13万社 → PostgreSQL インポート
[Phase 3] 100社リスト設定
[Phase 4] Step1~3 実行（媒体URL収集 → 企業データ収集 → 連絡先収集）
[Phase 5] 連絡先を PostgreSQL に書き込む（本番パイプライン）
[Phase 6] 類似・競合企業を Gemini で生成
[Phase 7] エン評判データ同期
[Phase 8] HRサービス収集（3媒体）
[Phase 9] BQ アップロード確認
```

---

## Phase 0: 環境準備

```bash
cd /Users/masahiromatsuyama/Product/企業情報収集
source venv/bin/activate

# 環境変数確認
cat .env
# → SCRAPINGDOG_API_KEY, GEMINI_API_KEY(_2, _3), UPLOAD_TO_BIGQUERY が設定されていること
```

---

## Phase 1: データリセット

### 1-1. PostgreSQL リセット（テストデータを全削除）

```bash
PSQL="/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql"

# DB削除 → 再作成
$PSQL postgres -c "DROP DATABASE IF EXISTS company_db;"
$PSQL postgres -c "CREATE DATABASE company_db;"

# テーブル作成（rawdata テーブル含む）
venv/bin/python -c "from db.connection import init_db; init_db()"

echo "PostgreSQL リセット完了"
```

### 1-2. BigQuery リセット（テストデータを削除）

```bash
# 確認のみ（dry-run）
venv/bin/python scripts/reset_bigquery.py --dry-run

# 本番削除（各テーブルの削除確認を求めてくる）
venv/bin/python scripts/reset_bigquery.py
```

> **call_logs は削除されません**（架電履歴は消えないよう保護）
> 削除したい場合は `--include-call-logs` を追加

---

## Phase 2: 13万社 → PostgreSQL インポート

> **安全性**: UPSERT方式のため同じCSVを複数回インポートしても重複しない
> **既存データへの影響**: 既存企業はスキップ（証券コードのみ未設定なら更新）

```bash
# パイプ区切り（既存フォーマット）の場合
venv/bin/python -m collectors.csv_upload.company_importer \
  --csv /path/to/companies_130k.csv \
  --delimiter "|" \
  --batch-size 1000

# カンマ区切りCSVの場合
venv/bin/python -m collectors.csv_upload.company_importer \
  --csv /path/to/companies_130k.csv \
  --delimiter "," \
  --batch-size 1000
```

**CSVフォーマット（列名）:**
```
企業名 | 本社都道府県 | 代表者名 | 従業員数 | 企業規模 | 業種 | 業種詳細 | 代表電話番号 | 証券コード（任意）
```

**完了確認:**
```bash
/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql company_db \
  -c "SELECT COUNT(*) FROM companies;"
# → 13万件前後が入っていればOK
```

---

## Phase 3: 100社リストを設定

`collectors/company_info/collect_media_urls.py` の `COMPANIES` リストを
収集対象の100社に書き換える。

```python
COMPANIES = [
    "収集対象企業名1",
    "収集対象企業名2",
    ...  # 100社分
]
```

> **重要**: `name_normalized` と完全一致する名前を使うこと（インポート時の企業名と同じ表記）

---

## Phase 4: Step1~3 実行

> 実行時間目安: **約18~20時間**（100社 × 5媒体 + 連絡先収集）

```bash
# バックグラウンド実行（BQアップロードも同時に行う）
export UPLOAD_TO_BIGQUERY=true
nohup venv/bin/python scripts/run_experiment.py \
  > data/logs/production_run.log 2>&1 &

echo "PID: $!"  # プロセスIDを記録しておく
```

**進捗確認:**
```bash
# ログをリアルタイムで確認
tail -f data/logs/production_run.log

# フィールド充填率をリアルタイムで確認（別ターミナルで）
watch -n 60 '/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql company_db \
  -c "SELECT フィールド名, 取得済み社数, 充填率 FROM v_collection_progress ORDER BY 充填率 DESC LIMIT 20;"'
```

**完了したら確認:**
```bash
cat data/output/experiment_report.txt
ls -la data/output/
# → company_media_urls.csv, company_data_master.csv, contacts_experiment.csv が生成されていること
```

---

## Phase 5: 連絡先を PostgreSQL に書き込む（本番パイプライン）

> Step3 は CSV 出力のみ。**電話番号・担当者を PostgreSQL に書き込むには別途実行が必要**

```bash
# 100社分の連絡先をDBに書き込む
venv/bin/python -m collectors.contacts.run --limit 100
```

> **チェックポイント**: 途中で止まった場合はそのまま再実行すれば再開
> `--reset-checkpoint` オプションで最初からやり直せる

**確認:**
```bash
/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql company_db \
  -c "SELECT COUNT(*) FROM phone_numbers; SELECT COUNT(*) FROM company_persons;"
```

---

## Phase 6: 類似・競合企業を Gemini で生成

> **前提**: Phase 5 完了後（DB に企業データが入っている状態）
> **実行時間目安**: 100社 × 2秒 ≒ **約3分20秒**

```bash
venv/bin/python -m collectors.gemini_enrichment.sync --limit 100
```

**確認:**
```bash
/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql company_db \
  -c "SELECT COUNT(*) FROM rawdata_competitors;"
```

---

## Phase 7: エン評判データ同期

> **前提**: `scraping_en_hyouban` 側でスクレイピングが完了していること

```bash
# 1. scraping_en_hyouban プロジェクトでスクレイピング実行
#    （詳細は scraping_en_hyouban のマニュアル参照）
cd /Users/masahiromatsuyama/Product/scraping_en_hyouban
# ... スクレイピング実行 ...

# 2. 結果を本プロジェクトの DB + BQ に同期
cd /Users/masahiromatsuyama/Product/企業情報収集
UPLOAD_TO_BIGQUERY=true venv/bin/python -m collectors.en_hyouban.sync
```

---

## Phase 8: HRサービス収集（3媒体）

**対象媒体を選んで実行**（例: labbase, career_ticket, bizreach_campus）:

```bash
cd collectors/hr_services

# 利用可能なサービス一覧確認
python run_all.py --list

# 3媒体を実行（BQにアップロード）
UPLOAD_TO_BIGQUERY=true python run_all.py labbase career_ticket bizreach_campus
```

**全サービス一覧:**
| キー | 名前 | カテゴリ |
|---|---|---|
| labbase | Labbase | 新卒 |
| talentbook | タレントブック | 新卒 |
| type_shinsotsu | type就活 | 新卒 |
| onecareer | ワンキャリア | 新卒 |
| levtech_rookie | レバテックルーキー | 新卒 |
| bizreach_campus | ビズリーチキャンパス | 新卒 |
| offerbox | オファーボックス | 新卒 |
| kimisuka | キミスカ | 新卒 |
| caritasu | キャリタス | 新卒 |
| career_ticket | キャリアチケット | 新卒 |
| en_tenshoku | EN転職 | 中途 |
| bizreach | ビズリーチ | 中途 |
| en_ambi | アンビ | 中途 |
| type_chuto | type中途 | 中途 |

---

## Phase 9: 完了確認

### PostgreSQL 確認

```bash
PSQL="/opt/homebrew/Cellar/postgresql@15/15.15_1/bin/psql"

# フィールド充填率（全フィールド）
$PSQL company_db -c "
  SELECT カテゴリ, フィールド名, 取得済み社数, 充填率
  FROM v_collection_progress
  ORDER BY カテゴリ, 充填率 DESC;"

# 企業ごとの収集状況
$PSQL company_db -c "
  SELECT 企業名, 取得済みフィールド数, 充填率, 電話あり, 担当者あり
  FROM v_company_coverage
  ORDER BY 充填率 DESC
  LIMIT 20;"

# 連絡先サマリー
$PSQL company_db -c "
  SELECT
    COUNT(DISTINCT company_id) AS 電話取得企業数,
    COUNT(*) AS 総電話番号数
  FROM phone_numbers;"
```

### BigQuery 確認（BQ コンソールまたは CLI）

```bash
# テーブル行数確認
bq show --format=prettyjson company-data-collector:company_data.companies
bq show --format=prettyjson company-data-collector:company_data.contacts
bq show --format=prettyjson company-data-collector:company_data.hr_service_usages
bq show --format=prettyjson company-data-collector:company_data.en_hyouban_reviews
```

---

## 途中で止まった場合の再開

| フェーズ | 再開方法 |
|---|---|
| Phase 4 (Step1-3) | `run_experiment.py` を再実行（チェックポイントにより自動再開） |
| Phase 5 (連絡先DB書き込み) | `contacts/run.py` を再実行（未処理企業のみ処理） |
| Phase 6 (Gemini) | `gemini_enrichment/sync.py` を再実行（未処理企業のみ処理） |
| Phase 7 (エン評判) | `en_hyouban/sync.py` を再実行（UPSERT のため安全） |
| Phase 8 (HRサービス) | `run_all.py` を再実行（前回取得分は上書きされる） |

---

## よくある問題

| 問題 | 対処法 |
|---|---|
| `GEMINI_API_KEY が設定されていません` | `.env` を確認。`source venv/bin/activate` 後に再実行 |
| ScrapingDog エラー多発 | APIクレジット残量確認。`SCRAPINGDOG_API_KEY` 確認 |
| rawdata テーブルが存在しない | `venv/bin/python -c "from db.connection import init_db; init_db()"` を実行 |
| BQ 認証エラー | `gcloud auth application-default login` を実行 |
| PostgreSQL 接続エラー | PostgreSQL が起動しているか確認: `brew services start postgresql@15` |
