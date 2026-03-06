# データベース設計書

最終更新: 2026-03-07

> **実装者へ（必読）**
> このファイルはプロジェクト設計の唯一の正です。
> セッションをまたいで実装する際は、必ず最初にこのファイルを読んでから作業を開始してください。
> コードより先にこのファイルを更新してください。
>
> **⚠️ 大規模リファクタリング前の注意事項（必読）**
> - 既存コードはEAV設計。新設計はソース別rawdataテーブル設計。根本的に別物。
> - 以下のテーブルは**廃止対象**（Section 9-1参照）。削除前に確認すること。
> - `deals`, `sales_reps`, `products`, `deal_activities` は**設計対象外**（営業管理機能）。絶対に変更・削除しないこと。

---

## 0. 共通ルール

### 全テーブル共通カラム

| カラム | 型 | 説明 | 例外 |
|--------|-----|------|------|
| `original_id` | TEXT (UUID) | 企業マスターへのFK。全テーブル突合の主キー | なし |
| `source_url` | TEXT | スクレイピング元URL | `rawdata_call_logs`（CSVインポート）には不要 |
| `scraped_at` | TIMESTAMP | 収集日時 | `rawdata_call_logs`は`called_at`を使用 |

> `rawdata_persons`もGoogle検索スニペット起点のため`source_url`を持つ。

### rawdataの設計思想
- rawdataには、スクレイパーが収集するよう設計した項目の **生の値（クレンジング前）** をそのまま保存する
- 「全ページの全項目」ではなく「各媒体ごとに指定した項目のみ」を収集する設計
- 目的: 媒体のページ構造変更によって予期せぬ値が入ってきた際の検知・デバッグ用バッファ層

### 企業名正規化（name_normalized）ルール
HRサービス掲載企業名・競合類似企業名への Original ID 付与に使用。

1. 全角英数字・記号 → 半角に変換
2. 法人格を末尾に統一: `(株)` `㈱` → `株式会社`、`(有)` → `有限会社`
3. 前後・連続スペース除去
4. マッチング時は法人格を除いた部分でも比較
5. 完全一致しない場合 → Geminiで候補提示 or 手動確認フラグを立てる

---

## 1. 全体フロー

```
【INPUT】
①登録データ（CSV 13万件）
        ↓
【STEP 1】企業マスターシート生成
  original_id / name / name_normalized / stock_code
  一括生成・以降更新なし。ローカル + BQ 両方に即保存。
        ↓
【STEP 2】スクレイピング → rawdata（29テーブル）
  - 企業マスターの企業名・証券コードをもとに実行
  - 各テーブルに original_id + source_url を自動挿入
  - stock_code がある企業のみ みんかぶ をスクレイピング
        ↓
【STEP 3】項目選定 + クレンジング → 中間データ1
  - 各rawdataテーブルから中間2に渡すカラムを選択（項目選定）
  - 選定後の値を型別ロジックで正規化（クレンジング）
  - HRサービス掲載企業名・競合類似企業名 → original_id に変換（企業マスター参照）
  - ※ 現時点ではrawdata設計時に項目を絞っているが、今後ソース追加や項目変更が生じた場合は中間1側で対応できる柔軟設計
        ↓
【STEP 4】突合・編集 → 中間データ2
  - 企業情報DB: original_id で各メディア中間1を突合（重複項目はsource priorityで解決）
  - 電話番号/担当者/メール: 中間1をそのまま中間2へ
  - 競合類似企業: original_id 変換済みをそのまま
  - HRサービス: original_id 変換済み × 14 を縦持ちに統合
  - 架電データ: 1日1回まとめてBQ形式に整形
        ↓
【STEP 5】BQアップロード（WRITE_APPEND: 行を追加）+ ローカル保存（直近N回分）
        ↓
【STEP 6】中間データ1・中間データ2 を消去（BQアップロード確認後）
※ rawdata①登録データ・企業マスターシートは消去しない
```

---

## 2. rawdata テーブル（全29テーブル）

---

### ① 登録データ（企業情報）

`rawdata_company_info`
ソース: CSVアップロード（13万件）
更新: 一括登録のみ。以降変更なし。

| カラム | 型 |
|--------|-----|
| original_id | TEXT (PK, UUID生成) |
| 企業名 | TEXT |
| 本社都道府県 | TEXT |
| 代表者名 | TEXT |
| 従業員数 | TEXT |
| 企業規模 | TEXT |
| 業種 | TEXT |
| 業種詳細 | TEXT |
| 代表電話番号 | TEXT |
| scraped_at | TIMESTAMP |

---

### ② リクルートエージェント（企業詳細ページ）

`rawdata_ra_company`
ソース: `https://www.r-agent.com/kensaku/companydetail/{id}/`
取得方式: ScrapingDog（JSレンダリング）

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 本社所在地 | TEXT |
| 設立 | TEXT |
| 資本金 | TEXT |
| 事業所 | TEXT |
| 関連会社 | TEXT |
| 株主 | TEXT |
| 株主公開 | TEXT |
| 決算情報 | TEXT |
| 備考 | TEXT |
| 公開求人数 | TEXT |
| scraped_at | TIMESTAMP |

---

### ⑭ リクルートエージェント（求人ページ）

`rawdata_ra_kyujin`
ソース: `https://www.r-agent.com/viewjob/{id}/`（旧: `/kensaku/kyujin/`）
取得方式: ScrapingDog（JSレンダリング）。各社の求人を最初の1件のみ取得。

> 2026年3月のサイトリニューアルにより、旧「この求人に似た求人を探す」セクション（業界_階層1〜5・職種_求人・勤務地_求人・スキル・こだわり）は廃止。
> 新ページからは想定年収と仕事の特徴（タグ）を抽出する。

| カラム | 型 | 備考 |
|--------|-----|------|
| original_id | TEXT | |
| source_url | TEXT | |
| 想定年収 | TEXT | 給与セル内から抽出。例: `690万円～1,235万円` |
| 仕事の特徴 | TEXT | カンマ区切りタグ。例: `業界未経験歓迎, 年間休日120日以上, ...` |
| scraped_at | TIMESTAMP | |

---

### ③ マイナビ

`rawdata_mynavi`
ソース: `https://job.mynavi.jp/{year}/pc/search/corp{id}/outline.html`
取得方式: ScrapingDog（JSレンダリング）

> マイナビが中間データ2に提供する項目は以下の3項目のみ。
> 他の項目（業種・設立・従業員数等）は他ソースから取得するため収集しない。

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 本社郵便番号 | TEXT |
| 採用実績校 | TEXT |
| 採用実績学部学科 | TEXT |
| scraped_at | TIMESTAMP |

---

### ④ リクナビ（企業情報）

`rawdata_rikunabi_company`
ソース: `https://job.rikunabi.com/{year}/company/{id}/`
取得方式: requests（直接）

> 本社所在地はリクナビのページに掲載されていないため項目に含まない。

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 業種 | TEXT |
| 設立 | TEXT |
| 代表者 | TEXT |
| 資本金 | TEXT |
| 従業員数 | TEXT |
| 売上高 | TEXT |
| 純利益 | TEXT |
| 地域別売上高 | TEXT |
| 連結子会社数 | TEXT |
| 連結研究開発費 | TEXT |
| 事業内容 | TEXT |
| 事業所 | TEXT |
| 企業理念 | TEXT |
| 関連会社 | TEXT |
| リクナビ限定情報 | TEXT |
| プレエントリー候補リスト登録人数 | TEXT |
| scraped_at | TIMESTAMP |

---

### 15 リクナビ（採用情報）

`rawdata_rikunabi_employ`
ソース: `https://job.rikunabi.com/{year}/company/{id}/employ/`
取得方式: requests（直接）

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 主な募集職種 | TEXT |
| 主な勤務地 | TEXT |
| 応募資格 | TEXT |
| 積極採用対象 | TEXT |
| 採用予定学科 | TEXT |
| 採用人数 | TEXT |
| 新卒採用人数 | TEXT |
| 初年度月収例 | TEXT |
| 選考フロー | TEXT |
| 給与 | TEXT |
| 手当 | TEXT |
| 昇給 | TEXT |
| 賞与 | TEXT |
| 勤務時間 | TEXT |
| 休日休暇 | TEXT |
| 福利厚生 | TEXT |
| 試用期間 | TEXT |
| 研修制度 | TEXT |
| 自己啓発支援 | TEXT |
| メンター制度 | TEXT |
| キャリアコンサルティング制度 | TEXT |
| 社内検定制度 | TEXT |
| 月平均残業時間 | TEXT |
| 有給休暇取得日数 | TEXT |
| 育児休業取得者数 | TEXT |
| 女性管理職比率 | TEXT |
| 海外赴任者数 | TEXT |
| 受動喫煙対策 | TEXT |
| 過去3年間採用実績 | TEXT |
| 平均勤続年数 | TEXT |
| 平均年齢 | TEXT |
| 提出書類 | TEXT |
| 採用活動開始時期 | TEXT |
| scraped_at | TIMESTAMP |

---

### ⑤ キャリタス

`rawdata_caritasu`
ソース: `https://job.career-tasu.jp/corp/{id}/`
取得方式: requests（直接）

> キャリタスが中間データ2に提供する項目は以下の3項目のみ。
> 他の項目（本社所在地・設立・代表者等）は他ソースから取得するため収集しない。

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 企業名 | TEXT |
| 上場区分 | TEXT |
| 沿革 | TEXT |
| scraped_at | TIMESTAMP |

---

### ⑥ PR TIMES

`rawdata_prtimes`
ソース: `https://prtimes.jp/main/html/searchrlp/company_id/{id}`
取得方式: Playwright（React SPA）

> PR TIMESが中間データ2に提供する項目は以下の7項目のみ。
> 業種・設立・代表者・上場区分・資本金は他ソースから取得するため収集しない。

| カラム | 型 | 備考 |
|--------|-----|------|
| original_id | TEXT | |
| source_url | TEXT | |
| 本社所在地 | TEXT | |
| 電話番号 | TEXT | |
| 企業URL | TEXT | |
| プレスリリース | TEXT (JSON) | `[{"title": "...", "date": "..."}]` 形式、直近3件 |
| SNS_X | TEXT | |
| SNS_Facebook | TEXT | |
| SNS_YouTube | TEXT | |
| scraped_at | TIMESTAMP | |

---

### 16 みんかぶ

`rawdata_minkabu`
ソース: `https://minkabu.jp/stock/{stock_code}/settlement`
取得方式: requests（直接）
対象: stock_code がある企業のみ

列名形式: `{指標}_{期名}` 例: `売上高_2024年6月期`

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 売上高_{期1} | TEXT |
| 売上高_{期2} | TEXT |
| 売上高_{期3} | TEXT |
| 営業利益_{期1} | TEXT |
| 営業利益_{期2} | TEXT |
| 営業利益_{期3} | TEXT |
| 自己資本率_{期1} | TEXT |
| 自己資本率_{期2} | TEXT |
| 自己資本率_{期3} | TEXT |
| ROE_{期1} | TEXT |
| ROE_{期2} | TEXT |
| ROE_{期3} | TEXT |
| 営業CF_{期1} | TEXT |
| 営業CF_{期2} | TEXT |
| 営業CF_{期3} | TEXT |
| フリーCF_{期1} | TEXT |
| フリーCF_{期2} | TEXT |
| フリーCF_{期3} | TEXT |
| 売上成長率_{期1} | TEXT |
| 売上成長率_{期2} | TEXT |
| 売上成長率_{期3} | TEXT |
| scraped_at | TIMESTAMP |

---

### ⑦ エン評判サイト

`rawdata_en_hyouban`
ソース: en-hyouban.com

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| id | TEXT |
| company_name | TEXT |
| total_score | TEXT |
| review_count | TEXT |
| founded_year | TEXT |
| employees | TEXT |
| capital | TEXT |
| listed_year | TEXT |
| avg_salary | TEXT |
| avg_age | TEXT |
| score_growth | TEXT |
| score_advantage | TEXT |
| score_meritocracy | TEXT |
| score_culture | TEXT |
| score_youth | TEXT |
| score_contribution | TEXT |
| score_innovation | TEXT |
| score_leadership | TEXT |
| reviews_text | TEXT |
| scraped_at | TIMESTAMP |

---

### ⑧ 電話番号 & 付随データ

`rawdata_phones`
ソース: Google検索（ScrapingDog）
1社につき複数行OK。

| カラム | 型 | 説明 |
|--------|-----|------|
| original_id | TEXT | FK to 企業マスター |
| source_url | TEXT | 出典スニペットURL |
| 拠点 | TEXT | 東京本社、大阪支社 等 |
| 事業部 | TEXT | 人事部、採用担当窓口 等 |
| ラベル | TEXT | `新卒` / `中途` / `不明` |
| 電話番号 | TEXT | |
| 担当者名リレーションキー | TEXT | FK to rawdata_persons（初期値。架電データで更新可） |
| scraped_at | TIMESTAMP | |

---

### ⑨ 担当者名 & 付随データ

`rawdata_persons`
1社につき複数行OK。

| カラム | 型 | 説明 |
|--------|-----|------|
| original_id | TEXT | FK to 企業マスター |
| source_url | TEXT | 出典スニペットURL |
| 拠点 | TEXT | |
| 事業部 | TEXT | |
| ラベル | TEXT | `新卒` / `中途` / `不明` |
| 担当者名 | TEXT | |
| 電話番号リレーションキー | TEXT | FK to rawdata_phones（初期値。架電データで更新可） |
| scraped_at | TIMESTAMP | |

---

### ⑩ メールアドレス

`rawdata_emails`
1社につき複数行OK。

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| 事業部 | TEXT |
| メールアドレス | TEXT |
| scraped_at | TIMESTAMP |

---

### Ⅺ 競合・類似企業

`rawdata_competitors`
ソース: Gemini API生成

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| 類似企業1 | TEXT |
| 類似企業2 | TEXT |
| 類似企業3 | TEXT |
| 競合企業1 | TEXT |
| 競合企業2 | TEXT |
| 競合企業3 | TEXT |
| scraped_at | TIMESTAMP |

---

### Ⅻ HRサービス（17テーブル）

各サービスごとに独立したテーブル。掲載確認が目的。

| # | サービス名 | テーブル名 | 取得方式 |
|---|-----------|-----------|---------|
| 1 | Labbase | rawdata_hr_labbase | ScrapingDog |
| 2 | タレントブック | rawdata_hr_talentbook | requests |
| 3 | type就活 | rawdata_hr_type_shinsotsu | requests |
| 4 | ワンキャリア | rawdata_hr_onecareer | requests |
| 5 | レバテックルーキー | rawdata_hr_levtech_rookie | Playwright |
| 6 | ビズリーチキャンパス | rawdata_hr_bizreach_campus | Playwright |
| 7 | オファーボックス | rawdata_hr_offerbox | Playwright |
| 8 | EN転職 | rawdata_hr_en_tenshoku | requests |
| 9 | キミスカ | rawdata_hr_kimisuka | Playwright（休止中） |
| 10 | キャリタス（新卒一覧） | rawdata_hr_caritasu | requests |
| 11 | キャリアチケット | rawdata_hr_career_ticket | requests |
| 12 | ビズリーチ | rawdata_hr_bizreach | Playwright |
| 13 | アンビ | rawdata_hr_en_ambi | requests |
| 14 | type中途 | rawdata_hr_type_chuto | requests |
| 15 | ヒトトレ | rawdata_hr_hitotore | CSVアップロード |
| 16 | アカリク | rawdata_hr_acaric | CSVアップロード |
| 17 | サポーターズ | rawdata_hr_supporters | CSVアップロード |

各テーブルのカラム:

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| source_url | TEXT |
| 企業名_掲載名 | TEXT |
| 掲載日 | TEXT |
| scraped_at | TIMESTAMP |

> ⑤キャリタス（`rawdata_caritasu`）は中途・詳細情報取得用。
> Ⅻ-10 `rawdata_hr_caritasu` は新卒一覧への掲載確認用。用途が異なるため別テーブル。

---

### 13 担当者の記録データ（架電ログ）

`rawdata_call_logs`

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| company_name | TEXT |
| sales_rep_name | TEXT |
| called_at | TIMESTAMP |
| phone_number | TEXT |
| phone_status | TEXT |
| product_name | TEXT |
| phone_status_memo | TEXT |
| discovered_number | TEXT |
| discovered_number_memo | TEXT |
| call_result | TEXT |
| spoke_with | TEXT |
| discovered_person_chuto | TEXT |
| discovered_person_shinsotsu | TEXT |
| notes | TEXT |

---

## 3. 中間データ1（項目選定 + クレンジング済み）

rawdataテーブルごとに、①中間2に渡すカラムの選択（項目選定）と②値の正規化（クレンジング）を行ったもの。

**設計方針:**
- 現時点ではrawdata設計時に各ソースの収集項目を絞っているため、rawdata → 中間1 でのカラム削減は最小限
- ただし今後、新しいrawdataソースを追加した場合や、収集中に不要と判断した項目を除外したい場合に備え、**中間1で柔軟に項目選定できる設計**とする
- 各テーブルのカラム構成は基本的にrawdataと対応するが、設定ファイルで選定カラムを制御できるようにすること

テーブル名は `intermediate1_{rawdataのサフィックス}` で対応。

### 中間1のテーブル一覧

| 中間1テーブル | 対応rawdata | 補足 |
|------------|------------|------|
| `intermediate1_company_master` | rawdata①から生成 | 企業マスターシート。以降変更なし |
| `intermediate1_ra_company` | rawdata② | 項目選定 + クレンジング |
| `intermediate1_ra_kyujin` | rawdata14 | 項目選定 + クレンジング |
| `intermediate1_mynavi` | rawdata③ | 項目選定 + クレンジング |
| `intermediate1_rikunabi_company` | rawdata④ | 項目選定 + クレンジング |
| `intermediate1_rikunabi_employ` | rawdata15 | 項目選定 + クレンジング |
| `intermediate1_caritasu` | rawdata⑤ | 項目選定 + クレンジング |
| `intermediate1_prtimes` | rawdata⑥ | 項目選定 + クレンジング |
| `intermediate1_minkabu` | rawdata16 | 項目選定 + クレンジング |
| `intermediate1_en_hyouban` | rawdata⑦ | 項目選定 + クレンジング |
| `intermediate1_phones` | rawdata⑧ | 項目選定 + クレンジング |
| `intermediate1_persons` | rawdata⑨ | 項目選定 + クレンジング |
| `intermediate1_emails` | rawdata⑩ | 項目選定 + クレンジング |
| `intermediate1_competitors` | rawdataⅺ | クレンジングのみ（企業名→ID変換は中間2で実施） |
| `intermediate1_hr_{service}` × 17 | rawdataⅻ × 17 | dedup + クレンジング + **企業名 → original_id 変換** |
| `intermediate1_call_logs` | rawdata13 | クレンジング + **company_name → original_id 変換** + BQ形式整形 |

### 企業マスターシート（`intermediate1_company_master`）

rawdata①から一括生成。以降変更なし（rawdataと同等の扱い）。

| カラム | 型 |
|--------|-----|
| original_id | TEXT (PK) |
| name | TEXT |
| name_normalized | TEXT |
| stock_code | TEXT |

---

## 4. 中間データ2（BQアップロード直前）

### 4-1. 企業情報データベース（`intermediate2_company_info`）

original_id をキーに中間1の各メディアテーブルを突合。
重複項目はsource priorityに従い上位ソースの値を採用（空値はスキップ）。

#### 収録フィールドとsource priority

| フィールド | 主ソース | 副ソース（主が空の場合） |
|-----------|---------|----------------------|
| **基本情報** | | |
| 本社所在地 | ② RA企業詳細 | ⑥ PR TIMES |
| 本社都道府県 | ① CSV | — |
| 本社郵便番号 | ③ マイナビ | — |
| 設立 | ② RA企業詳細 | ④ リクナビ企業情報 |
| 代表者 | ④ リクナビ企業情報 | ① CSV（代表者名） |
| 資本金 | ② RA企業詳細 | ④ リクナビ企業情報 |
| 従業員数 | ④ リクナビ企業情報 | ① CSV |
| 企業規模 | ① CSV | — |
| 業種 | ④ リクナビ企業情報 | — |
| 業種_csv | ① CSV | — |
| 業種詳細 | ① CSV | — |
| 上場区分 | ⑤ キャリタス | — |
| 企業URL | ⑥ PR TIMES | — |
| 電話番号 | ⑥ PR TIMES | — |
| 代表電話番号 | ① CSV | — |
| 事業所 | ② RA企業詳細 | ④ リクナビ企業情報 |
| 関連会社 | ② RA企業詳細 | ④ リクナビ企業情報 |
| 沿革 | ⑤ キャリタス | — |
| **財務情報** | | |
| 売上高 | ④ リクナビ企業情報 | — |
| 純利益 | ④ リクナビ企業情報 | — |
| 地域別売上高 | ④ リクナビ企業情報 | — |
| 連結子会社数 | ④ リクナビ企業情報 | — |
| 連結研究開発費 | ④ リクナビ企業情報 | — |
| 株主 | ② RA企業詳細 | — |
| 株主公開 | ② RA企業詳細 | — |
| 決算情報 | ② RA企業詳細 | — |
| **企業詳細** | | |
| 事業内容 | ④ リクナビ企業情報 | — |
| 企業理念 | ④ リクナビ企業情報 | — |
| リクナビ限定情報 | ④ リクナビ企業情報 | — |
| プレエントリー候補リスト登録人数 | ④ リクナビ企業情報 | — |
| 公開求人数 | ② RA企業詳細 | — |
| 備考 | ② RA企業詳細 | — |
| **PR TIMES** | | |
| プレスリリース | ⑥ PR TIMES | — |
| SNS_X | ⑥ PR TIMES | — |
| SNS_Facebook | ⑥ PR TIMES | — |
| SNS_YouTube | ⑥ PR TIMES | — |
| **求人情報** | | |
| 想定年収 | ⑭ RA求人 | — |
| 仕事の特徴 | ⑭ RA求人 | — |
| **採用情報** | | |
| 主な募集職種 | 15 リクナビ採用情報 | — |
| 主な勤務地 | 15 リクナビ採用情報 | — |
| 応募資格 | 15 リクナビ採用情報 | — |
| 積極採用対象 | 15 リクナビ採用情報 | — |
| 採用予定学科 | 15 リクナビ採用情報 | — |
| 採用人数 | 15 リクナビ採用情報 | — |
| 新卒採用人数 | 15 リクナビ採用情報 | — |
| 採用実績校 | ③ マイナビ | — |
| 採用実績学部学科 | ③ マイナビ | — |
| 初年度月収例 | 15 リクナビ採用情報 | — |
| 選考フロー | 15 リクナビ採用情報 | — |
| 給与 | 15 リクナビ採用情報 | — |
| 手当 | 15 リクナビ採用情報 | — |
| 昇給 | 15 リクナビ採用情報 | — |
| 賞与 | 15 リクナビ採用情報 | — |
| 勤務時間 | 15 リクナビ採用情報 | — |
| 休日休暇 | 15 リクナビ採用情報 | — |
| 福利厚生 | 15 リクナビ採用情報 | — |
| 試用期間 | 15 リクナビ採用情報 | — |
| 研修制度 | 15 リクナビ採用情報 | — |
| 自己啓発支援 | 15 リクナビ採用情報 | — |
| メンター制度 | 15 リクナビ採用情報 | — |
| キャリアコンサルティング制度 | 15 リクナビ採用情報 | — |
| 社内検定制度 | 15 リクナビ採用情報 | — |
| 月平均残業時間 | 15 リクナビ採用情報 | — |
| 有給休暇取得日数 | 15 リクナビ採用情報 | — |
| 女性役員比率 | 15 リクナビ採用情報 | — |
| 女性管理職比率 | 15 リクナビ採用情報 | — |
| 育休取得者数_男性 | 15 リクナビ採用情報 | — |
| 育休取得者数_女性 | 15 リクナビ採用情報 | — |
| 育休対象者数_男性 | 15 リクナビ採用情報 | — |
| 育休対象者数_女性 | 15 リクナビ採用情報 | — |
| 育休取得率_男性 | 15 リクナビ採用情報 | — |
| 育休取得率_女性 | 15 リクナビ採用情報 | — |
| 海外赴任者数 | 15 リクナビ採用情報 | — |
| 受動喫煙対策 | 15 リクナビ採用情報 | — |
| 直近採用者数 | 15 リクナビ採用情報 | — |
| 直近離職者数 | 15 リクナビ採用情報 | — |
| 直近定着率 | 15 リクナビ採用情報 | — |
| 平均勤続年数 | 15 リクナビ採用情報 | — |
| 平均年齢 | 15 リクナビ採用情報 | — |
| 提出書類 | 15 リクナビ採用情報 | — |
| 採用活動開始時期 | 15 リクナビ採用情報 | — |
| **みんかぶ財務**（stock_codeがある企業のみ） | | |
| 売上高_{期1〜3} | 16 みんかぶ | — |
| 営業利益_{期1〜3} | 16 みんかぶ | — |
| 自己資本率_{期1〜3} | 16 みんかぶ | — |
| ROE_{期1〜3} | 16 みんかぶ | — |
| 営業CF_{期1〜3} | 16 みんかぶ | — |
| フリーCF_{期1〜3} | 16 みんかぶ | — |
| 売上成長率_{期1〜3} | 16 みんかぶ | — |
| **エン評判** | | |
| total_score | ⑦ エン評判 | — |
| review_count | ⑦ エン評判 | — |
| founded_year | ⑦ エン評判 | — |
| employees | ⑦ エン評判 | — |
| capital | ⑦ エン評判 | — |
| listed_year | ⑦ エン評判 | — |
| avg_salary | ⑦ エン評判 | — |
| avg_age | ⑦ エン評判 | — |
| score_growth | ⑦ エン評判 | — |
| score_advantage | ⑦ エン評判 | — |
| score_meritocracy | ⑦ エン評判 | — |
| score_culture | ⑦ エン評判 | — |
| score_youth | ⑦ エン評判 | — |
| score_contribution | ⑦ エン評判 | — |
| score_innovation | ⑦ エン評判 | — |
| score_leadership | ⑦ エン評判 | — |
| reviews_text | ⑦ エン評判 | — |
| **メールアドレス** | | |
| 企業メールアドレス | ⑩ メール | — |

> `企業メールアドレス` は `intermediate1_emails` から集約。1社に複数ある場合はカンマ区切り。

---

### 4-2. 電話番号データベース（`intermediate2_phones`）

`intermediate1_phones` をそのまま使用

---

### 4-3. 担当者データベース（`intermediate2_persons`）

`intermediate1_persons` をそのまま使用

---

### 4-4. メールアドレスデータベース（`intermediate2_emails`）

`intermediate1_emails` をそのまま使用

---

### 4-5. 連絡先×担当者リレーションDB（`intermediate2_phone_person_relation`）

電話番号と担当者の対応関係。
初期値はスクレイピング時に生成。架電データの結果（`discovered_number` / `spoke_with`）により事後更新。

| カラム | 型 | 説明 |
|--------|-----|------|
| phone_id | TEXT | → intermediate2_phones の FK |
| person_id | TEXT | → intermediate2_persons の FK |
| source | TEXT | `scraping`（初期推定）/ `call_confirmed`（架電で確認） |
| confirmed_at | TIMESTAMP | 確認日時 |
| call_log_id | TEXT | 架電データのFK（call_confirmed時のみ） |

---

### 4-6. 類似企業データベース（`intermediate2_competitors`）

`intermediate1_competitors` + 企業名 → original_id 変換（`company_resolver` で解決）

| カラム | 型 | 説明 |
|--------|-----|------|
| original_id | TEXT | FK to 企業マスター（エンリッチ対象企業） |
| 類似企業1〜3 | TEXT | Gemini生成の企業名 |
| 類似企業1_id〜3_id | TEXT | 企業マスターとの突合結果（未マッチ時は null） |
| 競合企業1〜3 | TEXT | Gemini生成の企業名 |
| 競合企業1_id〜3_id | TEXT | 企業マスターとの突合結果（未マッチ時は null） |
| scraped_at | TIMESTAMP | |

---

### 4-7. 競合HRサービスデータベース（`intermediate2_hr_services`）

`intermediate1_hr_{service}` × 17 を縦持ちに統合

| カラム | 型 |
|--------|-----|
| original_id | TEXT |
| service_name | TEXT |
| 企業名_掲載名 | TEXT |
| 掲載日 | DATE |
| scraped_at | TIMESTAMP |

---

### 4-8. 一日一回突合架電データ（`intermediate2_call_logs`）

`intermediate1_call_logs` を日次でBQ形式に整形

---

### 4-9. ログ情報（`intermediate2_logs`）

| 計測対象 | カラム |
|---------|--------|
| 実施ログ | `run_id`, `run_at`, `config_name`, `status`, `progress`, `error_message` |
| 項目充填率 | `run_id`, `table_name`, `field_name`, `filled_count`, `total_count`, `fill_rate` |
| エラー検出 | `run_id`, `source`, `company`, `expected_field`, `actual_value`, `error_type` |
| ScrapingDog使用数 | `run_id`, `api_calls`, `api_credits_used` |
| Gemini使用トークン | `run_id`, `model`, `input_tokens`, `output_tokens` |

---

## 5. 最終アウトプット

### ローカル保存（直近N回分。Nは後で決定）

| # | ファイル名 | 内容 |
|---|-----------|------|
| ① | `company_master.csv` | 企業マスターシート |
| ② | `company_info.csv` | 企業情報DB（累積） |
| ③ | `phones.csv` | 電話番号DB（累積） |
| ④ | `persons.csv` | 担当者DB（累積） |
| ⑤ | `emails.csv` | メールアドレスDB（累積） |
| ⑥ | `phone_person_relation.csv` | 連絡先×担当者リレーション（累積） |
| ⑦ | `competitors.csv` | 競合・類似企業DB（累積） |
| ⑧ | `call_logs.csv` | 架電結果DB（累積） |
| ⑨ | `logs.csv` | ログ情報DB（累積） |

### BigQuery（WRITE_APPEND。毎回行を追加し累積保存）

BQテーブル名はローカルファイルと対応。①企業マスターのみ WRITE_TRUNCATE（常に最新の全社分で上書き）。

---

## 6. 連絡先収集ロジック詳細

```
企業名
  ↓
[1] Google検索でスニペット取得 (page_fetcher.py)
    クエリ × 3:
      "{社名} 採用担当 電話番号"
      "{社名} 人事部 採用窓口 連絡先"
      "{社名} 採用 メールアドレス"
    各クエリ10件、title + snippet を連結してスニペット化

  ↓
[2] 連絡先シグナルでフィルタリング (regex_extractor.py)
    以下のいずれかがあれば次ステップへ:
      - 電話番号パターンにマッチ
      - メールアドレスにマッチ
      - キーワード含む（電話/TEL/メール/採用/人事/担当）

  ↓
[3] 正規表現で候補抽出 (regex_extractor.py)
    電話番号:
      3パターン（ハイフンあり / 括弧 / フリーダイヤル）
      桁数チェック: 10〜12桁以外は除外
      前後100文字のコンテキストキーワードをpriorityヒントとして付与
    メールアドレス:
      正規表現: r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"
      ローカルパートからタイプ自動推定:
        recruit/saiyo → "recruit"
        hr/jinji/career → "hr"
        info/contact → "info"
        その他 → "other"

  ↓
[4] Gemini AI構造化 (gemini_analyzer.py)
    モデル: gemini-2.5-flash-lite
    バッチ: 3スニペット / 1 API呼び出し
    入力: スニペットテキスト + 電話番号候補+コンテキスト + メール候補+タイプヒント
    Gemini が出力:
      担当者名（日本人名として妥当なもののみ。役職名単体は除外）
      priority（1=採用直通 / 2=採用部署代表 / 3=会社代表 / 4=その他）
      office_name（拠点名）
      department_name（事業部名）
      ラベル（新卒 / 中途 / 不明）
      email_type（recruit / hr / info / other）
      confidence_score（0.0〜1.0）

  ↓
[5] 重複除去
    電話番号: 数字のみ正規化（10〜11桁）で一致判定
    メール: 小文字化で一致判定
    担当者: {person_name}_{department_name} で一致判定

  ↓
[6] 3テーブルに書き込み（1社複数行OK・重複制御あり）
    → rawdata_phones（⑧）: (original_id, 電話番号) が既存ならスキップ
    → rawdata_persons（⑨）: (original_id, 担当者名, 事業部) が既存ならスキップ
    → rawdata_emails（⑩）: (original_id, メールアドレス) が既存ならスキップ
```

---

## 7. クレンジングロジック（型別）

| 型 | 関数名 | 対象フィールド例 | クレンジング内容 | 出力例 |
|---|---|---|---|---|
| 長テキスト | `clean_long_text` | 事業内容, 備考, 沿革, reviews_text | trim、連続空白→単一スペース | そのまま |
| 短テキスト | `clean_short_text` | 業種, 上場区分, 代表者 | 全角→半角、前後スペース除去 | `代表取締役社長 田中太郎` |
| 数値（円） | `clean_money` | 資本金, 売上高, 純利益 | 全表記を**円単位の整数文字列**に変換。注釈除去 | `1億円`→`100000000` |
| 数値（百万円） | `clean_money_million` | みんかぶ各指標 | 百万円単位→円単位の整数文字列に変換 | `1672377`→`1672377000000` |
| 数値（人） | `clean_people_count` | 従業員数, 採用人数 | 単体/単独優先で**数値のみ**抽出。単位除去 | `1,042名`→`1042` |
| 数値（率） | `clean_ratio` | score_*, ROE, 自己資本率 | `%`除去、数値のみ保持 | `3.5%`→`3.5` |
| 数値（汎用） | `clean_numeric_value` | 月平均残業時間, 平均年齢 | 先頭の数値のみ抽出。注釈除去 | `10.0時間`→`10.0` |
| 日付 | `clean_date` | 設立 | `YYYY年M月`→`YYYY-MM`、`YYYY年`→`YYYY` | `2012年7月`→`2012-07` |
| 住所 | `clean_address` | 本社所在地 | 全角→半角数字、〒郵便番号除去、ダッシュ統一 | `〒100-6640 東京都...`→`東京都...` |
| 電話番号 | `clean_phone` | 電話番号, 代表電話番号 | 全角→半角、`XX-XXXX-XXXX`形式に統一 | `03-1234-5678` |
| メールアドレス | `clean_email` | メールアドレス | 全角→半角、小文字化、形式バリデーション（不正なら空文字） | `info@example.com` |
| 郵便番号 | `clean_zipcode` | 本社郵便番号 | `XXX-XXXX`形式に統一 | `100-0001` |
| URL | `clean_url` | 企業URL, SNS_* | http→https、末尾スラッシュ統一 | `https://example.com/` |
| JSON | `clean_json` | プレスリリース | バリデーションのみ（壊れていれば`[]`） | JSON文字列 |
| 構造化展開 | `parse_female_ratio` | 女性管理職比率 | テキストから女性役員比率+女性管理職比率を分解 | `40.0`, `7.7` |
| 構造化展開 | `parse_childcare_leave` | 育児休業取得者数 | テキストから男女別の取得者数・対象者数・取得率を分解 | `0`, `1`, `100.0` |
| 構造化展開 | `parse_retention` | 過去3年間採用実績 | 直近年度の採用者数・離職者数・定着率を抽出 | `37`, `0`, `100.0` |

---

## 8. フォルダ構造 & 物理ストレージ設計

### 8-1. フォルダ構造

```
企業情報収集/
├── collectors/             # 各スクレイパー（ソースごとにサブディレクトリ）
│   ├── csv_upload/         # ①登録データCSVの取り込み
│   ├── company_info/       # ②〜⑥⑯ メディアスクレイピング
│   ├── contacts/           # ⑧⑨⑩ 連絡先収集（Google検索 + Gemini）
│   ├── en_hyouban/         # ⑦ エン評判
│   ├── gemini_enrichment/  # Ⅺ 競合・類似企業（Gemini生成）
│   ├── hr_services/        # Ⅻ HRサービス14社
│   └── call_data/          # 13 架電ログCSV取り込み
│
├── db/                     # DB接続・モデル・BQ操作
│   ├── connection.py       # PostgreSQL接続
│   ├── models.py           # rawdataテーブル定義（SQLAlchemy）
│   ├── bigquery.py         # BQアップロード処理
│   └── migrations/         # スキーマ変更履歴
│
├── config/
│   └── settings.py         # 環境変数・共通定数（API keys, パス等）
│
├── schemas/
│   └── master_fields.json  # フィールド定義・ソース対応の参照用JSON
│
├── data/
│   ├── checkpoints/        # スクレイピング中断時の再開ポイント（JSON）
│   ├── debug/              # 構造解析用HTML/JSON（開発・デバッグ時のみ）
│   ├── logs/               # 実行ログ（{collector}_{YYYYMMDD_HHMMSS}.log）
│   ├── output/             # 最終出力CSV（直近N回分を保持）
│   │   ├── company_master.csv
│   │   ├── company_info.csv
│   │   ├── phones.csv
│   │   ├── persons.csv
│   │   ├── emails.csv
│   │   ├── phone_person_relation.csv
│   │   ├── competitors.csv
│   │   ├── call_logs.csv
│   │   └── logs.csv
│   └── templates/          # 入力CSVのテンプレート（ユーザー配布用）
│
├── docs/
│   └── DATABASE_DESIGN.md  # 本ファイル（設計の唯一の正）
│
└── scripts/
    ├── auto_pipeline.sh    # 全体パイプライン実行シェル
    └── reset_bigquery.py   # BQテーブルリセット用（開発時のみ）
```

---

### 8-2. 物理ストレージ対応表

| 論理レイヤー | 保存場所 | 形式 | 消去タイミング |
|---|---|---|---|
| rawdata（①登録データ） | PostgreSQL | DB テーブル | **消去しない**（マスター） |
| rawdata（②〜⑬ スクレイピング） | PostgreSQL | DB テーブル | BQアップロード確認後に TRUNCATE |
| 中間データ1 | インメモリ（pandas） | DataFrame | BQアップロード完了後にメモリ解放 |
| 中間データ2 | インメモリ（pandas） | DataFrame | BQアップロード完了後にメモリ解放 |
| 最終データ | BQ + `data/output/` | BQ テーブル / CSV | BQは累積保持。ローカルは直近N回分のみ |
| チェックポイント | `data/checkpoints/` | JSON | 次回スクレイピング完了後に上書き |
| ログ | `data/logs/` | .log | 手動管理（削除しない） |

> **中間データをインメモリにする理由:**
> rawdataから都度再生成できるため、ディスク保存は不要。
> 13万件 × 全フィールドでもメモリに収まる規模感（数百MB以内を想定）。

---

### 8-3. データライフサイクル

```
[スクレイピング実行]
  → rawdata①（登録データ）: PostgreSQL に永続保存（以降変更なし）
  → rawdata②〜⑬: PostgreSQL に書き込み。中断しても再開可能。

[クレンジング〜BQアップロード]
  → rawdata を読み込んで 中間1 を pandas DataFrame として生成
  → 中間1 から 中間2 を pandas DataFrame として生成
  → 中間2 を BQ に WRITE_APPEND でアップロード
  → 中間2 を data/output/ に CSV 保存（N回分ローリング）

[後処理]
  → rawdata②〜⑬ を PostgreSQL から TRUNCATE
  → pandas の DataFrame をメモリ解放
  → チェックポイントを更新
```

---

## 9. 移行ガイド（既存コードからの変更）

### 9-1. 廃止済みPostgreSQLテーブル一覧

> **✅ 移行完了済み（2026-03）**。以下のテーブルは DROP 済み。関連ファイル（`db/seed.py`, `db/views.sql`, `schemas/master_fields.json`, `scripts/migrate_to_rawdata.py`）も削除済み。

| テーブル名 | 旧役割 | 新設計での代替 |
|---|---|---|
| `field_definitions` | フィールド定義マスタ（EAVのAttribute） | 廃止。rawdata テーブル設計に置き換え |
| `company_field_values` | 企業×フィールドの最新値（EAV） | 廃止。中間データ2（インメモリpandas）に置き換え |
| `company_field_values_history` | 全ソースの生データ履歴（EAV） | 廃止。rawdata①〜⑯テーブルに置き換え |
| `hr_services` | HRサービスマスタ | 廃止。`config.py` の `SERVICE_REGISTRY` で管理 |
| `company_service_usage` | 企業×HRサービス利用状況 | 廃止。`rawdata_hr_*` × 14テーブルに置き換え |
| `person_transfers` | 担当者異動履歴 | 廃止。新設計に異動履歴の概念なし |

---

### 9-2. 設計対象外テーブル（変更・削除禁止）

以下は**営業管理機能**であり、このデータ収集パイプラインの設計対象外。
リファクタリング時に絶対に変更・削除しないこと。

| テーブル名 | 役割 |
|---|---|
| `sales_reps` | 営業担当者マスタ |
| `products` | 商品・サービスマスタ |
| `deals` | 商談管理 |
| `deal_activities` | 商談活動履歴 |

---

### 9-3. BQアップロードポリシーの変更（全テーブル）

現在の `db/bigquery.py` は**全テーブルWRITE_TRUNCATE**で実装されているが、
新設計では以下の通り変更が必要：

| テーブル | 現在 | 新設計 |
|---|---|---|
| 企業マスター | 未実装 | **WRITE_TRUNCATE** |
| 企業情報DB・その他全て | WRITE_TRUNCATE | **WRITE_APPEND** |
| 架電ログ | WRITE_APPEND ✅ | WRITE_APPEND（変更なし） |

---

## 10. 残課題（実装前に要確認）

| # | 内容 | ステータス |
|---|------|-----------|
| 1 | ローカル保存のN回数（直近何回分保存するか） | **後で決定** |
| 2 | 中間データ2 企業情報DBの列構成に追加・変更があれば随時更新 | **継続確認** |
