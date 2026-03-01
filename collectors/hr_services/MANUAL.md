# HRサービス使用状況調査 - 運用マニュアル

---

## 1. 環境構築（初回のみ）

### 1-1. Python仮想環境の作成

```bash
cd /Users/masahiromatsuyama/Product/企業情報収集/HRサービス使用状況調査
python3 -m venv venv
source venv/bin/activate
```

### 1-2. パッケージインストール

```bash
pip install -r requirements.txt
```

### 1-3. Playwrightブラウザのインストール（オファーボックス・ビズリーチ用）

```bash
playwright install chromium
```

### 1-4. .env の確認

`.env` ファイルに以下のキーが設定されていることを確認する。

| キー | 用途 | 確認方法 |
|------|------|---------|
| `SCRAPINGDOG_API_KEY` | JS描画サイトのHTML取得 | [ScrapingDog管理画面](https://www.scrapingdog.com/)でAPI残量確認 |
| `OFFERBOX_EMAIL` / `OFFERBOX_PASSWORD` | オファーボックスログイン | ブラウザで手動ログインできるか確認 |
| `BIZREACH_EMAIL` / `BIZREACH_PASSWORD` | ビズリーチログイン | 同上 |

---

## 2. スクレイピング実行

### 2-0. 仮想環境の有効化（毎回）

```bash
cd /Users/masahiromatsuyama/Product/企業情報収集/HRサービス使用状況調査
source venv/bin/activate
```

### 2-1. 実装済みサービス一覧の確認

```bash
python run_all.py --list
```

以下の14サービスが表示される。

| # | サービスキー | サービス名 | カテゴリ | 取得方式 |
|---|------------|-----------|---------|---------|
| 1 | labbase | Labbase | 新卒 | requests |
| 2 | talentbook | タレントブック | 新卒 | requests |
| 3 | type_shinsotsu | type就活 | 新卒 | requests |
| 4 | onecareer | ワンキャリア | 新卒 | ScrapingDog |
| 5 | levtech_rookie | レバテックルーキー | 新卒 | ScrapingDog |
| 6 | bizreach_campus | ビズリーチキャンパス | 新卒 | ScrapingDog |
| 7 | offerbox | オファーボックス | 新卒 | Playwright |
| 8 | en_tenshoku | EN転職 | 新卒 | requests |
| 9 | kimisuka | キミスカ | 新卒 | requests |
| 10 | caritasu | キャリタス | 新卒 | requests |
| 11 | career_ticket | キャリアチケット | 新卒 | requests |
| 12 | bizreach | ビズリーチ | 中途 | Playwright |
| 13 | en_ambi | アンビ | 中途 | requests |
| 14 | type_chuto | type中途 | 中途 | requests |

### 2-2. 推奨実行順序

サイトの難易度・安定性に応じて以下の順序で実行する。
1つずつ結果を確認しながら進めることを推奨。

#### ステップ1: シンプルなSSRサイト（最初にテスト）

```bash
python run_all.py career_ticket
```

- 最も構造がシンプルで安定
- 業種10カテゴリ × ページネーション
- 所要時間目安: 10〜20分
- 結果確認: `output/career_ticket.csv` を開いて企業名が正しく取得できているか確認

成功を確認したら残りを実行:

```bash
python run_all.py type_shinsotsu caritasu en_tenshoku en_ambi
```

#### ステップ2: フレームワーク系サイト

```bash
python run_all.py labbase talentbook kimisuka
```

- Next.js系サイト。`__NEXT_DATA__` JSON からの抽出を試みる
- JSON抽出が失敗した場合は HTML フォールバックで取得

#### ステップ3: ScrapingDog利用サイト

```bash
python run_all.py onecareer bizreach_campus levtech_rookie
```

- ScrapingDog APIのクレジットを消費する
- レバテックルーキーは565+ページと大量のため時間がかかる
- **中断しても** `logs/levtech_rookie_checkpoint.txt` から自動再開される

#### ステップ4: 大量ページネーションサイト

```bash
python run_all.py type_chuto
```

- 9つの職種カテゴリURL × offset型ページネーション
- 約2,700件の求人データ
- 所要時間目安: 1〜2時間

#### ステップ5: ログイン必須サイト（最後に実行）

```bash
python run_all.py offerbox
python run_all.py bizreach
```

- Playwrightでブラウザを自動操作
- ログインに失敗する場合は `.env` の認証情報を再確認
- ビズリーチは18万件以上 → 新規企業発見率5%未満で自動停止
- **必ず個別に実行し、ログを確認してから次へ進む**

### 2-3. 全サービス一括実行（慣れた後）

```bash
python run_all.py
```

---

## 3. 結果確認

### 3-1. 各サービスCSVの確認

`output/` フォルダに以下の形式でCSVが生成される。

| 列名 | 内容 |
|------|------|
| 企業名 | サービス上での企業表示名（生データ） |
| タイトル | 求人/イベントタイトル（該当なしは空欄） |
| 掲載日 | 掲載日（取得不可は空欄） |

確認ポイント:
- ファイルサイズが 0KB でないこと
- 企業名列にゴミデータ（ナビ要素のテキスト等）が混入していないこと
- Excel で開いて文字化けしていないこと（BOM付きUTF-8）

### 3-2. 取得件数の目安

| サービス | 期待件数 |
|---------|---------|
| Labbase | 約600社 |
| タレントブック | 約486社 |
| type就活 | 約51社 |
| キャリタス | 約206社 |
| キャリアチケット | 約200〜2,400社 |
| レバテックルーキー | 5,000社以上 |
| type中途 | 約2,700件 |
| ビズリーチ | 数千〜数万件 |

件数が極端に少ない場合はパーサーの調整が必要（後述）。

---

## 4. マスターDB構築

### 4-1. マスターマッピングCSV生成

全サービスのCSVが揃った後（一部欠けていてもOK）:

```bash
python run_all.py --master-only
```

または:

```bash
python build_master.py
```

### 4-2. 出力ファイル

| ファイル | 内容 |
|---------|------|
| `output/master_mapping.csv` | 企業名 × 14サービスの利用有無（0/1） |
| `output/fuzzy_review.csv` | 類似企業名クラスタ（レビュー用） |

### 4-3. ファジーマッチレビュー

`fuzzy_review.csv` が生成された場合、以下を確認する。

| クラスタID | 企業名 | 採用名 |
|-----------|--------|--------|
| 1 | ソニー | ← 統合する場合ここに正式名を記入 |
| 1 | ソニーグループ | |
| 2 | NTTドコモ | |
| 2 | NTT docomo | |

- 同一企業と判断できるペアは「採用名」列に正式名を記入
- 別企業の場合は空欄のまま
- レビュー後の反映は手動（現時点では自動マージ機能なし）

---

## 5. トラブルシューティング

### 5-1. 取得件数が0件

| 原因 | 対処 |
|------|------|
| サイト側のHTML構造が変更された | ブラウザで対象URLを開き、開発者ツールで現在のHTML構造を確認。該当スクレイパーのCSSセレクタを修正 |
| IPブロック・レート制限 | `config.py` の `REQUEST_INTERVAL` を 2.0〜3.0 に増やして再実行 |
| ScrapingDog APIクレジット切れ | [管理画面](https://www.scrapingdog.com/)で残量確認。プラン追加 |

### 5-2. ログインが失敗する（offerbox / bizreach）

1. ブラウザで手動ログインできるか確認
2. パスワードが変更されていないか確認
3. 二要素認証が有効になっていないか確認
4. `.env` の認証情報を更新
5. `headless=True` を `headless=False` に変更して挙動を目視確認:
   - `scrapers/offerbox.py` または `scrapers/bizreach.py` の `browser = await p.chromium.launch(headless=False)` に変更

### 5-3. 中断からの再開（大量データスクレイパー）

レバテックルーキー・キャリタス・ビズリーチはチェックポイント機能あり。
途中で止まった場合、同じコマンドを再実行するだけで自動再開される。

```bash
# 中断した場合
python run_all.py levtech_rookie
# → logs/levtech_rookie_checkpoint.txt から自動再開
```

チェックポイントをリセットして最初から実行したい場合:

```bash
rm logs/levtech_rookie_checkpoint.txt
rm output/levtech_rookie.csv
python run_all.py levtech_rookie
```

### 5-4. CSS セレクタの修正方法

サイト構造が変わった場合のスクレイパー修正手順:

1. ブラウザで対象URLを開く
2. 開発者ツール（F12）→ Elements で企業名が含まれる要素を特定
3. 該当スクレイパーの `_parse_page()` メソッド内のセレクタを修正
4. 1ページ分だけテスト:
   ```python
   # 例: scrapers/career_ticket.py を直接実行
   python -m scrapers.career_ticket
   ```

---

## 6. 定期実行する場合

月次など定期的にデータを更新する場合の手順:

1. 前回の `output/` を `output_YYYYMM/` にリネームしてバックアップ
2. チェックポイントファイルを削除: `rm logs/*_checkpoint.txt`
3. 全スクレイパー実行: `python run_all.py`
4. マスターDB構築: `python run_all.py --master-only`
5. `fuzzy_review.csv` をレビュー
6. 前回データとの差分を確認

---

## 7. ファイル一覧

```
HRサービス使用状況調査/
├── .env                    # API キー・認証情報
├── requirements.txt        # Python パッケージ
├── config.py               # サービス定義・共通設定
├── http_client.py          # HTTP クライアント（3モード）
├── company_cleaner.py      # 企業名正規化
├── run_all.py              # オーケストレーター
├── build_master.py         # マスターDB構築
├── MANUAL.md               # ← このファイル
├── scrapers/
│   ├── base.py             # 基底クラス
│   ├── labbase.py          # Labbase
│   ├── talentbook.py       # タレントブック
│   ├── type_shinsotsu.py   # type就活
│   ├── onecareer.py        # ワンキャリア
│   ├── levtech_rookie.py   # レバテックルーキー
│   ├── bizreach_campus.py  # ビズリーチキャンパス
│   ├── offerbox.py         # オファーボックス
│   ├── en_tenshoku.py      # EN転職
│   ├── kimisuka.py         # キミスカ
│   ├── caritasu.py         # キャリタス
│   ├── career_ticket.py    # キャリアチケット
│   ├── bizreach.py         # ビズリーチ
│   ├── en_ambi.py          # アンビ
│   └── type_chuto.py       # type中途
├── output/                 # CSV出力先
└── logs/                   # ログ・チェックポイント
```
