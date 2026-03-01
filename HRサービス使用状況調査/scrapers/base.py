"""全スクレイパーの抽象基底クラス"""

import csv
import logging
from abc import ABC, abstractmethod
from pathlib import Path

from config import OUTPUT_DIR, LOG_DIR, CSV_COLUMNS, CSV_ENCODING
from http_client import HttpClient


class BaseScraper(ABC):
    """
    全14スクレイパーの基底クラス。
    CSV出力、ログ、チェックポイントによる中断再開を共通化する。
    """

    service_name: str = ""  # サブクラスで設定（例: "labbase"）
    output_filename: str = ""  # サブクラスで設定（例: "labbase.csv"）

    def __init__(self, client: HttpClient | None = None):
        self.client = client or HttpClient()
        self.logger = logging.getLogger(f"scraper.{self.service_name}")
        self.results: list[dict] = []

    @abstractmethod
    def scrape(self) -> list[dict]:
        """
        全ページをスクレイピングしてself.resultsに格納して返す。
        各dictは {"企業名": str, "タイトル": str, "掲載日": str} の形式。
        """
        pass

    def run(self):
        """スクレイピング実行 → CSV保存の一連フロー"""
        self.logger.info(f"=== {self.service_name} スクレイピング開始 ===")
        try:
            self.scrape()
            self.save_csv()
            self.logger.info(
                f"=== {self.service_name} 完了: {len(self.results)}件 ==="
            )
        except Exception as e:
            self.logger.error(f"{self.service_name} エラー: {e}", exc_info=True)
            # 途中結果があれば保存
            if self.results:
                self.save_csv()
                self.logger.info(
                    f"途中結果を保存: {len(self.results)}件"
                )

    def save_csv(self):
        """結果をBOM付きUTF-8のCSVに保存"""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / self.output_filename

        with open(output_path, "w", newline="", encoding=CSV_ENCODING) as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(self.results)

        self.logger.info(f"CSV保存: {output_path}（{len(self.results)}件）")

    # --- チェックポイント（大量データスクレイパー用）---

    def get_checkpoint(self) -> int:
        """最後に成功したページ番号を取得（チェックポイントファイルから）"""
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        checkpoint_path = LOG_DIR / f"{self.service_name}_checkpoint.txt"
        if checkpoint_path.exists():
            try:
                return int(checkpoint_path.read_text().strip())
            except ValueError:
                return 0
        return 0

    def save_checkpoint(self, page: int):
        """現在のページ番号をチェックポイントに保存"""
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        checkpoint_path = LOG_DIR / f"{self.service_name}_checkpoint.txt"
        checkpoint_path.write_text(str(page))

    def clear_checkpoint(self):
        """チェックポイントファイルを削除"""
        checkpoint_path = LOG_DIR / f"{self.service_name}_checkpoint.txt"
        if checkpoint_path.exists():
            checkpoint_path.unlink()

    def load_existing_results(self) -> list[dict]:
        """既存CSV出力があれば読み込み（中断再開用）"""
        output_path = OUTPUT_DIR / self.output_filename
        if not output_path.exists():
            return []
        rows = []
        with open(output_path, newline="", encoding=CSV_ENCODING) as f:
            for row in csv.DictReader(f):
                rows.append(row)
        self.logger.info(f"既存結果ロード: {len(rows)}件")
        return rows
