"""ビズリーチキャンパス スクレイパー - https://br-campus.jp/events
React SPA + GraphQL APIで企業/イベント情報を読み込み。
認証必須のため、現状スキップ。
→ Playwright対応が必要。
"""

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "bizreach_campus"
    output_filename = "bizreach_campus.csv"

    def scrape(self) -> list[dict]:
        self.logger.warning(
            "ビズリーチキャンパスはReact SPA + GraphQL APIで、"
            "イベント/企業情報の取得に認証が必要です。Playwright対応が必要です。"
        )
        return self.results


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    import logging
    from config import LOG_DIR

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "scraper.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    scraper = Scraper()
    scraper.run()
