"""キミスカ スクレイパー - https://kimisuka.com/
学生向け企業検索はログイン必須。公開ページ(/company/case)は
企業導入事例（法人向けページ）で企業リストではない。
→ Playwright対応が必要。
"""

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "kimisuka"
    output_filename = "kimisuka.csv"

    def scrape(self) -> list[dict]:
        self.logger.warning(
            "キミスカの企業検索はログイン必須です。"
            "公開ページ(/company/case)は法人向け導入事例で企業リストではありません。"
            "Playwright対応が必要です。"
        )
        return self.results


if __name__ == "__main__":
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
