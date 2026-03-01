"""レバテックルーキー スクレイパー - https://rookie.levtech.jp/company/
Nuxt SPA + API経由で企業リストを動的読み込み。
ScrapingDogでも企業カードが描画されないため、現状スキップ。
→ Playwright対応が必要。
"""

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "levtech_rookie"
    output_filename = "levtech_rookie.csv"

    def scrape(self) -> list[dict]:
        self.logger.warning(
            "レバテックルーキーはNuxt SPAで企業リストがAPI経由で読み込まれるため、"
            "現在の方式ではスクレイピングできません。Playwright対応が必要です。"
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
