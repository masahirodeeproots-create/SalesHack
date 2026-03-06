"""type就活 スクレイパー - https://typeshukatsu.jp/company/"""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "type_shinsotsu"
    output_filename = "type_shinsotsu.csv"

    BASE_URL = "https://typeshukatsu.jp/company/"

    def scrape(self) -> list[dict]:
        page = 1
        prev_names: set[str] = set()
        while True:
            url = f"{self.BASE_URL}?page={page}"
            self.logger.info(f"Page {page}: {url}")

            html = self.client.fetch_requests(url)
            if html is None:
                self.logger.warning(f"Page {page}: HTML取得失敗")
                break

            companies = self._parse_page(html)
            if not companies:
                self.logger.info(f"Page {page}: 結果なし → 終了")
                break

            # 同一結果ループ検出（終端ページ以降は同じ結果が返る）
            current_names = {c["企業名"] for c in companies}
            if current_names == prev_names:
                self.logger.info(f"Page {page}: 前ページと同一 → 終了")
                break
            prev_names = current_names

            self.results.extend(companies)
            self.logger.info(
                f"Page {page}: {len(companies)}社取得（累計: {len(self.results)}社）"
            )
            page += 1
            self.client.sleep()

        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業情報を抽出"""
        soup = BeautifulSoup(html, "lxml")
        companies = []

        # .card-company > .card > .card-head > h3.card-head-title
        cards = soup.select(".card-company")
        if not cards:
            # 代替: card-head-title を直接探す
            cards = soup.select(".card")

        for card in cards:
            name_el = card.select_one(
                ".card-head-title, h3.card-head-title, h3"
            )
            if name_el:
                name = name_el.get_text(strip=True)
                if name:
                    companies.append({
                        "企業名": name,
                        "タイトル": "",
                        "掲載日": "",
                    })

        return companies


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
