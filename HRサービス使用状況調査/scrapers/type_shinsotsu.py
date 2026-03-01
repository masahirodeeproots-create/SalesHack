"""type就活 スクレイパー - https://typeshukatsu.jp/company/"""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "type_shinsotsu"
    output_filename = "type_shinsotsu.csv"

    BASE_URL = "https://typeshukatsu.jp/company/"

    def scrape(self) -> list[dict]:
        page = 1
        while True:
            self.logger.info(f"Page {page}: POST送信")

            # type就活はform POSTでページネーション
            if page == 1:
                html = self.client.fetch_requests(self.BASE_URL)
            else:
                html = self._fetch_page_post(page)

            if html is None:
                self.logger.warning(f"Page {page}: HTML取得失敗")
                break

            companies = self._parse_page(html)
            if not companies:
                self.logger.info(f"Page {page}: 結果なし → 終了")
                break

            self.results.extend(companies)
            self.logger.info(
                f"Page {page}: {len(companies)}社取得（累計: {len(self.results)}社）"
            )
            page += 1
            self.client.sleep()

        return self.results

    def _fetch_page_post(self, page: int) -> str | None:
        """POSTリクエストで指定ページを取得"""
        import requests as req

        try:
            response = req.post(
                self.BASE_URL,
                data={"page": page},
                headers={"User-Agent": self.client._session.headers["User-Agent"]},
                timeout=20,
            )
            response.raise_for_status()
            response.encoding = response.apparent_encoding
            return response.text
        except Exception as e:
            self.logger.error(f"POST失敗 (page={page}): {e}")
            return None

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
