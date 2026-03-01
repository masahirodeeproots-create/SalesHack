"""Labbase スクレイパー - https://compass.labbase.jp/search
SPA(React)のため ScrapingDog dynamic=true を使用。
企業カードはsection要素、企業名は各カード内の最初のh3。
"""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "labbase"
    output_filename = "labbase.csv"

    BASE_URL = "https://compass.labbase.jp/search"

    def scrape(self) -> list[dict]:
        page = 0
        while True:
            url = f"{self.BASE_URL}?mode=default&page={page}"
            self.logger.info(f"Page {page}: {url}")

            html = self.client.fetch_scrapingdog(url, dynamic=True)
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

        # 企業名で重複排除
        seen = set()
        unique = []
        for r in self.results:
            if r["企業名"] not in seen:
                seen.add(r["企業名"])
                unique.append(r)
        self.results = unique
        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業カードから企業名を抽出"""
        soup = BeautifulSoup(html, "lxml")
        companies = []
        seen = set()

        # 企業カード: section要素内のh3が企業名
        # Tailwindのgroup/cardクラスを持つsection、またはリンク付きsection
        sections = soup.find_all("section")
        for section in sections:
            h3 = section.find("h3")
            if not h3:
                continue
            name = h3.get_text(strip=True)
            if not name or len(name) < 2 or name in seen:
                continue
            # ナビゲーション等のh3を除外
            if name in ("検索条件", "新着企業", "おすすめ", "人気企業"):
                continue
            seen.add(name)
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
