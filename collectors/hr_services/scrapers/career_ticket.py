"""キャリアチケット スクレイパー - https://careerticket.jp/industry/{1-10}/"""

from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class Scraper(BaseScraper):
    service_name = "career_ticket"
    output_filename = "career_ticket.csv"

    INDUSTRY_IDS = range(1, 11)  # /industry/1/ 〜 /industry/10/
    MAX_PAGES_PER_INDUSTRY = 50  # 安全上限

    def scrape(self) -> list[dict]:
        for industry_id in self.INDUSTRY_IDS:
            self.logger.info(f"--- 業種 {industry_id}/10 ---")
            page = 1

            while page <= self.MAX_PAGES_PER_INDUSTRY:
                if page == 1:
                    url = f"https://careerticket.jp/industry/{industry_id}/"
                else:
                    url = f"https://careerticket.jp/industry/{industry_id}/?page={page}"

                self.logger.info(f"業種{industry_id} Page {page}: {url}")
                html = self.client.fetch_requests(url)
                if html is None:
                    self.logger.warning(f"HTML取得失敗")
                    break

                companies = self._parse_page(html)
                if not companies:
                    self.logger.info(f"結果なし → 次の業種へ")
                    break

                # 重複排除（同じ企業が複数業種に出る場合）
                for company in companies:
                    self.results.append(company)

                self.logger.info(
                    f"{len(companies)}社取得（累計: {len(self.results)}社）"
                )
                page += 1
                self.client.sleep()

        # 企業名ベースで重複排除
        seen = set()
        unique_results = []
        for r in self.results:
            if r["企業名"] not in seen:
                seen.add(r["企業名"])
                unique_results.append(r)
        self.logger.info(
            f"重複排除: {len(self.results)} → {len(unique_results)}社"
        )
        self.results = unique_results
        return self.results

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業情報を抽出"""
        soup = BeautifulSoup(html, "lxml")
        companies = []

        # 企業カード: li.p-companyCard 内の p.p-companyCard__ttl
        company_cards = soup.select("li.p-companyCard")

        for card in company_cards:
            ttl = card.select_one("p.p-companyCard__ttl")
            if not ttl:
                continue
            name = ttl.get_text(strip=True)
            if not name or len(name) < 2:
                continue
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
