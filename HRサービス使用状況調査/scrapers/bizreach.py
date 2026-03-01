"""ビズリーチ スクレイパー - https://www.bizreach.jp/job/
Playwright によるログイン認証 + 大量求人ページネーション。
182,427件 → 新規企業発見率が低下したら自動停止。
チェックポイントで中断再開対応。
"""

import os
import asyncio
import re
from dotenv import load_dotenv

from scrapers.base import BaseScraper

load_dotenv()


class Scraper(BaseScraper):
    service_name = "bizreach"
    output_filename = "bizreach.csv"

    LOGIN_URL = "https://www.bizreach.jp/login/"
    JOB_SEARCH_URL = "https://www.bizreach.jp/job/"

    # 新規企業発見率がこの閾値未満になったら停止
    NEW_COMPANY_RATE_THRESHOLD = 0.05
    # 閾値チェック開始ページ数
    THRESHOLD_CHECK_START = 50
    CHECKPOINT_INTERVAL = 20

    def scrape(self) -> list[dict]:
        """Playwrightで非同期実行"""
        asyncio.run(self._async_scrape())
        return self.results

    async def _async_scrape(self):
        from playwright.async_api import async_playwright

        email = os.getenv("BIZREACH_EMAIL")
        password = os.getenv("BIZREACH_PASSWORD")
        if not email or not password:
            self.logger.error("BIZREACH_EMAIL / BIZREACH_PASSWORD が.envに設定されていません")
            return

        # 中断再開
        start_page = self.get_checkpoint()
        known_companies = set()
        if start_page > 0:
            self.results = self.load_existing_results()
            known_companies = {r["企業名"] for r in self.results}
            self.logger.info(
                f"チェックポイントから再開: Page {start_page}（既存{len(self.results)}件、{len(known_companies)}社）"
            )

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()

            try:
                # ログイン
                self.logger.info("ログインページへ遷移中...")
                await page.goto(self.LOGIN_URL, wait_until="networkidle")
                await page.wait_for_timeout(2000)

                # メールアドレス入力
                email_input = page.locator('input[type="email"], input[name="email"], input[name="username"]')
                if await email_input.count() > 0:
                    await email_input.first.fill(email)

                # パスワード入力
                password_input = page.locator('input[type="password"]')
                if await password_input.count() > 0:
                    await password_input.first.fill(password)

                # ログインボタン
                submit_btn = page.locator('button[type="submit"], input[type="submit"]')
                if await submit_btn.count() > 0:
                    await submit_btn.first.click()
                else:
                    login_btn = page.locator('button:has-text("ログイン")')
                    if await login_btn.count() > 0:
                        await login_btn.first.click()

                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(3000)
                self.logger.info("ログイン完了")

                # 求人検索ページネーション
                page_num = max(start_page, 1)
                recent_new_count = 0
                recent_total_count = 0

                while True:
                    url = f"{self.JOB_SEARCH_URL}?p={page_num}&pageSize=20"
                    self.logger.info(f"Page {page_num}: {url}")

                    await page.goto(url, wait_until="networkidle")
                    await page.wait_for_timeout(2000)

                    html = await page.content()
                    items = self._parse_page(html)

                    if not items:
                        self.logger.info(f"Page {page_num}: 結果なし → 終了")
                        break

                    # 新規企業カウント
                    new_count = 0
                    for item in items:
                        if item["企業名"] not in known_companies:
                            known_companies.add(item["企業名"])
                            new_count += 1
                        self.results.append(item)

                    recent_new_count += new_count
                    recent_total_count += len(items)

                    self.logger.info(
                        f"Page {page_num}: {len(items)}件（新規{new_count}社）"
                        f"累計: {len(self.results)}件（{len(known_companies)}社）"
                    )

                    # チェックポイント保存
                    if page_num % self.CHECKPOINT_INTERVAL == 0:
                        self.save_checkpoint(page_num)
                        self.save_csv()

                        # 新規企業発見率チェック
                        if page_num >= self.THRESHOLD_CHECK_START and recent_total_count > 0:
                            rate = recent_new_count / recent_total_count
                            self.logger.info(
                                f"直近{self.CHECKPOINT_INTERVAL}ページの新規企業率: "
                                f"{rate:.1%} ({recent_new_count}/{recent_total_count})"
                            )
                            if rate < self.NEW_COMPANY_RATE_THRESHOLD:
                                self.logger.info(
                                    f"新規企業率 {rate:.1%} < {self.NEW_COMPANY_RATE_THRESHOLD:.0%} → 自動停止"
                                )
                                break
                        recent_new_count = 0
                        recent_total_count = 0

                    page_num += 1

            except Exception as e:
                self.logger.error(f"Playwright エラー: {e}", exc_info=True)
                # 途中結果を保存
                if self.results:
                    self.save_csv()
                    self.save_checkpoint(page_num)
            finally:
                await browser.close()

        self.clear_checkpoint()

    def _parse_page(self, html: str) -> list[dict]:
        """HTML解析で企業名と求人タイトルを抽出"""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "lxml")
        items = []

        # 求人カード: h2/h3見出しとリンク
        job_cards = soup.find_all("a", href=re.compile(r"/job/\d+"))
        seen_urls = set()

        for link in job_cards:
            href = link.get("href", "")
            if href in seen_urls:
                continue
            seen_urls.add(href)

            # カード内からデータ抽出
            card = link.find_parent("li") or link.find_parent("div") or link
            company_name = ""
            title = ""

            # 見出し要素からタイトル
            heading = card.find(["h2", "h3"])
            if heading:
                title = heading.get_text(strip=True)

            # 企業名を探す（ログイン後に表示される）
            text_lines = card.get_text(separator="\n", strip=True).split("\n")
            text_lines = [l.strip() for l in text_lines if l.strip()]

            for line in text_lines:
                if line == title:
                    continue
                # 給与・勤務地パターンをスキップ
                if re.match(r"^\d+万円", line) or "万円〜" in line or "万円～" in line:
                    continue
                if line in ("NEW", "注目", "急募", "おすすめ"):
                    continue
                if len(line) >= 2 and len(line) <= 50:
                    company_name = line
                    break

            if company_name:
                items.append({
                    "企業名": company_name,
                    "タイトル": title,
                    "掲載日": "",
                })

        return items


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
