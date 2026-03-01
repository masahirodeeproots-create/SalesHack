FROM python:3.13-slim

# Playwright に必要なシステム依存パッケージ
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright ブラウザインストール（Chromium のみ）
RUN playwright install chromium

# アプリケーションコード
COPY config/ config/
COPY collectors/ collectors/
COPY db/ db/
COPY schemas/ schemas/
COPY data/ data/

# GCP 環境では Secret Manager を使用
ENV USE_SECRET_MANAGER=true

CMD ["python", "-m", "collectors.company_info.collect_company_data"]
