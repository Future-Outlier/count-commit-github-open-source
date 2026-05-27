# 1. 使用輕量級的 Python 官方映像檔作為基底
FROM python:3.10-slim

# 2. 設定時區為台北，確保 Excel 報表的紀錄時間完全正確
ENV TZ=Asia/Taipei

# 3. 安裝系統工具與虛擬螢幕 (Xvfb)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    xvfb \
    tzdata \
    python3-tk \
    && rm -rf /var/lib/apt/lists/*

# 4. 加入軟體源並安裝 Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/googlechrome-linux-keyring.gpg \
    && sh -c 'echo "deb [arch=amd64 signed-by=/usr/share/keyrings/googlechrome-linux-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# 5. 設定容器內的工作目錄
WORKDIR /app

# 6. 安裝所有 Python 必備套件 (包含我們最後找出的關鍵 Jinja2)
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    selenium \
    webdriver-manager \
    Pillow \
    openpyxl \
    tkcalendar \
    Jinja2

# 7. 將本機的爬蟲程式碼複製到容器內
COPY auto_scraper.py /app/auto_scraper.py
COPY 自訂爬蟲清單.txt /app/自訂爬蟲清單.txt

# 8. 直接設定環境變數：強制 Python 不使用緩衝
ENV PYTHONUNBUFFERED=1

# 9. 終極解決方案：手動啟動 Xvfb 並執行 Python
# 這會先在背景啟動一個解析度 1920x1080 的虛擬螢幕 :99，然後讓 Python 使用它
CMD Xvfb :99 -ac -screen 0 1920x1080x24 & DISPLAY=:99 python3 -u auto_scraper.py auto
