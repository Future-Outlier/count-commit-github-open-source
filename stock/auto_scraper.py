import requests
from bs4 import BeautifulSoup
import time
import re
import os
import sys  # 新增 sys 模組以供 CI 環境控制退出狀態碼
import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from openpyxl.styles import Alignment

# ==========================================
# 基礎設定與共用資料夾 (動態相對路徑)
# ==========================================
try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

STOCK_NAME_CACHE = {}

# ==========================================
# 股票名稱與資料爬取模組
# ==========================================
def get_stock_name_from_web(stock_id):
    stock_id = str(stock_id).strip()
    if " " in stock_id:
        return stock_id
    if stock_id in STOCK_NAME_CACHE:
        return f"{stock_id} {STOCK_NAME_CACHE[stock_id]}"

    url = f"https://tw.stock.yahoo.com/quote/{stock_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=3)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.find('title')
            if title and "(" in title.text:
                stock_name = title.text.split("(")[0].strip()
                if len(stock_name) < 15 and "Yahoo" not in stock_name:
                    STOCK_NAME_CACHE[stock_id] = stock_name
                    return f"{stock_id} {stock_name}"
    except Exception:
        pass
    return stock_id

def get_stock_data(symbol):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    data = {'股票代碼': symbol}

    quote_url = f'https://tw.stock.yahoo.com/quote/{symbol}.TW'
    try:
        response = requests.get(quote_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        main_info = soup.find_all('li', class_='PriceDetailItem')
        for item in main_info:
            label = item.find('span', class_='C(#6e7780)').text
            value = item.find_all('span')[1].text
            data[label] = value

        data['成交價'] = soup.find('span', class_='Fz(32px)').text

        price_elem = soup.find('span', class_='Fz(32px)')
        if price_elem:
            trend_amt_elem = price_elem.find_next_sibling()
            trend_pct_elem = trend_amt_elem.find_next_sibling() if trend_amt_elem else None

            def format_trend(elem):
                if not elem: return "無資料"
                class_str = " ".join(elem.get('class', []))
                raw_text = elem.text.replace('△', '').replace('▽', '').strip()
                if 'c-trend-up' in class_str:
                    return f"🔺 {raw_text}"
                elif 'c-trend-down' in class_str:
                    return f"🔻 {raw_text}"
                else:
                    return raw_text

            data['漲跌'] = format_trend(trend_amt_elem)
            data['漲跌幅'] = format_trend(trend_pct_elem)
        else:
            data['漲跌'] = "無資料"
            data['漲跌幅'] = "無資料"

    except Exception as e:
        print(f"⚠️ 抓取 {symbol} 報價時發生錯誤: {e}")

    inst_url = f'https://tw.stock.yahoo.com/quote/{symbol}.TW/institutional-trading'
    try:
        response = requests.get(inst_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        categories = ['外資', '投信', '自營商', '三大法人']
        table = soup.select('div[class*="table-body-wrapper"] .table-row')

        for idx, row in enumerate(table[:4]):
            cols = row.find_all('div')
            name = categories[idx]
            data[f'{name}_買賣超'] = cols[4].text
            data[f'{name}_連買連賣'] = cols[5].text
    except Exception as e:
        print(f"⚠️ 抓取 {symbol} 法人資料時發生錯誤: {e}")

    broker_url = f'https://tw.stock.yahoo.com/quote/{symbol}.TW/broker-trading'
    try:
        response = requests.get(broker_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        target_fields = ['主力買賣超(張)', '買賣超佔成交量']
        for field in target_fields:
            elem = soup.find(string=lambda text: text and field in text)
            if elem:
                container = elem.parent.parent
                strings = list(container.stripped_strings)
                if len(strings) >= 2: data[field] = strings[-1]
                else: data[field] = "解析失敗"
            else: data[field] = "無資料"
    except Exception as e:
        print(f"⚠️ 抓取 {symbol} 主力進出資料時發生錯誤: {e}")

    return data

# ==========================================
# 爬蟲主程式模組 (無UI版本)
# ==========================================
def run_scraping_task(stock_items):
    current_time = datetime.datetime.now()
    today_date = current_time.strftime("%Y%m%d")
    datetime_str = current_time.strftime("%Y%m%d%H%M%S")
    all_results = []

    PARENT_SCRAPE_DIR = os.path.join(BASE_DIR, "股票爬蟲與看圖")
    os.makedirs(PARENT_SCRAPE_DIR, exist_ok=True)

    folder_name = os.path.join(PARENT_SCRAPE_DIR, f"爬蟲結果_{datetime_str}")
    os.makedirs(folder_name, exist_ok=True)

    options = webdriver.ChromeOptions()
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-notifications')
    options.add_argument('--headless')

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.page_load_strategy = 'eager'

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    # === Excel 存檔與格式化函式 ===
    def save_to_excel(results_data):
        if not results_data: return
        df = pd.DataFrame(results_data)

        if '日期' in df.columns:
            df = df.drop(columns=['日期'])

        cols = df.columns.tolist()
        if '股票代碼' in cols:
            cols.insert(0, cols.pop(cols.index('股票代碼')))

        if '成交價' in cols:
            temp_cols = []
            for target in ['買賣超佔成交量', '漲跌', '漲跌幅']:
                if target in cols:
                    temp_cols.append(cols.pop(cols.index(target)))
            base_idx = cols.index('成交價')
            for i, col_name in enumerate(temp_cols):
                cols.insert(base_idx + 1 + i, col_name)

        df = df[cols]

        def color_buy_sell(val):
            try:
                clean_val = str(val).replace('🔺', '').replace('🔻', '').replace('🟢', '').replace(',', '').strip()
                num = float(clean_val)
                if num < 0: return 'background-color: #92D050'
                elif num > 0: return 'background-color: #FF9999'
            except: pass
            return ''

        def color_kd(val):
            try:
                num = float(str(val).replace(',', '').replace('%', '').strip())
                if num < 20: return 'background-color: #ADD8E6'
                elif num > 80: return 'background-color: #FFFF99'
            except: pass
            return ''

        def color_parentheses(val):
            try:
                match = re.search(r'\(([-+]?[\d.,]+)\)', str(val))
                if match:
                    num = float(match.group(1).replace(',', '').strip())
                    if num > 0: return 'background-color: #87CEEB'
                    elif num < 0: return 'background-color: #92D050'
            except: pass
            return ''

        buy_sell_cols = [c for c in ['外資_買賣超', '投信_買賣超', '自營商_買賣超', '三大法人_買賣超','主力買賣超(張)', 'MACD數值'] if c in df.columns]
        kd_cols = [c for c in ['K9數值', 'D9數值'] if c in df.columns]
        parentheses_cols = [c for c in ['外資_連買連賣', '投信_連買連賣', '自營商_連買連賣', '三大法人_連買連賣'] if c in df.columns]

        def apply_style(dataframe):
            styler = dataframe.style
            if hasattr(styler, 'map'):
                return styler.map(color_buy_sell, subset=[c for c in buy_sell_cols if c in dataframe.columns])\
                             .map(color_kd, subset=[c for c in kd_cols if c in dataframe.columns])\
                             .map(color_parentheses, subset=[c for c in parentheses_cols if c in dataframe.columns])
            else:
                return styler.applymap(color_buy_sell, subset=[c for c in buy_sell_cols if c in dataframe.columns])\
                             .applymap(color_kd, subset=[c for c in kd_cols if c in dataframe.columns])\
                             .applymap(color_parentheses, subset=[c for c in parentheses_cols if c in dataframe.columns])

        def clean_num(val):
            try:
                clean_str = str(val).replace('🔺', '').replace('🔻', '').replace('🟢', '').replace(',', '').strip()
                return float(clean_str)
            except:
                return None

        mask1 = []
        mask2 = []
        for _, row in df.iterrows():
            vals_dict = {}
            for c in ['外資_買賣超', '投信_買賣超', '自營商_買賣超', '三大法人_買賣超', '主力買賣超(張)']:
                if c in df.columns: vals_dict[c] = clean_num(row[c])
                else: vals_dict[c] = None

            if any(v is None for v in vals_dict.values()):
                mask1.append(False)
                mask2.append(False)
                continue

            f_val, t_val, d_val, all_val, m_val = vals_dict['外資_買賣超'], vals_dict['投信_買賣超'], vals_dict['自營商_買賣超'], vals_dict['三大法人_買賣超'], vals_dict['主力買賣超(張)']

            is_sheet1 = False
            if f_val > 0 and t_val > 0 and d_val > 0 and all_val > 0 and m_val > 0: is_sheet1 = True
            elif t_val == 0 and f_val > 0 and d_val > 0 and all_val > 0 and m_val > 0: is_sheet1 = True
            elif d_val == 0 and f_val > 0 and t_val > 0 and all_val > 0 and m_val > 0: is_sheet1 = True
            elif m_val == 0 and f_val > 0 and t_val > 0 and d_val > 0 and all_val > 0: is_sheet1 = True
            elif f_val == 0 and t_val > 0 and d_val > 0 and all_val > 0 and m_val > 0: is_sheet1 = True
            elif all_val == 0 and f_val > 0 and t_val > 0 and d_val > 0 and m_val > 0: is_sheet1 = True

            is_sheet2 = False
            if f_val < 0 and t_val < 0 and d_val < 0 and all_val < 0 and m_val < 0: is_sheet2 = True
            elif t_val == 0 and f_val < 0 and d_val < 0 and all_val < 0 and m_val < 0: is_sheet2 = True
            elif d_val == 0 and f_val < 0 and t_val < 0 and all_val < 0 and m_val < 0: is_sheet2 = True
            elif f_val == 0 and t_val < 0 and d_val < 0 and all_val < 0 and m_val < 0: is_sheet2 = True
            elif m_val == 0 and t_val < 0 and d_val < 0 and all_val < 0 and f_val < 0: is_sheet2 = True

            mask1.append(is_sheet1)
            mask2.append(is_sheet2)

        df_sheet1 = df[mask1]
        df_sheet2 = df[mask2]
        mask3 = [not (m1 or m2) for m1, m2 in zip(mask1, mask2)]
        df_sheet3 = df[mask3]

        def sort_by_vol_ratio(dataframe):
            if '買賣超佔成交量' not in dataframe.columns or dataframe.empty:
                return dataframe
            def parse_ratio(val):
                try: return float(str(val).replace('%', '').replace(',', '').strip())
                except: return -float('inf')
            df_sorted = dataframe.copy()
            df_sorted['_sort_key'] = df_sorted['買賣超佔成交量'].apply(parse_ratio)
            df_sorted = df_sorted.sort_values(by='_sort_key', ascending=False).drop(columns=['_sort_key'])
            return df_sorted

        df = sort_by_vol_ratio(df)
        df_sheet1 = sort_by_vol_ratio(df_sheet1)
        df_sheet2 = sort_by_vol_ratio(df_sheet2)
        df_sheet3 = sort_by_vol_ratio(df_sheet3)

        output_file = os.path.join(folder_name, f'stock_data_爬蟲日期_{today_date}_產出時間_{datetime_str}.xlsx')

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            apply_style(df).to_excel(writer, index=False, sheet_name='StockData')
            apply_style(df_sheet1).to_excel(writer, index=False, sheet_name='New_Sheet1')
            apply_style(df_sheet2).to_excel(writer, index=False, sheet_name='New_Sheet2')
            apply_style(df_sheet3).to_excel(writer, index=False, sheet_name='New_Sheet3')

            for sheet_name in ['StockData', 'New_Sheet1', 'New_Sheet2', 'New_Sheet3']:
                if sheet_name in writer.sheets:
                    ws = writer.sheets[sheet_name]
                    for cell in ws[1]:
                        cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')
                    for col in ['G', 'K', 'O', 'S']:
                        if col in ws.column_dimensions: ws.column_dimensions[col].width = 18
                    if 'B' in ws.column_dimensions: ws.column_dimensions['B'].width = 15

    # === 爬蟲主迴圈 ===
    try:
        total = len(stock_items)
        for idx, full_item in enumerate(stock_items):
            print(f"[{idx+1}/{total}] 正在爬取: {full_item}")

            stock_code = full_item.split(" ")[0]
            full_name = get_stock_name_from_web(stock_code)
            stock_info = get_stock_data(stock_code)
            stock_info['股票代碼'] = full_name
            stock_info['日期'] = today_date

            tech_url = f"https://tw.stock.yahoo.com/quote/{stock_code}.TW/technical-analysis"
            driver.get(tech_url)
            time.sleep(1.5)

            # 擷取 KD 數值
            try:
                kd_option = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'KD')]")))
                driver.execute_script("arguments[0].click();", kd_option)
                time.sleep(1)
            except: pass

            k9_val, d9_val = "找不到", "找不到"
            try:
                page_text = driver.find_element(By.TAG_NAME, "body").text
                k_match = re.search(r'K9\s*(\d+\.\d+)', page_text)
                d_match = re.search(r'D9\s*(\d+\.\d+)', page_text)
                if k_match: k9_val = k_match.group(1)
                if d_match: d9_val = d_match.group(1)
            except: pass

            stock_info['K9數值'] = k9_val
            stock_info['D9數值'] = d9_val

            # 擷取 MACD
            try:
                macd_option = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'MACD')]")))
                driver.execute_script("arguments[0].click();", macd_option)
                time.sleep(2)
            except: pass

            macd_val = "找不到"
            try:
                page_text = driver.find_element(By.TAG_NAME, "body").text
                macd_match = re.search(r'MACD\s*([-+]?\d+\.\d+)', page_text)
                if macd_match: macd_val = macd_match.group(1)
            except: pass

            stock_info['MACD數值'] = macd_val

            all_results.append(stock_info)

            # 每爬完 10 檔強制儲存
            if (idx + 1) % 10 == 0:
                try:
                    save_to_excel(all_results)
                    print(f"  -> 已完成 {idx+1} 筆進度存檔...")
                except Exception as e:
                    print(f"  -> 批次存檔發生錯誤: {e}")

        # 最終完整存檔
        try:
            save_to_excel(all_results)
            print(f"\n✅ 爬蟲結束！所有資料已最終存檔。")
            print(f"📂 輸出路徑位於: {folder_name}")
        except Exception as e:
            print(f"最終存檔發生錯誤: {e}")

    finally:
        driver.quit()


# ==========================================
# 程式進入點 (Main)
# ==========================================
if __name__ == "__main__":
    list_file = os.path.join(BASE_DIR, "自訂爬蟲清單.txt")
    print("==================================================")
    print("  股票爬蟲自動執行程式啟動 v1")
    print("==================================================")

    if not os.path.exists(list_file):
        print(f"⚠️ 找不到清單檔案：'{list_file}'")
        print(f"請在程式同一目錄下建立 '自訂爬蟲清單.txt'，並填入要爬取的股票代碼 (每行一檔)。")
        # 修改：發生錯誤時呼叫 sys.exit(1) 讓 Jenkins 判定為 Failed
        sys.exit(1)
    else:
        with open(list_file, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()

        stock_list = []
        for l in lines:
            stock = l.strip()
            # 濾除前面可能自帶的序號 (例如 "1. 2330")
            stock = re.sub(r'^\d+\.\s*', '', stock)
            if stock and stock not in stock_list:
                stock_list.append(stock)

        if not stock_list:
            print("⚠️ 錯誤：'自訂爬蟲清單.txt' 內沒有找到任何有效的股票代碼。")
            # 修改：發生錯誤時呼叫 sys.exit(1) 讓 Jenkins 判定為 Failed
            sys.exit(1)
        else:
            print(f"✅ 成功載入 {len(stock_list)} 檔股票。準備開始自動爬取...\n")
            start_time = time.time()

            run_scraping_task(stock_list)

            elapsed_time = time.time() - start_time
            minutes, seconds = divmod(int(elapsed_time), 60)
            print("==================================================")
            print(f"🎉 任務總耗時: {minutes} 分 {seconds} 秒")
            # 修改：移除 input() 等待指令。程式自然執行結束，Jenkins 會收到 exit code 0，判定為 Success