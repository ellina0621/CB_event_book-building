import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

def scrape_twsa_data(year=2025, report_type='inquiry'):
    """
    爬取台灣證券商公會的公告資料
    
    Parameters:
    year (int): 查詢年度 (西元年)
    report_type (str): 'inquiry'(詢圈公告) 或 'underwriting'(承銷公告)
    
    Returns:
    pandas.DataFrame: 包含所有公告資料的DataFrame
    """
    
    # 目標網址
    url = "https://web.twsa.org.tw/edoc2/default.aspx"
    
    # 設定 headers 模擬瀏覽器
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # 首先發送 GET 請求獲取頁面
        session = requests.Session()
        response = session.get(url, headers=headers)
        response.raise_for_status()
        
        # 解析 HTML 取得必要的隱藏欄位
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 取得 __VIEWSTATE 等隱藏欄位
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstate_generator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})
        
        # 查找所有 radio buttons 來確定正確的值
        radio_buttons = soup.find_all('input', {'type': 'radio', 'name': 'ctl00$cphMain$rblReportType'})
        print(f"找到 {len(radio_buttons)} 個選項")
        for i, rb in enumerate(radio_buttons):
            print(f"選項 {i}: value={rb.get('value')}, id={rb.get('id')}")
        
        # 設定表單資料
        # 從原始碼確認：
        # - 承銷公告: value="UnderwritingNotice"
        # - 詢圈公告: value="BookBuilding"
        type_mapping = {
            'underwriting': 'UnderwritingNotice',
            'inquiry': 'BookBuilding'
        }
        
        form_data = {
            'ctl00$cphMain$ddlYear': str(year),
            'ctl00$cphMain$rblReportType': type_mapping.get(report_type, 'BookBuilding')
        }
        
        # 加入隱藏欄位
        if viewstate:
            form_data['__VIEWSTATE'] = viewstate.get('value', '')
        if viewstate_generator:
            form_data['__VIEWSTATEGENERATOR'] = viewstate_generator.get('value', '')
        if event_validation:
            form_data['__EVENTVALIDATION'] = event_validation.get('value', '')
        
        print(f"\n正在爬取 {year} 年的{'詢圈公告' if report_type == 'inquiry' else '承銷公告'}...")
        
        # 發送 POST 請求
        response = session.post(url, data=form_data, headers=headers)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        # 解析回應的 HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 找到資料表格
        table = soup.find('table', {'id': 'ctl00_cphMain_gvResult'})
        
        if not table:
            print("未找到資料表格，可能需要調整 report_type 參數")
            return None
        
        # 取得表格標題以確認類型
        caption = table.find('caption')
        if caption:
            print(f"表格標題: {caption.text.strip()}")
        
        # 解析表格資料
        data = []
        rows = table.find_all('tr')[1:]  # 跳過標題列
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                # 根據表格結構解析欄位
                record = {
                    '序號': cols[0].text.strip(),
                    '發行公司': cols[1].text.strip(),
                    '主辦承銷商': cols[2].text.strip() if len(cols) > 2 else '',
                }
                
                # 詢圈公告有更多欄位
                if len(cols) >= 8:
                    record.update({
                        '發行性質': cols[3].text.strip() if len(cols) > 3 else '',
                        '承銷股數(千股)': cols[4].text.strip() if len(cols) > 4 else '',
                        '詢圈銷售股數(千股/張)': cols[5].text.strip() if len(cols) > 5 else '',
                        '圈購期間': cols[6].text.strip() if len(cols) > 6 else '',
                        '價格(元)': cols[7].text.strip() if len(cols) > 7 else ''
                    })
                elif len(cols) >= 4:
                    record['公告日期'] = cols[3].text.strip()
                
                data.append(record)
        
        # 轉換為 DataFrame
        df = pd.DataFrame(data)
        
        print(f"成功爬取 {len(df)} 筆資料")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"網路請求錯誤: {e}")
        return None
    except Exception as e:
        print(f"發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_excel(df, save_dir=r"D:\我才不要走量化\可轉換公司債", filename=None):
    """
    將 DataFrame 儲存為 Excel 檔案
    
    Parameters:
    df (pandas.DataFrame): 要儲存的資料
    save_dir (str): 儲存目錄路徑
    filename (str): 檔案名稱，預設為當天日期
    
    Returns:
    str: 完整檔案路徑
    """
    if df is None or df.empty:
        print("沒有資料可儲存")
        return None
    
    # 確保目錄存在
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"已建立目錄: {save_dir}")
    
    if filename is None:
        filename = f"詢圈公告_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
    # 完整檔案路徑
    full_path = os.path.join(save_dir, filename)
    
    try:
        # 儲存為 Excel
        df.to_excel(full_path, index=False, engine='openpyxl')
        print(f"資料已成功儲存至: {full_path}")
        return full_path
    except Exception as e:
        print(f"儲存 Excel 時發生錯誤: {e}")
        return None

def send_email(file_path, recipient_email, sender_email, sender_password, df=None):
    """
    寄送 Excel 檔案到指定信箱，並在郵件中顯示表格
    
    Parameters:
    file_path (str): Excel 檔案路徑
    recipient_email (str or list): 收件者 email，可以是單一信箱或信箱列表
    sender_email (str): 寄件者 email (Gmail)
    sender_password (str): Gmail 應用程式密碼
    df (pandas.DataFrame): 要顯示在郵件中的資料
    
    Returns:
    bool: 是否成功寄送
    """
    try:
        # 處理收件者列表
        if isinstance(recipient_email, str):
            recipients = [recipient_email]
        else:
            recipients = list(recipient_email)
        
        # 建立郵件
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)  # 多個收件者用逗號+空格分隔字串
        msg['Subject'] = f'證券商詢圈公告 - {datetime.now().strftime("%Y/%m/%d")}'
        
        # 設定表格樣式
        table_style = """
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
            }
            th {
                background-color: #000080;
                color: white;
                padding: 10px;
                text-align: left;
                border: 1px solid #ddd;
            }
            td {
                padding: 8px;
                border: 1px solid #ddd;
            }
            tr:nth-child(even) {
                background-color: #E8EDF4;
            }
            tr:hover {
                background-color: #ddd;
            }
        </style>
        """
        
        # 建立 HTML 表格
        html_table = ""
        if df is not None and not df.empty:
            # 轉換 DataFrame 為 HTML
            html_table = df.to_html(index=False, border=1, classes='dataframe', escape=False)
        
        # 郵件內容 (HTML格式)
        body_html = f"""
        <html>
        <head>{table_style}</head>
        <body>
            <h2>證券商詢圈公告 - {datetime.now().strftime("%Y/%m/%d")}</h2>
            <p>您好，</p>
            <p>以下為今日爬取的證券商詢圈公告資料，共 <strong>{len(df) if df is not None else 0}</strong> 筆。</p>
            <p>爬取時間：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            
            <hr>
            
            {html_table}
            
            <hr>
            <p style="color: #666; font-size: 11px;">
                此郵件為系統自動發送，請勿直接回覆。<br>
                完整資料請參考附件 Excel 檔案。
            </p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))
        
        # 附加檔案
        if file_path and os.path.exists(file_path):
            filename = os.path.basename(file_path)
            # 確保檔名是 UTF-8 編碼
            with open(file_path, 'rb') as attachment:
                part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                # 使用正確的檔名編碼
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename="{filename}"'
                )
                msg.attach(part)
        
        # 連接到 Gmail SMTP 伺服器
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
        # 發送郵件給所有收件者
        text = msg.as_string()
        server.sendmail(sender_email, recipients, text)
        server.quit()
        
        print(f"郵件已成功寄送至: {', '.join(recipients)}")
        return True
        
    except Exception as e:
        print(f"寄送郵件時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return False

# 主程式
if __name__ == "__main__":
    SEND_EMAIL = True 
    
    # 方法一：寄給單一收件者
    # RECIPIENT_EMAIL = "your_email@gmail.com"
    
    RECIPIENT_EMAIL = [
        "ella51284226@gmail.com",
        "peterxu331@gmail.com",
        "1148288@taishinbank.com.tw"
    ]
    
    SENDER_EMAIL = "ella51284226@gmail.com"  
    SENDER_PASSWORD = "bytz dsvb yybh hjsc" 
    # ========================================
    
    save_path = r"D:\我才不要走量化\可轉換公司債"
    
    df = scrape_twsa_data(year=2025, report_type='inquiry')
    
    if df is not None:
        print("\n前 5 筆資料預覽:")
        print(df.head())
        print(f"\n資料欄位: {list(df.columns)}")
        print(f"總共 {len(df)} 筆資料")
        
        file_path = save_to_excel(df, save_dir=save_path)
        

        if SEND_EMAIL and file_path:
            print("\n正在寄送郵件...")
            send_email(file_path, RECIPIENT_EMAIL, SENDER_EMAIL, SENDER_PASSWORD, df=df)
        
        # 如果要爬取承銷公告，使用：
        # df_underwriting = scrape_twsa_data(year=2025, report_type='underwriting')
        # save_to_excel(df_underwriting, save_dir=save_path, filename="承銷公告_20251229.xlsx")