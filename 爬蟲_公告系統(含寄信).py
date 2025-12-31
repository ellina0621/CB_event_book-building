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
import json

def scrape_twsa_data(year=2025, report_type='inquiry'):
    """
    爬取台灣證券商公會的公告資料
    
    Parameters:
    year (int): 查詢年度 (西元年)
    report_type (str): 'inquiry'(詢圈公告) 或 'underwriting'(承銷公告)
    
    Returns:
    pandas.DataFrame: 包含所有公告資料的DataFrame
    """
    
    url = "https://web.twsa.org.tw/edoc2/default.aspx"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        session = requests.Session()
        response = session.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstate_generator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})
        
        radio_buttons = soup.find_all('input', {'type': 'radio', 'name': 'ctl00$cphMain$rblReportType'})
        print(f"找到 {len(radio_buttons)} 個選項")
        for i, rb in enumerate(radio_buttons):
            print(f"選項 {i}: value={rb.get('value')}, id={rb.get('id')}")
        
        type_mapping = {
            'underwriting': 'UnderwritingNotice',
            'inquiry': 'BookBuilding'
        }
        
        form_data = {
            'ctl00$cphMain$ddlYear': str(year),
            'ctl00$cphMain$rblReportType': type_mapping.get(report_type, 'BookBuilding')
        }
        
        if viewstate:
            form_data['__VIEWSTATE'] = viewstate.get('value', '')
        if viewstate_generator:
            form_data['__VIEWSTATEGENERATOR'] = viewstate_generator.get('value', '')
        if event_validation:
            form_data['__EVENTVALIDATION'] = event_validation.get('value', '')
        
        print(f"\n正在爬取 {year} 年的{'詢圈公告' if report_type == 'inquiry' else '承銷公告'}...")
        
        response = session.post(url, data=form_data, headers=headers)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'ctl00_cphMain_gvResult'})
        
        if not table:
            print("未找到資料表格")
            return None
        
        caption = table.find('caption')
        if caption:
            print(f"表格標題: {caption.text.strip()}")
        
        data = []
        rows = table.find_all('tr')[1:]
        
        for row in rows:
            cols = row.find_all('td')
            if len(cols) >= 4:
                record = {
                    '序號': cols[0].text.strip(),
                    '發行公司': cols[1].text.strip(),
                    '主辦承銷商': cols[2].text.strip() if len(cols) > 2 else '',
                }
                
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
        
        df = pd.DataFrame(data)
        print(f"成功爬取 {len(df)} 筆資料")
        return df
        
    except Exception as e:
        print(f"發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_last_data(save_dir):
    """
    讀取上次儲存的資料
    
    Returns:
    tuple: (DataFrame, 檔案日期)
    """
    history_file = os.path.join(save_dir, 'last_data.json')
    
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                df = pd.DataFrame(data['records'])
                last_date = data['date']
                return df, last_date
        except Exception as e:
            print(f"讀取歷史資料失敗: {e}")
            return None, None
    return None, None

def save_current_data(df, save_dir):
    """儲存當前資料作為歷史記錄"""
    history_file = os.path.join(save_dir, 'last_data.json')
    
    try:
        data = {
            'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'records': df.to_dict('records')
        }
        with open(history_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"已儲存當前資料作為歷史記錄")
    except Exception as e:
        print(f"儲存歷史資料失敗: {e}")

def compare_data(current_df, last_df):
    """
    比對當前資料與上次資料的差異
    
    Returns:
    DataFrame: 新增的資料
    """
    if last_df is None or last_df.empty:
        return current_df, "首次執行"
    
    # 使用序號作為唯一識別
    last_ids = set(last_df['序號'].tolist())
    current_ids = set(current_df['序號'].tolist())
    
    # 找出新增的序號
    new_ids = current_ids - last_ids
    
    if new_ids:
        new_records = current_df[current_df['序號'].isin(new_ids)]
        return new_records, None
    else:
        return pd.DataFrame(), None

def save_to_excel(df, save_dir=r"D:\我才不要走量化\可轉換公司債", filename=None):
    """將 DataFrame 儲存為 Excel 檔案"""
    if df is None or df.empty:
        print("沒有資料可儲存")
        return None
    
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"已建立目錄: {save_dir}")
    
    if filename is None:
        filename = f"詢圈公告_{datetime.now().strftime('%Y%m%d')}.xlsx"
    
    full_path = os.path.join(save_dir, filename)
    
    try:
        df.to_excel(full_path, index=False, engine='openpyxl')
        print(f"資料已成功儲存至: {full_path}")
        return full_path
    except Exception as e:
        print(f"儲存 Excel 時發生錯誤: {e}")
        return None

def send_email(file_path, recipient_email, sender_email, sender_password, df=None, new_records=None, last_date=None):
    """
    寄送 Excel 檔案到指定信箱，並在郵件中顯示表格和差異
    
    Parameters:
    file_path (str): Excel 檔案路徑
    recipient_email (str or list): 收件者 email
    sender_email (str): 寄件者 email (Gmail)
    sender_password (str): Gmail 應用程式密碼
    df (pandas.DataFrame): 完整資料
    new_records (pandas.DataFrame): 新增的資料
    last_date (str): 上次爬取的日期
    """
    try:
        if isinstance(recipient_email, str):
            recipients = [recipient_email]
        else:
            recipients = list(recipient_email)
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = f'證券商詢圈公告 - {datetime.now().strftime("%Y/%m/%d")}'
        
        table_style = """
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
                font-size: 12px;
                margin-bottom: 20px;
            }
            th {
                background-color: #325385;
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
            .new-record {
                background-color: #ffffcc !important;
                font-weight: bold;
            }
            .summary-box {
             padding: 10px 0;
             margin: 10px 0;
            }
            .highlight {
                color: #d9534f;
                font-weight: bold;
                font-size: 16px;
            }
        </style>
        """
        
        # 建立差異摘要
        summary_html = ""
        if last_date:
            if new_records is not None and not new_records.empty:
                summary_html = f"""
                <div class="summary-box">
                    <h3>與上次比對結果</h3>
                    <p>上次爬取時間：{last_date}</p>
                    <p class="highlight">新增 {len(new_records)} 筆公告</p>
                    <ul>
                """
                for _, row in new_records.iterrows():
                    summary_html += f"<li><strong>{row['序號']}</strong> - {row['發行公司']} ({row['圈購期間']})</li>"
                summary_html += """
                    </ul>
                </div>
                """
            else:
                summary_html = f"""
                <div class="summary-box">
                    <h3>與上次比對結果</h3>
                    <p>上次爬取時間：{last_date}</p>
                    <p>無新增公告</p>
                </div>
                """
        else:
            summary_html = """
            <div class="summary-box">
                <h3>data</h3>
                <p>這是首次執行，所有資料都是新的</p>
            </div>
            """
        
        # 建立完整資料表格
        full_table = ""
        if df is not None and not df.empty:
            full_table = df.to_html(index=False, border=1, escape=False)
        
        # 建立新增資料表格
        new_table = ""
        if new_records is not None and not new_records.empty:
            new_table = f"""
            <h3>新增的公告明細</h3>
            {new_records.to_html(index=False, border=1, escape=False)}
            <hr>
            """
        
        body_html = f"""
        <html>
        <head>{table_style}</head>
        <body>
            <h2>證券商詢圈公告 - {datetime.now().strftime("%Y/%m/%d")}</h2>
            <p>您好，</p>
            <p>本次共爬取 <strong>{len(df) if df is not None else 0}</strong> 筆詢圈公告資料。</p>
            <p>爬取時間：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            
            {summary_html}
            
            {new_table}
            
            <h3>完整資料列表</h3>
            {full_table}
            
            <hr>
            <p style="color: #666; font-size: 11px;">
                此郵件為系統自動發送，請勿直接回覆。<br>
                完整資料請參考附件 Excel 檔案。
            </p>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(body_html, 'html', 'utf-8'))
        
        if file_path and os.path.exists(file_path):
            filename = os.path.basename(file_path)
            with open(file_path, 'rb') as attachment:
                part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
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
    
    RECIPIENT_EMAIL = [
        "ella51284226@gmail.com",
        "1148288@taishinbank.com.tw"
    ]
    
    SENDER_EMAIL = "ella51284226@gmail.com"  
    SENDER_PASSWORD = "xxxxxxx" 
    
    save_path = r"D:\我才不要走量化\可轉換公司債"
    
    # 讀取上次的資料
    print("檢查歷史資料...")
    last_df, last_date = load_last_data(save_path)
    
    if last_df is not None:
        print(f"找到上次資料: {last_date}, 共 {len(last_df)} 筆")
    else:
        print("未找到歷史資料（首次執行）")
    
    # 爬取當前資料
    df = scrape_twsa_data(year=2025, report_type='inquiry')
    
    if df is not None:
        print(f"\n總共 {len(df)} 筆資料")
        
        # 比對差異
        print("\n比對資料差異...")
        new_records, first_run = compare_data(df, last_df)
        
        if first_run:
            print("首次執行，所有資料都是新的")
        elif not new_records.empty:
            print(f"發現 {len(new_records)} 筆新增公告:")
            for _, row in new_records.iterrows():
                print(f"  - {row['序號']}: {row['發行公司']}")
        else:
            print("無新增資料")
        
        # 儲存 Excel
        file_path = save_to_excel(df, save_dir=save_path)
        
        # 儲存當前資料作為歷史記錄
        save_current_data(df, save_path)
        
        # 寄送郵件
        if SEND_EMAIL and file_path:
            print("\n正在寄送郵件...")
            send_email(
                file_path, 
                RECIPIENT_EMAIL, 
                SENDER_EMAIL, 
                SENDER_PASSWORD, 
                df=df,
                new_records=new_records if not first_run else df,
                last_date=last_date
            )
        
        print("\n完成!")


