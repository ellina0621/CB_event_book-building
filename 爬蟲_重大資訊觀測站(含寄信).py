import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import urllib3
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MOPSScraper:
    """
    抓 MOPS「重大訊息主旨全文檢索」(t51sb10_q1)
    實際資料透過 POST /mops/web/ajax_t51sb10 回傳 HTML（AJAX）。
    """

    def __init__(self):
        self.base = "https://mopsov.twse.com.tw"
        self.entry_url = self.base + "/mops/web/t51sb10_q1"
        self.ajax_url = self.base + "/mops/web/ajax_t51sb10"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Referer": self.entry_url,
        }

    # ----------------------------
    # 1) 解析「結果表格」
    # ----------------------------
    def _find_result_table(self, soup: BeautifulSoup):
        """
        找到包含「代號/簡稱/日期/序號/主旨」的表格
        """
        tables = soup.find_all("table")
        for tb in tables:
            th_text = tb.get_text(" ", strip=True)
            if ("代號" in th_text) and ("簡稱" in th_text) and ("主旨" in th_text):
                return tb
        return None

    def _extract_detail_link(self, row):
        """
        嘗試從「詳細資料」按鈕/連結中抓出可用的 URL（若抓不到就回傳空字串）
        """
        # 常見：<input type="button" value="詳細資料" onclick="...">
        btn = row.find("input", attrs={"value": re.compile(r"詳細資料")})
        if btn and btn.get("onclick"):
            onclick = btn["onclick"]

            # 盡量抓出 onclick 裡面的網址/路徑
            # 例：openWindow('t05st01','...') or location='...'
            m = re.search(r"(\/mops\/web\/[^\s'\";]+)", onclick)
            if m:
                return self.base + m.group(1)

            m2 = re.search(r"href\s*=\s*['\"]([^'\"]+)['\"]", onclick)
            if m2:
                url = m2.group(1)
                return url if url.startswith("http") else (self.base + url)

        # 或者直接是 <a> 詳細資料
        a = row.find("a", string=re.compile("詳細資料"))
        if a and a.get("href"):
            href = a["href"]
            return href if href.startswith("http") else (self.base + href)

        return ""

    def _parse_rows_from_html(self, html: str, market_tag: str):
        soup = BeautifulSoup(html, "html.parser")
        tb = self._find_result_table(soup)
        if not tb:
            return []

        rows = tb.find_all("tr")
        if len(rows) <= 1:
            return []

        # 嘗試用表頭定位欄位順序（更耐改版）
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        # 期待：代號/簡稱/日期/序號/主旨/（詳細資料）
        # 如果找不到就用固定位置 fallback
        def idx(name, default=None):
            try:
                return headers.index(name)
            except ValueError:
                return default

        i_code = idx("代號", 0)
        i_name = idx("簡稱", 1)
        i_date = idx("日期", 2)
        i_seq  = idx("序號", 3)
        i_subj = idx("主旨", 4)

        out = []
        for r in rows[1:]:
            cols = r.find_all("td")
            if not cols or len(cols) < 5:
                continue

            code = cols[i_code].get_text(strip=True) if i_code is not None and i_code < len(cols) else ""
            short = cols[i_name].get_text(strip=True) if i_name is not None and i_name < len(cols) else ""
            roc_date = cols[i_date].get_text(strip=True) if i_date is not None and i_date < len(cols) else ""
            seq = cols[i_seq].get_text(strip=True) if i_seq is not None and i_seq < len(cols) else ""
            subject = cols[i_subj].get_text(" ", strip=True) if i_subj is not None and i_subj < len(cols) else ""

            detail_link = self._extract_detail_link(r)

            out.append({
                "市場別": market_tag,     # 上市 / 上櫃
                "代號": code,
                "簡稱": short,
                "日期(ROC)": roc_date,
                "序號": seq,
                "主旨": subject,
                "詳細連結": detail_link,
            })
        return out

    # ----------------------------
    # 2) Pagination：從 autoForm 找「下一頁」參數
    # ----------------------------
    def _extract_autoform_payload(self, html: str):
        """
        MOPS 回傳內容常會帶 <form name="autoForm">... 用來翻頁
        我們抓裡面的 input hidden，拿來 POST 下一次。
        """
        soup = BeautifulSoup(html, "html.parser")
        f = soup.find("form", attrs={"name": "autoForm"})
        if not f:
            return None

        payload = {}
        for inp in f.find_all("input"):
            name = inp.get("name")
            if not name:
                continue
            payload[name] = inp.get("value", "")

        # autoForm 通常需要 run=Y 才會真的翻頁
        if "run" in payload:
            payload["run"] = "Y"
        else:
            payload["run"] = "Y"

        # 有些頁會給 action（相對路徑）
        action = f.get("action", "") or "/mops/web/ajax_t51sb10"
        if not action.startswith("http"):
            action = self.base + action

        return action, payload

    # ----------------------------
    # 3) 主抓取：上市(L) / 上櫃(O)
    # ----------------------------
    def fetch_mops(self,
                   keyword: str = "存儲專戶行庫",
                   roc_year: int = 114,
                   month1: int = 0,
                   begin_day: int = 1,
                   end_day: int = 31,
                   orderby: int = 1,
                   kind: str = "L",
                   code_industry: str = ""):
        """
        kind: L=上市, O=上櫃
        month1: 0=全年度
        """
        market_tag = "上市" if kind == "L" else ("上櫃" if kind == "O" else kind)

        s = requests.Session()
        s.headers.update(self.headers)

        # 先進入頁面拿 cookie
        s.get(self.entry_url, timeout=30, verify=False)

        # 依照你圖上的設定：用「市場別/產業別」那個 radio（r1=1）
        payload = {
            "step": "1",
            "firstin": "true",
            "id": "",
            "key": "",
            "TYPEK": "",
            "Stp": "4",
            "go": "false",

            "r1": "1",                 # 市場別/產業別
            "co_id": "",               # 不用公司代號
            "KIND": kind,              # L / O
            "CODE": code_industry,     # 產業別：全部=空字串

            "keyWord": keyword,
            "Condition2": "1",         # 且含
            "keyWord2": "公司債",

            "year": str(roc_year),
            "month1": str(month1),
            "begin_day": str(begin_day),
            "end_day": str(end_day),

            "Orderby": str(orderby),
        }

        all_rows = []
        seen_signatures = set()

        # 第一次查詢
        resp = s.post(self.ajax_url, data=payload, timeout=30, verify=False)
        resp.encoding = "utf-8"
        html = resp.text

        # 解析第一頁
        rows = self._parse_rows_from_html(html, market_tag=market_tag)
        all_rows.extend(rows)

        # 翻頁：透過 autoForm 連續抓
        while True:
            af = self._extract_autoform_payload(html)
            if not af:
                break

            next_url, next_payload = af

            # 防呆：避免無限迴圈（autoForm 參數不變）
            sig = tuple(sorted(next_payload.items()))
            if sig in seen_signatures:
                break
            seen_signatures.add(sig)

            time.sleep(0.8)
            r2 = s.post(next_url, data=next_payload, timeout=30, verify=False)
            r2.encoding = "utf-8"
            html = r2.text

            rows2 = self._parse_rows_from_html(html, market_tag=market_tag)
            if not rows2:
                break
            all_rows.extend(rows2)

        df = pd.DataFrame(all_rows).drop_duplicates()
        return df

    # ----------------------------
    # 4) 輸出 Excel
    # ----------------------------
    def save_to_excel(self, df_all: pd.DataFrame, df_L: pd.DataFrame, df_O: pd.DataFrame,
                      filename: str, save_dir: str):
        os.makedirs(save_dir, exist_ok=True)
        filepath = os.path.join(save_dir, filename)

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df_all.to_excel(writer, index=False, sheet_name="ALL")
            df_L.to_excel(writer, index=False, sheet_name="上市(L)")
            df_O.to_excel(writer, index=False, sheet_name="上櫃(O)")

        print(f"已儲存到 {filepath}")
        return filepath

    # ----------------------------
    # 5) 寄信（沿用你原本的寫法）
    # ----------------------------
    def send_email(self, file_path, recipient_email, sender_email, sender_password, df=None, keyword=""):
        try:
            if isinstance(recipient_email, str):
                recipients = [recipient_email]
            else:
                recipients = list(recipient_email)

            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = f"MOPS 公告搜尋結果 - {keyword} - {datetime.now().strftime('%Y/%m/%d')}"

            table_style = """
            <style>
                table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; font-size: 12px; }
                th { background-color: #4979A6; color: white; padding: 10px; text-align: left; border: 1px solid #ddd; }
                td { padding: 8px; border: 1px solid #ddd; }
                tr:nth-child(even) { background-color: #E8EDF4; }
                tr:hover { background-color: #ddd; }
                a { color: #0066cc; text-decoration: none; }
                a:hover { text-decoration: underline; }
            </style>
            """

            html_table = ""
            if df is not None and not df.empty:
                df_display = df.copy()
                if "詳細連結" in df_display.columns:
                    df_display["詳細連結"] = df_display["詳細連結"].apply(
                        lambda x: f'<a href="{x}" target="_blank">查看</a>' if x else ""
                    )
                html_table = df_display.to_html(index=False, border=1, escape=False)

            body_html = f"""
            <html>
            <head>{table_style}</head>
            <body>
                <h2>公司債專戶行庫相關公告（重大公告）</h2>
                <p>您好，</p>
                <p>以下為關鍵字「<strong>{keyword}</strong>」的搜尋結果，共 <strong>{len(df) if df is not None else 0}</strong> 筆。</p>
                <p>爬取時間：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                <hr>
                {html_table}
                <hr>
                <p style="color: #666; font-size: 11px;">
                    此郵件為系統自動發送，請勿直接回覆。<br>
                    完整資料請參考附件檔案。
                </p>
            </body>
            </html>
            """
            msg.attach(MIMEText(body_html, "html", "utf-8"))

            if file_path and os.path.exists(file_path):
                filename = os.path.basename(file_path)
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
                    msg.attach(part)

            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipients, msg.as_string())
            server.quit()

            print(f"郵件已成功寄送至: {', '.join(recipients)}")
            return True

        except Exception as e:
            print(f"寄送郵件時發生錯誤: {e}")
            import traceback
            traceback.print_exc()
            return False


# =========================
# 使用範例（你只要改密碼）
# =========================
if __name__ == "__main__":
    SEND_EMAIL = True

    RECIPIENT_EMAIL = [
        "ella51284226@gmail.com",
        "peterxu331@gmail.com",
        "1148288@taishinbank.com.tw"
    ]

    SENDER_EMAIL = "ella51284226@gmail.com"
    SENDER_PASSWORD = "bytz dsvb yybh hjsc"    # ← 換成 Gmail 應用程式密碼（不是登入密碼）

    save_directory = r"D:\我才不要走量化\可轉換公司債"
    keyword = "專戶行庫"

    scraper = MOPSScraper()

    print("開始抓 MOPS（上市 + 上櫃）")
    print("=" * 60)

    # 上市 (L)
    df_L = scraper.fetch_mops(
        keyword=keyword,
        roc_year=114,
        month1=0,
        begin_day=1,
        end_day=31,
        orderby=1,
        kind="L",
        code_industry=""
    )
    print(f"上市(L) 抓到 {len(df_L)} 筆")

    # 上櫃 (O)
    df_O = scraper.fetch_mops(
        keyword=keyword,
        roc_year=114,
        month1=0,
        begin_day=1,
        end_day=31,
        orderby=1,
        kind="O",
        code_industry=""
    )
    print(f"上櫃(O) 抓到 {len(df_O)} 筆")

    # 合併
    df_all = pd.concat([df_L, df_O], ignore_index=True).drop_duplicates()

    # 排序（由新到舊）：ROC 日期是字串，先保留原樣；你也可以再轉西元後排序
    # 這裡先以字串排序，通常 114/12/24 這種格式還算穩
    if "日期(ROC)" in df_all.columns:
        df_all = df_all.sort_values(["日期(ROC)", "市場別", "代號"], ascending=[False, True, True])

    # 存 Excel
    filename = f"MOPS_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    file_path = scraper.save_to_excel(df_all, df_L, df_O, filename, save_directory)

    # 寄信
    if SEND_EMAIL and file_path:
        print("\n正在寄送郵件...")
        scraper.send_email(
            file_path=file_path,
            recipient_email=RECIPIENT_EMAIL,
            sender_email=SENDER_EMAIL,
            sender_password=SENDER_PASSWORD,
            df=df_all,
            keyword=keyword
        )

    print("\n完成!")
