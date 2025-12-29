import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import seaborn as sns
from datetime import datetime, timedelta
from bisect import bisect_left
import pandas as pd
import sqlite3
import shioaji as sj
import time
import os
import sys
import statsmodels.api as sm
import warnings
warnings.filterwarnings('ignore')

###############################################################################
# 參數設定
###############################################################################

BASE_PATH = r"D:\我才不要走量化\可轉換公司債"
OUTPUT_DIR = os.path.join(BASE_PATH, "event_car_plots")

EVENT_DATE_TYPE = "start_date"  

EVENT_WINDOW_START = -7 
EVENT_WINDOW_END = 7    

ENTRY_DAY = -1   
EXIT_DAY = 6   
INITIAL_CAPITAL = 1_000_000  
TRANSACTION_COST_BPS = 0.0103  #手續費 58 bps + 滑價成本 45 bps

MARKET_DATA_YEARS = [2021, 2022, 2023, 2024, 2025]
BETA_YEARS = [2021, 2023, 2025]

ANNOUNCE_FILE = os.path.join(BASE_PATH, "可轉債公告.xlsx")  # 你的公告檔
ENTRY_DAY_A = -1
ENTRY_DAY_B_FROM_ANNOUNCE = 1


###############################################################################
# 資料載入
# 之前本來用扣掉台積電的加權，後來改成用Y9997，程式的寫法懶得改XD
###############################################################################

def load_book_building_data():

    print("=" * 60)
    print("Step 1: 載入資料")
    print("=" * 60)

    file_path = os.path.join(BASE_PATH, "詢圈.xlsx")
    df = pd.read_excel(file_path)

    # 拆解日期範圍
    s = df["date"].astype(str)
    pattern = r"(\d{4}/\d{1,2}/\d{1,2}).*?(\d{4}/\d{1,2}/\d{1,2})"
    dates = s.str.extract(pattern)

    df["start_date"] = pd.to_datetime(dates[0], format="%Y/%m/%d", errors="coerce")
    df["end_date"]   = pd.to_datetime(dates[1], format="%Y/%m/%d", errors="coerce")
    df["code"] = df["code"].astype(str).str.strip()

    if "number" not in df.columns:
        raise ValueError("ALL.xlsx 找不到 'number' 欄位，無法用流水號累積事件。")

    df["number"] = df["number"].astype(str).str.strip()

    df["event_date"] = df[EVENT_DATE_TYPE]

    df["event_id"] = (
        df["code"].astype(str) + "_" +
        df["event_date"].dt.strftime("%Y%m%d") + "_" +
        df["number"].astype(str)
    )

    print(f"✅ 載入 {len(df)} 筆資料")
    print(f"✅ unique event_id: {df['event_id'].nunique()}")

    return df


def load_market_data():
    print("\n" + "=" * 60)
    print("Step 2: 載入市場資料")
    print("=" * 60)
    
    dfs = {}
    for year in MARKET_DATA_YEARS:
        file_path = os.path.join(BASE_PATH, f"{year}_marketdata.csv")
        try:
            df = pd.read_csv(file_path, sep="\t", encoding="utf-16", 
                           dtype={'證券代碼': str, '年月日': str})
            dfs[year] = df
            print(f"✅ {year}_marketdata.csv 讀取成功，{len(df)} 筆")
        except Exception as e:
            print(f"無法讀取 {file_path}: {e}")
    
    if not dfs:
        raise ValueError("錯誤：沒有讀取到任何市場資料")
    
    df_all = pd.concat(dfs.values(), ignore_index=True)
    
    col_map = {
        "證券代碼": "stock_code_raw",
        "年月日": "date",
        "證期會代碼": "code",
        "公司中文簡稱": "name",
        "TSE產業名": "industry",
        "開盤價(元)": "open",
        "最高價(元)": "high",
        "最低價(元)": "low",
        "收盤價(元)": "close",
        "報酬率％": "return_pct",
        "週轉率％": "turnover_pct",
        "成交值(千元)": "value_1000",
        "市值(百萬元)": "mv",
        "股價淨值比-TSE": "pb_ratio",
        "CAPM_Beta 一年": "beta_1yr",
        "當日沖銷交易總成交股數占市場比重%": "daytrade_pct",
        "外資總投資比率%-TSE": "foreign_pct"
    }
    
    market_data = df_all.rename(columns=col_map)
    market_data["code"] = market_data["code"].astype(str).str.split().str[0].str.strip()
    market_data["date"] = market_data["date"].astype(str).str.replace(r'[\/\-]', '', regex=True).str.strip()
    
    # 排除金融股和KY股
    mask_financial = market_data['code'].str.startswith('28')
    mask_ky = market_data['name'].astype(str).str.contains('KY', case=False, na=False)
    market_data = market_data[~(mask_financial | mask_ky)].copy()
    
    market_data["date"] = pd.to_datetime(market_data["date"], format="%Y%m%d", errors="coerce")
    
    print(f"✅ 市場資料整理完成，共 {len(market_data)} 筆")
    return market_data


def load_capm_factors():
    print("\n" + "=" * 60)
    print("Step 3: 載入 CAPM 因子")
    print("=" * 60)

    rm_rf_path = os.path.join(BASE_PATH, "rm_and_rf.csv")
    rf_df = pd.read_csv(rm_rf_path, encoding="utf-16", sep="\t", dtype={"年月日": str})

    rf_df = rf_df.rename(columns={
        "年月日": "date",
        "無風險利率": "rf_pct"
    })
    rf_df["date"] = rf_df["date"].astype(str).str.replace(r"[\/\-]", "", regex=True).str.strip()
    rf_df["date"] = pd.to_datetime(rf_df["date"], format="%Y%m%d", errors="coerce")
    rf_df = rf_df[["date", "rf_pct"]].dropna(subset=["date"])


    no_2330_path = os.path.join(BASE_PATH, "y9997.xlsx")
    rm_df = pd.read_excel(no_2330_path)

    rm_df = rm_df.rename(columns={"return": "rm_pct"})
    rm_df["date"] = pd.to_datetime(rm_df["date"], errors="coerce")
    rm_df = rm_df[["date", "rm_pct"]].dropna(subset=["date"])

    # --------------------------
    # Rm - Rf
    # --------------------------
    capm_factors = rm_df.merge(rf_df, on="date", how="left")
    capm_factors["rm_rf_pct"] = capm_factors["rm_pct"] - capm_factors["rf_pct"]

    # --------------------------
    # Beta
    # --------------------------
    beta_list = []
    for year in BETA_YEARS:
        beta_path = os.path.join(BASE_PATH, f"{year}beta.csv")
        try:
            beta_df = pd.read_csv(beta_path, encoding="utf-16", sep="\t", dtype=str)

            if "證期會代碼" in beta_df.columns:
                beta_df = beta_df.rename(columns={"證期會代碼": "code"})
            elif "證券代碼" in beta_df.columns:
                beta_df = beta_df.rename(columns={"證券代碼": "code"})
            else:
                raise ValueError("beta 檔找不到「證期會代碼」或「證券代碼」")

            beta_df = beta_df.rename(columns={
                "年月日": "date",
                "CAPM_Beta_三月": "beta_3m"
            })

            beta_df["code"] = beta_df["code"].astype(str).str.split().str[0].str.strip()
            beta_df["date"] = beta_df["date"].astype(str).str.replace(r"[\/\-]", "", regex=True).str.strip()
            beta_df["date"] = pd.to_datetime(beta_df["date"], format="%Y%m%d", errors="coerce")

            beta_list.append(beta_df[["code", "date", "beta_3m"]])
            print(f"✅ {year}beta.csv 讀取成功")

        except Exception as e:
            print(f"⚠️  無法讀取 {beta_path}: {e}")

    beta_all = pd.concat(beta_list, ignore_index=True) if beta_list else pd.DataFrame(columns=["code", "date", "beta_3m"])

    print("CAPM 因子資料載入完成")
    return capm_factors, beta_all


###############################################################################
# 計算 CAPM 與異常報酬 
# 資料內的return皆用%的
###############################################################################

def calculate_abnormal_returns(market_data, capm_factors, beta_all):

    capm_df = market_data.merge(capm_factors, on="date", how="left")

    capm_df = capm_df.merge(beta_all, on=["code", "date"], how="left")

    for col in ["rf_pct", "rm_pct", "rm_rf_pct", "beta_3m", "return_pct"]:
        if col in capm_df.columns:
            capm_df[col] = (
                capm_df[col]
                .astype(str)
                .str.replace(",", "", regex=False)   
                .str.replace("%", "", regex=False)   
            )
            capm_df[col] = pd.to_numeric(capm_df[col], errors="coerce")


    capm_df["capm"] = capm_df["rf_pct"] + capm_df["beta_3m"] * capm_df["rm_rf_pct"]
    capm_df["ar"] = capm_df["return_pct"] - capm_df["capm"]


    print("finished calculating abnormal returns.")
    return capm_df

###############################################################################
# research
###############################################################################

def get_trading_dates(capm_df):

    trading_dates = sorted(capm_df['date'].unique())
    return trading_dates


def find_relative_trading_date(event_date, offset, trading_dates):
    try:
        idx = bisect_left(trading_dates, event_date)
        
        if idx < len(trading_dates) and trading_dates[idx] == event_date:
            target_idx = idx + offset
        else:

            target_idx = idx - 1 + offset #確定是以交易日做為date

        if 0 <= target_idx < len(trading_dates):
            return trading_dates[target_idx]
        else:
            return None
    except:
        return None


def create_event_panel(df_book_building, capm_df):

    trading_dates = get_trading_dates(capm_df)
    print(f"   交易日範圍: {trading_dates[0].date()} 至 {trading_dates[-1].date()}")
    print(f"   共 {len(trading_dates)} 個交易日")

    need_cols = ["event_id", "code", "start_date", "end_date", "number"]
    miss = [c for c in need_cols if c not in df_book_building.columns]
    if miss:
        raise ValueError(f"缺少欄位: {miss}")

    events = df_book_building[["event_id", "code", "start_date", "end_date", "number"]].copy() #用number作為之後事件累積的變數
    events["event_type"] = "bidding"
    events["event_date"] = df_book_building[EVENT_DATE_TYPE] 

    event_window = range(EVENT_WINDOW_START, EVENT_WINDOW_END + 1)
    panel_list = []

    for _, event_row in events.iterrows():
        event_id   = event_row["event_id"]
        code       = event_row["code"]
        event_date = event_row["event_date"]
        event_type = event_row["event_type"]
        start_date = event_row["start_date"]
        end_date   = event_row["end_date"]
        number     = event_row["number"]

        for offset in event_window:
            actual_date = find_relative_trading_date(event_date, offset, trading_dates)
            if actual_date is not None:
                panel_list.append({
                    "event_id": event_id,    
                    "number": number,      
                    "code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "event_date": event_date,
                    "event_type": event_type,
                    "event_day": offset,
                    "calendar_date": actual_date
                })

    event_panel = pd.DataFrame(panel_list)

    event_panel = event_panel.merge(
        capm_df[["code", "date", "ar"]],
        left_on=["code", "calendar_date"],
        right_on=["code", "date"],
        how="left"
    )

    missing_ar = event_panel["ar"].isna().sum()
    if missing_ar > 0:
        print(f" 仍有 {missing_ar} 筆缺少 AR 資料") #後來去看缺資料是因為當時股票尚未上市，例如3717在2023還沒上市，2025/08/15才上市
    event_panel["ar"] = event_panel["ar"].fillna(0)

    event_panel = event_panel.sort_values(["event_id", "event_day"])
    event_panel["car"] = event_panel.groupby(["event_id"])["ar"].cumsum()
    event_panel["year"] = event_panel["calendar_date"].dt.year

    print(f"finish 共 {len(event_panel)} 筆觀察值")
    print(f"   unique events (event_id): {event_panel['event_id'].nunique()}")
    print(f"   事件窗口: t={EVENT_WINDOW_START} 到 t={EVENT_WINDOW_END} (交易日)")
    return event_panel

###############################################################################
# 畫圖ㄌ
###############################################################################

def plot_average_car(event_panel):

    avg_car_all = event_panel.groupby("event_day")["car"].mean().reset_index()
    
    plt.figure(figsize=(10, 6))
    plt.plot(avg_car_all["event_day"], avg_car_all["car"], 
             marker="o", linewidth=2, markersize=6)
    plt.axvline(0, linestyle="--", color="red", alpha=0.7, label="Event Date")
    plt.axhline(0, linestyle=":", color="gray", alpha=0.5)
    plt.title(f"Average CAR around {EVENT_DATE_TYPE} (All Events)", 
              fontsize=14, weight="bold")
    plt.xlabel("Event Day (t)", fontsize=12)
    plt.ylabel("CAR (%)", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()
    
    # 分年度
    years = sorted(event_panel["year"].unique())
    for year in years:
        df_year = event_panel[event_panel["year"] == year]
        avg_car_year = df_year.groupby("event_day")["car"].mean().reset_index()
        
        plt.figure(figsize=(10, 6))
        plt.plot(avg_car_year["event_day"], avg_car_year["car"], 
                marker="o", linewidth=2, markersize=6)
        plt.axvline(0, linestyle="--", color="red", alpha=0.7)
        plt.axhline(0, linestyle=":", color="gray", alpha=0.5)
        plt.title(f"Average CAR — Year {year}", fontsize=14, weight="bold")
        plt.xlabel("Event Day (t)", fontsize=12)
        plt.ylabel("CAR (%)", fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    # 平均異常報酬 
    avg_ar = event_panel.groupby("event_day")["ar"].mean().reset_index()
    
    plt.figure(figsize=(10, 6))
    plt.plot(avg_ar["event_day"], avg_ar["ar"], 
             marker="o", linewidth=2, markersize=6, color="green")
    plt.axvline(0, linestyle="--", color="red", alpha=0.7, label="Event Date")
    plt.axhline(0, linestyle=":", color="gray", alpha=0.5)
    plt.title(f"Average Abnormal Return (AAR) around {EVENT_DATE_TYPE}", 
              fontsize=14, weight="bold")
    plt.xlabel("Event Day (t)", fontsize=12)
    plt.ylabel("AAR (%)", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()
    
    print("畫完ㄌ~")

###############################################################################
# 簡單回測，以事件(number)畫權益曲線
###############################################################################
def backtest_strategy(event_panel, capm_df):

    print("\n" + "=" * 60)
    print(f"交易回測 (進場=t{ENTRY_DAY}, 出場=t{EXIT_DAY})")
    print("=" * 60)

    events_price = event_panel.merge(
        capm_df[["code", "date", "close", "open"]],
        left_on=["code", "calendar_date"],
        right_on=["code", "date"],
        how="left"
    )

    # 進場價格：事件日前一天收盤價
    entry_df = events_price[events_price["event_day"] == ENTRY_DAY].copy()
    entry_df = entry_df.rename(columns={"close": "entry_price"})

    # 出場價格：事件日後六天收盤價
    exit_df = events_price[events_price["event_day"] == EXIT_DAY].copy()
    exit_df = exit_df.rename(columns={"close": "exit_price"})

    print("\n Check:")
    print(f"   總事件數(unique event_id): {event_panel['event_id'].nunique()}")
    print(f"   有進場價格: {entry_df['entry_price'].notna().sum()} / {len(entry_df)}")
    print(f"   有出場價格: {exit_df['exit_price'].notna().sum()} / {len(exit_df)}")

    trades = pd.merge(
        entry_df[["event_id", "number", "code", "event_date", "event_type", "entry_price", "calendar_date"]],
        exit_df[["event_id", "exit_price"]],
        on=["event_id"],
        how="outer",
        suffixes=("_entry", "_exit")
    )

    missing_entry = trades[trades["entry_price"].isna()]
    missing_exit = trades[trades["exit_price"].isna()]

    if len(missing_entry) > 0:
        print(f"\n缺少進場價格的事件: {len(missing_entry)} 筆")
        print("前5筆:")
        print(missing_entry[["event_id","code", "event_date", "calendar_date"]].head())

    if len(missing_exit) > 0:
        print(f"\n缺少出場價格的事件: {len(missing_exit)} 筆")
        print("前5筆:")
        print(missing_exit[["event_id","code", "event_date"]].head())

    trades = trades.dropna(subset=["entry_price", "exit_price"]).copy()

    trades["gross_r"] = (trades["exit_price"] - trades["entry_price"]) / trades["entry_price"]
    trades["net_r"] = trades["gross_r"] - TRANSACTION_COST_BPS
    trades["profit"] = trades["net_r"] * INITIAL_CAPITAL
    trades["cumulative_profit"] = trades["profit"].cumsum()

    cum_curve = trades["cumulative_profit"]
    running_max = cum_curve.cummax()
    drawdown = cum_curve - running_max
    mdd = drawdown.min()

    trades["is_win"] = trades["net_r"] > 0
    win_count = (trades["net_r"] > 0).sum()
    total_trades = len(trades)
    win_rate = win_count / total_trades if total_trades > 0 else np.nan

    avg_win = trades.loc[trades["is_win"], "net_r"].mean()
    avg_loss = trades.loc[~trades["is_win"], "net_r"].mean()
    payoff_ratio = (avg_win / abs(avg_loss)) if (pd.notna(avg_win) and pd.notna(avg_loss) and avg_loss != 0) else np.nan

    gross_profit = trades.loc[trades["net_r"] > 0, "profit"].sum()
    gross_loss = -trades.loc[trades["net_r"] < 0, "profit"].sum()
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.nan

    mean_r = trades["net_r"].mean()
    std_r = trades["net_r"].std(ddof=1)
    sharpe_per_trade = (mean_r / std_r) if std_r and std_r > 0 else np.nan

    if len(trades) >= 2:
        n_years = (trades["event_date"].max() - trades["event_date"].min()).days / 365.25
        trades_per_year = (len(trades) / n_years) if n_years > 0 else np.nan
    else:
        trades_per_year = np.nan

    sharpe_annual = sharpe_per_trade * np.sqrt(trades_per_year) if pd.notna(trades_per_year) else np.nan

    n = len(trades)
    se = std_r / np.sqrt(n) if std_r and std_r > 0 else np.nan
    t_stat = mean_r / se if pd.notna(se) and se > 0 else np.nan

    print("Results:")
    print("=" * 60)
    print(f"總交易次數: {len(trades)}")
    print(f"Win Rate: {win_rate * 100:.2f}%")
    print(f"Total profit: NT$ {trades['profit'].sum():,.0f}")
    print(f"平均單筆報酬率: {mean_r * 100:.4f}%")
    print(f"報酬率標準差 : {std_r * 100:.4f}%")
    print(f"最大回撤 (MDD): NT$ {mdd:,.0f}")
    print("=" * 60)

    plot_equity_curve(trades)
    return trades


def plot_equity_curve(trades):
    sns.set_style("whitegrid")
    
    equity_curve = trades["cumulative_profit"] + INITIAL_CAPITAL
    
    plt.figure(figsize=(12, 6))
    plt.plot(equity_curve, color="#1f77b4", linewidth=2.5, label="Equity Curve")
    plt.axhline(INITIAL_CAPITAL, linestyle="--", color="gray", 
                alpha=0.5, label="Initial Capital")
    plt.title("Equity Curve", fontsize=16, weight="bold")
    plt.xlabel("Trade Number", fontsize=12)
    plt.ylabel("Portfolio Value (NT$)", fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=11)
    plt.tight_layout()
    plt.show()

###############################################################################
# 進場時間點比較
###############################################################################
def prepare_events_with_ann(df_book_building):

    df = df_book_building.copy()

    # 你的 load_book_building_data() 其實已經拆過 start/end_date 了
    # 這裡防呆：若沒有就再拆一次
    if "start_date" not in df.columns or "end_date" not in df.columns:
        s = df["date"].astype(str)
        pattern = r"(\d{4}/\d{1,2}/\d{1,2}).*?(\d{4}/\d{1,2}/\d{1,2})"
        dates = s.str.extract(pattern)
        df["start_date"] = pd.to_datetime(dates[0], format="%Y/%m/%d", errors="coerce")
        df["end_date"]   = pd.to_datetime(dates[1], format="%Y/%m/%d", errors="coerce")

    # ann_date
    if "ann_date" not in df.columns:
        raise ValueError("詢圈檔沒有 ann_date 欄位，請確認欄位名稱是否正確。")

    df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce")
    df["code"] = df["code"].astype(str).str.strip()
    df["number"] = df["number"].astype(str).str.strip()

    # 事件日（你原本的事件日 = start_date）
    df["event_date"] = pd.to_datetime(df["start_date"], errors="coerce")

    # event_id（如果你前面已經建過也沒關係，這裡統一覆蓋）
    df["event_id"] = (
        df["code"] + "_" +
        df["event_date"].dt.strftime("%Y%m%d") + "_" +
        df["number"]
    )

    # 只保留有公告日
    df_has_ann = df[df["ann_date"].notna()].copy()
    print(f" 有 ann_date 的事件數：{len(df_has_ann)} / {len(df)}")

    return df_has_ann

def build_trades_ABC(df_has_ann, capm_df, limit_up_th=0.085):
    """
    產生 A, B, C 三種策略的交易訊號
    A: start_date t=-1 Close
    B: ann_date t=+1 Open
    C: start_date t=0 Open (新增)
    """

    trading_dates = get_trading_dates(capm_df)

    # 準備價格資料 (包含 Open)
    px = capm_df[["code", "date", "open", "close"]].copy()
    px["code"] = px["code"].astype(str).str.strip()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")

    for c in ["open", "close"]:
        px[c] = pd.to_numeric(px[c], errors="coerce")

    # 快速查價
    open_map  = {(c, d): p for c, d, p in zip(px["code"], px["date"], px["open"])}
    close_map = {(c, d): p for c, d, p in zip(px["code"], px["date"], px["close"])}

    def get_open(code, d):
        if d is None or pd.isna(d): return np.nan
        return open_map.get((code, d), np.nan)

    def get_close(code, d):
        if d is None or pd.isna(d): return np.nan
        return close_map.get((code, d), np.nan)

    rowsA, rowsB, rowsC = [], [], []
    skipped = []

    for _, r in df_has_ann.iterrows():
        code = str(r["code"]).strip()
        event_id = r["event_id"]
        number = r["number"]
        start_date = pd.to_datetime(r["start_date"], errors="coerce")
        ann_date   = pd.to_datetime(r["ann_date"], errors="coerce")

        # === 共同出場：start_date 的 +6 收盤 ===
        exit_cal = find_relative_trading_date(start_date, 6, trading_dates)
        exit_px  = get_close(code, exit_cal)

        # -------------------------------------------------------
        # 策略 A：start_date 的 -1 收盤進場
        # -------------------------------------------------------
        entryA_cal = find_relative_trading_date(start_date, -1, trading_dates)
        entryA_px  = get_close(code, entryA_cal)
        prevA_cal = find_relative_trading_date(entryA_cal, -1, trading_dates) if entryA_cal else None
        prevA_close = get_close(code, prevA_cal)

        # A 濾網
        A_skip, reasonA = False, None
        A_gap = np.nan
        if pd.notna(entryA_px) and pd.notna(prevA_close) and prevA_close != 0:
            A_gap = entryA_px / prevA_close - 1
            if A_gap > limit_up_th: A_skip = True

        if pd.isna(entryA_px) or pd.isna(exit_px): A_skip, reasonA = True, "missing_price_A"
        elif pd.isna(prevA_close): A_skip, reasonA = True, "missing_prev_close_A"
        elif A_skip: reasonA = f"limit_up_A_gap={A_gap:.4%}"

        if not A_skip:
            rowsA.append({
                "event_id": event_id, "number": number, "code": code,
                "start_date": start_date, "ann_date": ann_date,
                "entry_rule": "A_start_m1_close",
                "entry_cal_date": entryA_cal, "entry_price": entryA_px,
                "exit_cal_date": exit_cal, "exit_price": exit_px
            })
        else:
            skipped.append({"event_id": event_id, "code": code, "which": "A", "reason": reasonA})

        # -------------------------------------------------------
        # 策略 B：ann_date 的 +1 開盤進場
        # -------------------------------------------------------
        entryB_cal = find_relative_trading_date(ann_date, 1, trading_dates)
        entryB_px  = get_open(code, entryB_cal)
        prevB_cal = find_relative_trading_date(entryB_cal, -1, trading_dates) if entryB_cal else None
        prevB_close = get_close(code, prevB_cal)

        # B 濾網
        B_skip, reasonB = False, None
        B_gap = np.nan
        if pd.notna(entryB_px) and pd.notna(prevB_close) and prevB_close != 0:
            B_gap = entryB_px / prevB_close - 1
            if B_gap > limit_up_th: B_skip = True

        if pd.isna(entryB_px) or pd.isna(exit_px): B_skip, reasonB = True, "missing_price_B"
        elif pd.isna(prevB_close): B_skip, reasonB = True, "missing_prev_close_B"
        elif B_skip: reasonB = f"limit_up_B_gap={B_gap:.4%}"

        if not B_skip:
            rowsB.append({
                "event_id": event_id, "number": number, "code": code,
                "start_date": start_date, "ann_date": ann_date,
                "entry_rule": "B_ann_p1_open",
                "entry_cal_date": entryB_cal, "entry_price": entryB_px,
                "exit_cal_date": exit_cal, "exit_price": exit_px
            })
        else:
            skipped.append({"event_id": event_id, "code": code, "which": "B", "reason": reasonB})

        # -------------------------------------------------------
        # ✅ 策略 C：start_date 的 t=0 開盤進場 (新增)
        # -------------------------------------------------------
        entryC_cal = find_relative_trading_date(start_date, 0, trading_dates)
        entryC_px  = get_open(code, entryC_cal)
        prevC_cal = find_relative_trading_date(entryC_cal, -1, trading_dates) if entryC_cal else None
        prevC_close = get_close(code, prevC_cal)

        # C 濾網 (比照 B，開盤漲停就不追)
        C_skip, reasonC = False, None
        C_gap = np.nan
        if pd.notna(entryC_px) and pd.notna(prevC_close) and prevC_close != 0:
            C_gap = entryC_px / prevC_close - 1
            if C_gap > limit_up_th: C_skip = True

        if pd.isna(entryC_px) or pd.isna(exit_px): C_skip, reasonC = True, "missing_price_C"
        elif pd.isna(prevC_close): C_skip, reasonC = True, "missing_prev_close_C"
        elif C_skip: reasonC = f"limit_up_C_gap={C_gap:.4%}"

        if not C_skip:
            rowsC.append({
                "event_id": event_id, "number": number, "code": code,
                "start_date": start_date, "ann_date": ann_date,
                "entry_rule": "C_start_p0_open",
                "entry_cal_date": entryC_cal, "entry_price": entryC_px,
                "exit_cal_date": exit_cal, "exit_price": exit_px
            })
        else:
            skipped.append({"event_id": event_id, "code": code, "which": "C", "reason": reasonC})

    tradesA = pd.DataFrame(rowsA).copy()
    tradesB = pd.DataFrame(rowsB).copy()
    tradesC = pd.DataFrame(rowsC).copy() # ✅ 新增
    skipped_df = pd.DataFrame(skipped).copy()

    print(f"A(詢圈前一日收盤) 成交筆數: {len(tradesA)}")
    print(f"B(公告後一日開盤) 成交筆數: {len(tradesB)}")
    print(f"C(詢圈當日開盤)   成交筆數: {len(tradesC)}")

    return tradesA, tradesB, tradesC, skipped_df

def compute_trade_stats_cash(
    trades: pd.DataFrame,
    label: str,
    capm_df: pd.DataFrame,
    initial_capital: float = 10_000_000,      # ✅ 1000萬
    cash_per_event: float = 1_000_000,        # ✅ 每事件 100萬
):
    """
    trades 需至少有：
    code, entry_cal_date, entry_price, exit_cal_date, exit_price, start_date, event_id, number
    """

    t = trades.copy()

    # 基本清理
    t["entry_cal_date"] = pd.to_datetime(t["entry_cal_date"], errors="coerce")
    t["exit_cal_date"]  = pd.to_datetime(t["exit_cal_date"], errors="coerce")
    t["entry_price"] = pd.to_numeric(t["entry_price"], errors="coerce")
    t["exit_price"]  = pd.to_numeric(t["exit_price"], errors="coerce")

    t = t.dropna(subset=["code", "entry_cal_date", "exit_cal_date", "entry_price", "exit_price"]).copy()
    t = t.sort_values(["entry_cal_date", "event_id"]).reset_index(drop=True)

    # ✅ 每筆事件投入固定金額 -> 整張買入
    t["buy_lots"] = (cash_per_event // (t["entry_price"] * 1000)).astype("Int64")
    t = t[t["buy_lots"].notna() & (t["buy_lots"] > 0)].copy()

    t["entry_cash"] = t["buy_lots"].astype(float) * t["entry_price"] * 1000.0
    t["gross_r"] = (t["exit_price"] - t["entry_price"]) / t["entry_price"]
    t["net_r"]   = t["gross_r"] - TRANSACTION_COST_BPS

    # ✅ 用實際投入(entry_cash)算損益（不是用 initial_capital）
    t["profit_ntd"] = t["net_r"] * t["entry_cash"]

    t["trade_no"] = np.arange(1, len(t) + 1)
    t["label"] = label

    # 摘要
    win_rate = (t["net_r"] > 0).mean() if len(t) else np.nan
    mean_r = t["net_r"].mean() if len(t) else np.nan
    std_r  = t["net_r"].std(ddof=1) if len(t) else np.nan

    avg_win  = t.loc[t["net_r"] > 0, "net_r"].mean() if len(t) else np.nan
    avg_loss = t.loc[t["net_r"] <= 0, "net_r"].mean() if len(t) else np.nan
    payoff_ratio = abs(avg_win / avg_loss) if (pd.notna(avg_win) and pd.notna(avg_loss) and avg_loss != 0) else np.nan

    gross_profit = t.loc[t["profit_ntd"] > 0, "profit_ntd"].sum() if len(t) else 0.0
    gross_loss   = -t.loc[t["profit_ntd"] <= 0, "profit_ntd"].sum() if len(t) else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else np.nan

    summary = pd.DataFrame([{
        "label": label,
        "initial_capital": initial_capital,
        "cash_per_event": cash_per_event,
        "n_trades": len(t),
        "win_rate": win_rate,
        "mean_net_r": mean_r,
        "std_net_r": std_r,
        "payoff_ratio": payoff_ratio,
        "profit_factor": profit_factor,
        "total_profit_ntd": t["profit_ntd"].sum() if len(t) else 0.0,
    }])

    return t, summary


# ============================================================
# 2) 用交易日序列做 daily equity（mark-to-market）+ MDD
# ============================================================
def build_daily_equity_from_trades(
    trades: pd.DataFrame,
    capm_df: pd.DataFrame,
    initial_capital: float = 10_000_000,
):
    """
    產出 equity_daily（每日）：
    - date
    - cash_ntd
    - position_value_ntd
    - equity_ntd
    - drawdown_ntd
    - mdd_ntd (回傳值)
    """

    if trades is None or len(trades) == 0:
        equity_daily = pd.DataFrame(columns=["date","cash_ntd","position_value_ntd","equity_ntd","drawdown_ntd"])
        return equity_daily, np.nan

    # trading dates
    trading_dates = sorted(pd.to_datetime(capm_df["date"], errors="coerce").dropna().unique())
    trading_dates = pd.to_datetime(trading_dates)

    # close matrix
    px = capm_df[["code","date","close"]].copy()
    px["code"] = px["code"].astype(str).str.strip()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    px["close"] = pd.to_numeric(px["close"], errors="coerce")
    px = px.dropna(subset=["code","date","close"]).copy()
    price_wide = px.pivot_table(index="date", columns="code", values="close", aggfunc="last").sort_index()

    t = trades.copy()
    t["entry_cal_date"] = pd.to_datetime(t["entry_cal_date"], errors="coerce")
    t["exit_cal_date"]  = pd.to_datetime(t["exit_cal_date"], errors="coerce")
    t["buy_lots"] = pd.to_numeric(t["buy_lots"], errors="coerce")

    t = t.dropna(subset=["code","entry_cal_date","exit_cal_date","buy_lots","entry_cash","profit_ntd"]).copy()
    t = t.sort_values(["entry_cal_date","event_id"]).reset_index(drop=True)

    d0 = t["entry_cal_date"].min()
    d1 = t["exit_cal_date"].max()

    all_days = trading_dates[(trading_dates >= d0) & (trading_dates <= d1)]
    all_days = pd.to_datetime(all_days)

    # 每天持倉 (code, lots)
    pos_by_day = {d: [] for d in all_days}
    for _, r in t.iterrows():
        code = str(r["code"]).strip()
        lots = int(r["buy_lots"])
        entry_d = pd.to_datetime(r["entry_cal_date"])
        exit_d  = pd.to_datetime(r["exit_cal_date"])
        for d in all_days:
            if (d >= entry_d) and (d <= exit_d):
                pos_by_day[d].append((code, lots))

    # 現金流：entry 扣 entry_cash；exit 加回 entry_cash + profit（profit 已含成本）
    cash_flow = pd.Series(0.0, index=all_days)
    for _, r in t.iterrows():
        entry_d = pd.to_datetime(r["entry_cal_date"])
        exit_d  = pd.to_datetime(r["exit_cal_date"])
        cash_flow.loc[entry_d] -= float(r["entry_cash"])
        cash_flow.loc[exit_d]  += float(r["entry_cash"] + r["profit_ntd"])

    cash = float(initial_capital)
    rows = []

    for d in all_days:
        cash += float(cash_flow.loc[d])

        pos_val = 0.0
        if d in price_wide.index:
            for code, lots in pos_by_day[d]:
                if code in price_wide.columns:
                    p = price_wide.at[d, code]
                    if pd.notna(p):
                        pos_val += float(p) * 1000.0 * float(lots)

        equity = cash + pos_val
        rows.append({
            "date": d,
            "cash_ntd": cash,
            "position_value_ntd": pos_val,
            "equity_ntd": equity
        })

    equity_daily = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    running_max = equity_daily["equity_ntd"].cummax()
    equity_daily["drawdown_ntd"] = equity_daily["equity_ntd"] - running_max
    mdd_ntd = float(equity_daily["drawdown_ntd"].min())

    return equity_daily, mdd_ntd


def run_ABC_time_equity(
    df_book_building: pd.DataFrame,
    capm_df: pd.DataFrame,
    initial_capital: float = 10_000_000,
    cash_per_event: float = 1_000_000,
    limit_up_th: float = 0.085,
    cost_bps: float = None,
    **kwargs
):
    """
    執行 A/B/C 三策略比較，並產出詳細指標 (含夏普比率)。
    """
    # 相容舊參數
    if "INITIAL_CAPITAL" in kwargs and (initial_capital is None or initial_capital == 10_000_000):
        initial_capital = kwargs["INITIAL_CAPITAL"]
    if "initial_cash" in kwargs and (initial_capital is None or initial_capital == 10_000_000):
        initial_capital = kwargs["initial_cash"]

    if cost_bps is None:
        cost_bps = TRANSACTION_COST_BPS

    # 1. 準備資料 & 產生交易訊號
    df_has_ann = prepare_events_with_ann(df_book_building)
    tradesA_raw, tradesB_raw, tradesC_raw, skipped_df = build_trades_ABC(df_has_ann, capm_df, limit_up_th=limit_up_th)

    def add_lots(trades):
        if len(trades) == 0: return trades
        t = trades.copy()
        t["buy_lots"] = (cash_per_event // (t["entry_price"] * 1000)).astype("Int64")
        t = t[t["buy_lots"].notna() & (t["buy_lots"] > 0)].copy()
        return t

    tradesA = add_lots(tradesA_raw)
    tradesB = add_lots(tradesB_raw)
    tradesC = add_lots(tradesC_raw)

    # 2. 建立每日帳務 (Equity Curve)
    equityA_daily, _, tradesA_enriched, _ = build_daily_ledger_from_trades_v2(
        tradesA, capm_df, initial_capital, cash_per_event, cost_bps,
        entry_date_col="entry_cal_date", exit_date_col="exit_cal_date", lots_col="buy_lots"
    )
    
    equityB_daily, _, tradesB_enriched, _ = build_daily_ledger_from_trades_v2(
        tradesB, capm_df, initial_capital, cash_per_event, cost_bps,
        entry_date_col="entry_cal_date", exit_date_col="exit_cal_date", lots_col="buy_lots"
    )

    equityC_daily, _, tradesC_enriched, _ = build_daily_ledger_from_trades_v2(
        tradesC, capm_df, initial_capital, cash_per_event, cost_bps,
        entry_date_col="entry_cal_date", exit_date_col="exit_cal_date", lots_col="buy_lots"
    )

    # 補算 net_r
    for t_df in [tradesA_enriched, tradesB_enriched, tradesC_enriched]:
        if len(t_df) > 0:
            t_df["net_r"] = t_df["realized_pnl"] / t_df["entry_cash"]

    # 3. 定義計算指標的函數
    def get_metrics(trades_df, equity_df, label):
        # 預設空值
        default_res = {
            "策略": label, "平均單筆報酬率": 0.0, "總獲利(淨利)": 0, "獲利因子": 0.0, 
            "賺賠比": 0.0, "MDD": 0, "夏普比率": 0.0, "勝率": 0.0, "交易次數": 0
        }
        
        if len(trades_df) == 0:
            return default_res

        # 基礎交易統計
        win_count = (trades_df["realized_pnl"] > 0).sum()
        n_trades = len(trades_df)
        win_rate = win_count / n_trades if n_trades > 0 else 0
        avg_ret = trades_df["net_r"].mean()
        total_profit = trades_df["realized_pnl"].sum()
        
        gross_win = trades_df.loc[trades_df["realized_pnl"] > 0, "realized_pnl"].sum()
        gross_loss = abs(trades_df.loc[trades_df["realized_pnl"] <= 0, "realized_pnl"].sum())
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else np.inf
        
        avg_win = trades_df.loc[trades_df["realized_pnl"] > 0, "net_r"].mean()
        avg_loss = abs(trades_df.loc[trades_df["realized_pnl"] <= 0, "net_r"].mean())
        payoff = (avg_win / avg_loss) if (avg_loss > 0 and not pd.isna(avg_win)) else 0
        
        mdd = equity_df["drawdown_ntd"].min() if len(equity_df) > 0 else 0

        # ✅ 新增：計算年化夏普比率 (Sharpe Ratio)
        sharpe = 0.0
        if len(equity_df) > 2:
            # 計算日報酬率
            daily_rets = equity_df["equity_ntd"].pct_change().fillna(0)
            mean_r = daily_rets.mean()
            std_r = daily_rets.std(ddof=1)
            
            if std_r > 0:
                sharpe = (mean_r / std_r) * np.sqrt(252)

        return {
            "策略": label,
            "平均單筆報酬率": avg_ret,
            "總獲利(淨利)": total_profit,
            "獲利因子": profit_factor,
            "賺賠比": payoff,
            "MDD": mdd,
            "夏普比率": sharpe,  # ✅ 這裡
            "勝率": win_rate,
            "交易次數": n_trades
        }

    # 4. 計算並合併 Summary
    statsA = get_metrics(tradesA_enriched, equityA_daily, "策略A")
    statsB = get_metrics(tradesB_enriched, equityB_daily, "策略B")
    statsC = get_metrics(tradesC_enriched, equityC_daily, "策略C")
    
    summaryABC = pd.DataFrame([statsA, statsB, statsC])

    print("\n[A/B/C Strategy Detailed Summary]")
    print(summaryABC.to_string(float_format=lambda x: f"{x:.4f}"))

    return tradesA_enriched, tradesB_enriched, tradesC_enriched, equityA_daily, equityB_daily, equityC_daily, skipped_df, summaryABC

def plot_equity_compare_time(equityA, equityB, equityC, title="Equity Curve (Daily)"):
    """
    畫出 A vs B vs C 的比較圖 (自動裁切空白期)
    """
    # 檢查資料
    has_A = equityA is not None and len(equityA) > 0
    has_B = equityB is not None and len(equityB) > 0
    has_C = equityC is not None and len(equityC) > 0 # ✅

    if not (has_A or has_B or has_C):
        print("沒有 A/B/C 任何資料，無法畫圖。")
        return

    # 計算 DD%
    for df in [equityA, equityB, equityC]:
        if df is not None and len(df) > 0:
            if "running_max" not in df.columns:
                df["running_max"] = df["equity_ntd"].cummax()
            df["dd_pct"] = np.where(df["running_max"] > 0, 
                                    df["drawdown_ntd"] / df["running_max"], 0)

    # 找出最早開始日
    def get_first_active_date(df):
        if df is None or len(df) == 0: return None
        if "n_positions" in df.columns:
            active_days = df[df["n_positions"] > 0]["date"]
            if not active_days.empty: return active_days.min()
        return df["date"].min()

    start_A = get_first_active_date(equityA) if has_A else None
    start_B = get_first_active_date(equityB) if has_B else None
    start_C = get_first_active_date(equityC) if has_C else None # ✅

    valid_starts = [d for d in [start_A, start_B, start_C] if d is not None]
    
    if valid_starts:
        start_date = min(valid_starts)
        plot_start_date = start_date - pd.Timedelta(days=5)
    else:
        plot_start_date = None

    # 過濾資料
    if plot_start_date:
        if has_A: equityA = equityA[equityA["date"] >= plot_start_date].copy()
        if has_B: equityB = equityB[equityB["date"] >= plot_start_date].copy()
        if has_C: equityC = equityC[equityC["date"] >= plot_start_date].copy()

    # 開始畫圖
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10), sharex=True, gridspec_kw={'height_ratios': [3, 1, 1]})
    
    # Subplot 1: Equity
    if has_A: ax1.plot(equityA["date"], equityA["equity_ntd"], label="Strategy A (Start -1 Close)", linewidth=2, color="#1f77b4")
    if has_B: ax1.plot(equityB["date"], equityB["equity_ntd"], label="Strategy B (Ann +1 Open)", linewidth=2, alpha=0.8, color="#ff7f0e")
    if has_C: ax1.plot(equityC["date"], equityC["equity_ntd"], label="Strategy C (Start Open)", linewidth=2, alpha=0.8, color="#2ca02c") # ✅ Green
    
    ax1.set_title(title, fontsize=14, weight="bold")
    ax1.set_ylabel("Equity (NTD)", fontsize=12)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="upper left")

    # Subplot 2: Drawdown (NTD)
    if has_A: 
        ax2.fill_between(equityA["date"], equityA["drawdown_ntd"], 0, alpha=0.2, color="#1f77b4")
        ax2.plot(equityA["date"], equityA["drawdown_ntd"], linewidth=1, color="#1f77b4")
    if has_B: 
        ax2.fill_between(equityB["date"], equityB["drawdown_ntd"], 0, alpha=0.2, color="#ff7f0e")
        ax2.plot(equityB["date"], equityB["drawdown_ntd"], linewidth=1, color="#ff7f0e")
    if has_C: 
        ax2.fill_between(equityC["date"], equityC["drawdown_ntd"], 0, alpha=0.2, color="#2ca02c")
        ax2.plot(equityC["date"], equityC["drawdown_ntd"], linewidth=1, color="#2ca02c")
        
    ax2.set_ylabel("Drawdown (NTD)", fontsize=12)
    ax2.grid(True, alpha=0.3)

    # Subplot 3: Drawdown (%)
    if has_A: ax3.plot(equityA["date"], equityA["dd_pct"], label="A DD%", linewidth=1.5, color="#1f77b4")
    if has_B: ax3.plot(equityB["date"], equityB["dd_pct"], label="B DD%", linewidth=1.5, color="#ff7f0e", linestyle="--")
    if has_C: ax3.plot(equityC["date"], equityC["dd_pct"], label="C DD%", linewidth=1.5, color="#2ca02c", linestyle="-.")

    ax3.set_ylabel("Drawdown (%)", fontsize=12)
    ax3.set_xlabel("Date", fontsize=12)
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc="lower left", fontsize=8)
    
    import matplotlib.ticker as mtick
    ax3.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    plt.tight_layout()
    plt.show()

###############################################################################
#加入濾網限制 + 回測（只用詢圈）
###############################################################################
def backtest_in_sample_bookbuilding_daily_equity(
    df_book_building: pd.DataFrame,
    capm_df: pd.DataFrame,
    initial_capital: float = 10_000_000,
    trade_cash_per_event: float = 3_000_000,
    liq_avg_value_th_ntd: float = 30_000_000,
    limit_up_th: float = 0.085,
    entry_day: int = -1,        # 👈 參數控制：0=當天開盤, -1=前天收盤
    exit_day: int = 6,
    cost_bps: float = None,   
):
    """
    產出：
    - trades: 事件層交易明細
    - skipped_df: 被跳過事件
    - summary: 總結（含 Sharpe, Calmar, MDD%, Cumulative Ret, Risk-Reward）
    - equity_daily: 逐日帳務
    
    邏輯更新：
    1. 若 entry_day == 0  -> 用 Open 進場
    2. 若 entry_day == -1 -> 用 Close 進場
    """

    if cost_bps is None:
        cost_bps = TRANSACTION_COST_BPS

    # ==========
    # 0) 前處理：交易日 & 價格表
    # ==========
    trading_dates = get_trading_dates(capm_df)

    # ✅ 同時抓取 Close 和 Open
    px = capm_df[["code", "date", "close", "open", "value_1000"]].copy()
    px["code"] = px["code"].astype(str).str.strip()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    px["close"] = pd.to_numeric(px["close"], errors="coerce")
    px["open"]  = pd.to_numeric(px["open"], errors="coerce")
    px["value_ntd"] = pd.to_numeric(px["value_1000"], errors="coerce") * 1000
    
    # 這裡只 drop 兩個價格都缺的，避免單一缺漏
    px = px.dropna(subset=["code", "date"]).copy()

    # 建立快速查價表
    close_map = {(c, d): p for c, d, p in zip(px["code"], px["date"], px["close"])}
    open_map  = {(c, d): p for c, d, p in zip(px["code"], px["date"], px["open"])}
    value_map = {(c, d): v for c, d, v in zip(px["code"], px["date"], px["value_ntd"])}

    def get_close(code, d):
        if d is None or pd.isna(d): return np.nan
        return close_map.get((str(code).strip(), pd.to_datetime(d)), np.nan)

    def get_open(code, d):
        if d is None or pd.isna(d): return np.nan
        return open_map.get((str(code).strip(), pd.to_datetime(d)), np.nan)

    def get_value(code, d):
        if d is None or pd.isna(d): return np.nan
        return value_map.get((str(code).strip(), pd.to_datetime(d)), np.nan)

    # ==========
    # 1) 事件表處理
    # ==========
    evt = df_book_building.copy()
    evt["code"] = evt["code"].astype(str).str.strip()
    evt["number"] = evt["number"].astype(str).str.strip()
    evt["start_date"] = pd.to_datetime(evt["start_date"], errors="coerce")

    if "event_id" not in evt.columns:
        evt["event_id"] = (
            evt["code"] + "_" +
            evt["start_date"].dt.strftime("%Y%m%d") + "_" +
            evt["number"]
        )

    # 計算日期
    evt["entry_cal"] = evt["start_date"].apply(lambda d: find_relative_trading_date(d, entry_day, trading_dates))
    evt["exit_cal"]  = evt["start_date"].apply(lambda d: find_relative_trading_date(d, exit_day, trading_dates))
    
    # 前一天交易日 (用來比對漲幅)
    evt["prev_cal"]  = evt["entry_cal"].apply(
        lambda d: find_relative_trading_date(d, -1, trading_dates) if pd.notna(d) else None
    )

    # ==============================================================================
    # ✅ 關鍵修改：根據 entry_day 決定是用 Open 還是 Close
    # ==============================================================================
    if entry_day == 0:
        # 事件日當天 -> 開盤進場
        print("💡 策略模式: 事件日當天 (t=0) [開盤] 進場")
        evt["entry_price"] = evt.apply(lambda r: get_open(r["code"], r["entry_cal"]), axis=1)
    elif entry_day == -1:
        # 事件日前一天 -> 收盤進場
        print("💡 策略模式: 事件日前一天 (t=-1) [收盤] 進場")
        evt["entry_price"] = evt.apply(lambda r: get_close(r["code"], r["entry_cal"]), axis=1)
    else:
        # 其他天數預設用收盤 (或者你可以自訂)
        print(f"💡 策略模式: t={entry_day} [收盤] 進場")
        evt["entry_price"] = evt.apply(lambda r: get_close(r["code"], r["entry_cal"]), axis=1)

    # 出場固定用 Close
    evt["exit_price"]  = evt.apply(lambda r: get_close(r["code"], r["exit_cal"]), axis=1)
    # 前一日固定用 Close (算漲跌幅用)
    evt["prev_close"]  = evt.apply(lambda r: get_close(r["code"], r["prev_cal"]), axis=1)

    skipped = []

    # 1. 缺價檢查
    m_px = evt["entry_price"].isna() | evt["exit_price"].isna()
    if m_px.any():
        tmp = evt.loc[m_px, ["event_id","code","number","start_date","entry_cal","exit_cal"]].copy()
        tmp["reason"] = "missing_entry_or_exit_price"
        skipped.append(tmp)

    # 2. 漲幅過大檢查
    # 邏輯：(進場價 / 前一日收盤價 - 1) > 8.5% 則跳過
    evt["gap"] = evt["entry_price"] / evt["prev_close"] - 1
    
    m_prev_missing = evt["prev_close"].isna() | (evt["prev_close"] == 0)
    if m_prev_missing.any():
        tmp = evt.loc[m_prev_missing, ["event_id","code","number","start_date","entry_cal","prev_cal"]].copy()
        tmp["reason"] = "missing_prev_close_for_gap_check"
        skipped.append(tmp)

    m_limit = (~m_px) & (~m_prev_missing) & (evt["gap"] > limit_up_th)
    if m_limit.any():
        tmp = evt.loc[m_limit, ["event_id","code","number","start_date","gap","entry_price","prev_close"]].copy()
        tmp["reason"] = tmp["gap"].map(lambda x: f"price_gap_too_high={x:.4%}")
        skipped.append(tmp)

    # 3. 流動性濾網
    offs = [-2,-3,-4,-5,-6,-7]
    liq = (
        evt[["event_id","code","start_date"]]
        .assign(_k=1)
        .merge(pd.DataFrame({"off": offs, "_k": 1}), on="_k")
        .drop(columns="_k")
    )
    liq["liq_date"] = liq.apply(
        lambda r: find_relative_trading_date(r["start_date"], int(r["off"]), trading_dates),
        axis=1
    )
    liq["value_ntd"] = liq.apply(lambda r: get_value(r["code"], r["liq_date"]), axis=1)

    liq_avg = (
        liq.groupby("event_id", as_index=False)["value_ntd"]
        .mean()
        .rename(columns={"value_ntd": "avg_value_ntd"})
    )
    evt = evt.merge(liq_avg, on="event_id", how="left")

    m_liq_missing = evt["avg_value_ntd"].isna()
    if m_liq_missing.any():
        tmp = evt.loc[m_liq_missing, ["event_id","code","number","start_date"]].copy()
        tmp["reason"] = "missing_value_for_liquidity_filter"
        skipped.append(tmp)

    m_liq_fail = (~m_liq_missing) & (evt["avg_value_ntd"] < liq_avg_value_th_ntd)
    if m_liq_fail.any():
        tmp = evt.loc[m_liq_fail, ["event_id","code","number","start_date","avg_value_ntd"]].copy()
        tmp["reason"] = tmp["avg_value_ntd"].map(lambda x: f"liq_filter_fail_avg_value={x:,.0f}")
        skipped.append(tmp)

    # 排除所有被 skip 的事件
    bad_ids = set(pd.concat(skipped, ignore_index=True)["event_id"].unique()) if skipped else set()
    trades = evt[~evt["event_id"].isin(bad_ids)].copy()

    # ==========
    # 2) 計算手數與費用
    # ==========
    trades = trades.sort_values(["entry_cal","event_id"]).reset_index(drop=True)
    trades["trade_no"] = np.arange(1, len(trades) + 1)

    trades["buy_lots"] = (trade_cash_per_event // (trades["entry_price"] * 1000)).astype("Int64")
    m_lots0 = trades["buy_lots"].isna() | (trades["buy_lots"] <= 0)
    if m_lots0.any():
        tmp = trades.loc[m_lots0, ["event_id","code","number","start_date","entry_price"]].copy()
        tmp["reason"] = "buy_lots_zero"
        skipped.append(tmp)
        trades = trades.loc[~m_lots0].copy()

    trades["entry_cash_gross"] = trades["buy_lots"].astype(float) * trades["entry_price"] * 1000
    trades["exit_cash_gross"]  = trades["buy_lots"].astype(float) * trades["exit_price"]  * 1000

    trades["fee_ntd"] = trades["entry_cash_gross"] * float(cost_bps)
    trades["realized_pnl_ntd"] = (trades["exit_cash_gross"] - trades["entry_cash_gross"]) - trades["fee_ntd"]
    trades["gross_r"] = (trades["exit_price"] - trades["entry_price"]) / trades["entry_price"]
    trades["net_r"]   = trades["realized_pnl_ntd"] / trades["entry_cash_gross"]

    # ==========
    # 3) 建立逐日 Ledger
    # ==========
    # ✅ allow_leverage=True，確保資金無限
    equity_daily, positions_daily, trades_enriched, skipped_cash = build_daily_ledger_from_trades_v2(
        trades=trades,
        capm_df=capm_df,
        initial_capital=initial_capital,
        trade_cash_per_event=trade_cash_per_event,
        cost_bps=float(cost_bps),
        price_col="close",
        date_col_px="date",
        code_col_px="code",
        entry_date_col="entry_cal",
        exit_date_col="exit_cal",
        entry_price_col="entry_price",
        exit_price_col="exit_price",
        lots_col="buy_lots",
        allow_leverage=True 
    )

    if skipped_cash is not None and len(skipped_cash) > 0:
        tmp = skipped_cash.copy()
        tmp["reason"] = "insufficient_cash"
        tmp = tmp.merge(
            trades_enriched[["trade_idx","number","start_date"]],
            on="trade_idx",
            how="left"
        )
        skipped.append(tmp[["event_id","code","number","start_date","reason","need_cash","cash","date","trade_idx"]])

    skipped_df = pd.concat(skipped, ignore_index=True) if skipped else pd.DataFrame()

    # ==========
    # 4) Summary (含 Sharpe, Calmar, MDD%, Cumulative Return, Risk-Reward)
    # ==========
    mdd_ntd = float(equity_daily["drawdown_ntd"].min()) if len(equity_daily) else np.nan
    
    # 預設值
    sharpe_ratio = np.nan
    calmar_ratio = np.nan
    max_dd_pct = np.nan
    cum_ret = np.nan
    risk_reward_ratio = np.nan

    if len(equity_daily) > 2:
        # 4.1 Sharpe (年化)
        equity_daily["daily_ret"] = equity_daily["equity_ntd"].pct_change().fillna(0)
        mean_ret = equity_daily["daily_ret"].mean()
        std_ret = equity_daily["daily_ret"].std(ddof=1)
        
        if std_ret > 0:
            sharpe_ratio = (mean_ret / std_ret) * np.sqrt(252)
        else:
            sharpe_ratio = 0.0
            
        # 4.2 Calmar & MDD% & Cumulative Return
        start_date = equity_daily["date"].min()
        end_date = equity_daily["date"].max()
        days = (end_date - start_date).days
        years = days / 252
        
        final_equity = equity_daily["equity_ntd"].iloc[-1]
        
        # ✅ 累積報酬率 (Cumulative Return)
        cum_ret = (final_equity / initial_capital) - 1

        if years > 0:
            cagr = (final_equity / initial_capital) ** (1 / years) - 1
        else:
            cagr = 0.0
            
        # 計算 MDD %
        roll_max = equity_daily["equity_ntd"].cummax()
        dd_pct = (equity_daily["equity_ntd"] - roll_max) / roll_max
        max_dd_pct = dd_pct.min() # 這是一個負值，例如 -0.2
        
        if max_dd_pct != 0:
            calmar_ratio = cagr / abs(max_dd_pct)
        else:
            calmar_ratio = np.nan

    if len(trades_enriched) > 0:
        win_rate = (trades_enriched["realized_pnl_ntd"] > 0).mean()
        avg_net_r = trades_enriched["net_r"].mean()
        std_net_r = trades_enriched["net_r"].std(ddof=1)

        avg_win  = trades_enriched.loc[trades_enriched["net_r"] > 0, "net_r"].mean()
        avg_loss = trades_enriched.loc[trades_enriched["net_r"] <= 0, "net_r"].mean()
        payoff_ratio = abs(avg_win / avg_loss) if (pd.notna(avg_win) and pd.notna(avg_loss) and avg_loss != 0) else np.nan

        total_profit_ntd = trades_enriched["realized_pnl_ntd"].sum()
        gross_profit = trades_enriched.loc[trades_enriched["realized_pnl_ntd"] > 0, "realized_pnl_ntd"].sum()
        gross_loss   = -trades_enriched.loc[trades_enriched["realized_pnl_ntd"] <= 0, "realized_pnl_ntd"].sum()
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else np.nan

        # ✅ 風報比 (總淨利 / 最大回撤金額)
        if mdd_ntd != 0 and not pd.isna(mdd_ntd):
            risk_reward_ratio = total_profit_ntd / abs(mdd_ntd)
        else:
            risk_reward_ratio = np.nan

        summary = pd.DataFrame([{
            "n_events": len(evt),
            "n_trades": len(trades_enriched),
            "win_rate": float(win_rate),
            "total_profit_ntd": float(total_profit_ntd),
            "avg_net_r": float(avg_net_r) if pd.notna(avg_net_r) else np.nan,
            "std_net_r": float(std_net_r) if pd.notna(std_net_r) else np.nan,
            "sharpe_ratio": float(sharpe_ratio),
            "calmar_ratio": float(calmar_ratio),
            "cum_ret": float(cum_ret),          
            "risk_reward_ratio": float(risk_reward_ratio), 
            "mdd_pct": float(max_dd_pct),        
            "mdd_ntd": float(mdd_ntd),
            "payoff_ratio": payoff_ratio,
            "profit_factor": profit_factor,
            "final_equity_ntd": float(equity_daily["equity_ntd"].iloc[-1]) if len(equity_daily) else np.nan,
        }])
    else:
        summary = pd.DataFrame([{
            "n_events": len(evt),
            "n_trades": 0,
            "win_rate": np.nan,
            "total_profit_ntd": 0.0,
            "avg_net_r": np.nan,
            "std_net_r": np.nan,
            "sharpe_ratio": np.nan,
            "calmar_ratio": np.nan,
            "cum_ret": np.nan,
            "risk_reward_ratio": np.nan,
            "mdd_pct": np.nan,
            "mdd_ntd": np.nan,
            "payoff_ratio": np.nan,
            "profit_factor": np.nan,
            "final_equity_ntd": float(equity_daily["equity_ntd"].iloc[-1]) if len(equity_daily) else np.nan,
        }])

    return trades_enriched.reset_index(drop=True), skipped_df, summary, equity_daily, positions_daily
def plot_equity_and_dd_time(equity_daily: pd.DataFrame, title="Strategy Equity"):
    """
    畫單一策略的三層圖：
    1. Equity
    2. Drawdown (NTD)
    3. Drawdown (%) -> 指定紅色
    """
    if equity_daily is None or len(equity_daily) == 0:
        print("沒有 equity 資料，無法畫圖")
        return

    # 確保有 dd_pct
    if "drawdown_pct" not in equity_daily.columns:
         equity_daily["drawdown_pct"] = np.where(equity_daily["running_max"] != 0,
            equity_daily["drawdown_ntd"] / equity_daily["running_max"], 0)

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [3, 1, 1]})

    # --- 1. Equity ---
    ax1.plot(equity_daily["date"], equity_daily["equity_ntd"], color="#1f60b4", linewidth=2)
    ax1.set_title(title, fontsize=15, weight="bold")
    ax1.set_ylabel("Equity (NTD)")
    ax1.grid(True, alpha=0.3)

    # --- 2. DD (NTD) ---
    ax2.fill_between(equity_daily["date"], equity_daily["drawdown_ntd"], 0, color="red", alpha=0.2)
    ax2.plot(equity_daily["date"], equity_daily["drawdown_ntd"], color="red", linewidth=1)
    ax2.set_ylabel("Drawdown (NTD)")
    ax2.grid(True, alpha=0.3)

    # --- 3. DD (%) -> 紅色 ---
    # 轉成 % 顯示
    dd_pct_series = equity_daily["drawdown_pct"] * 100
    
    ax3.fill_between(equity_daily["date"], dd_pct_series, 0, color="red", alpha=0.2)
    ax3.plot(equity_daily["date"], dd_pct_series, color="red", linewidth=1)
    ax3.set_ylabel("Drawdown (%)")
    ax3.set_xlabel("Date")
    ax3.grid(True, alpha=0.3)
    
    # 格式化 Y 軸
    import matplotlib.ticker as mtick
    ax3.yaxis.set_major_formatter(mtick.PercentFormatter())

    plt.tight_layout()
    plt.show()
# ============================================================

def build_daily_ledger_from_trades_v2(
    trades: pd.DataFrame,
    capm_df: pd.DataFrame,
    initial_capital: float = 10_000_000,
    trade_cash_per_event: float = 3_000_000,
    cost_bps: float = 0.0103,
    price_col: str = "close",
    date_col_px: str = "date",
    code_col_px: str = "code",
    entry_date_col: str = "entry_cal",
    exit_date_col: str = "exit_cal",
    entry_price_col: str = "entry_price",
    exit_price_col: str = "exit_price",
    lots_col: str = "buy_lots",
    allow_leverage: bool = False,
):
    """
    回傳：
    1) equity_daily：
       date, cash_ntd, position_value_ntd,
       unrealized_pnl_eod_ntd, unrealized_pnl_change_ntd,
       realized_pnl_today_ntd, realized_pnl_cum_ntd,
       fee_today_ntd, fee_cum_ntd,
       equity_ntd, drawdown_ntd, drawdown_pct, n_positions
    2) positions_daily：逐日逐持倉明細（方便你對帳）
    3) trades_enriched：補齊 entry_cash/exit_cash/fee/realized_pnl/trade_idx
    4) skipped_not_executed：現金不足而未下單的事件
    """

    if trades is None or len(trades) == 0:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    t = trades.copy()

    # --- 標準化欄位 ---
    t["code"] = t["code"].astype(str).str.strip()
    for c in [entry_date_col, exit_date_col]:
        if c in t.columns:
            t[c] = pd.to_datetime(t[c], errors="coerce")
    for c in [entry_price_col, exit_price_col]:
        if c in t.columns:
            t[c] = pd.to_numeric(t[c], errors="coerce")

    # --- 價格表（用每日 close 做估值）---
    px = capm_df[[code_col_px, date_col_px, price_col]].copy()
    px[code_col_px] = px[code_col_px].astype(str).str.strip()
    px[date_col_px] = pd.to_datetime(px[date_col_px], errors="coerce")
    px[price_col] = pd.to_numeric(px[price_col], errors="coerce")
    px = px.dropna(subset=[code_col_px, date_col_px, price_col]).copy()

    price_map = {(c, d): p for c, d, p in zip(px[code_col_px], px[date_col_px], px[price_col])}

    def get_price(code, d):
        if pd.isna(d):
            return np.nan
        return price_map.get((str(code).strip(), pd.to_datetime(d)), np.nan)

    # --- lots：若沒給就自算 ---
    if lots_col not in t.columns or t[lots_col].isna().all():
        t[lots_col] = np.floor(trade_cash_per_event / (t[entry_price_col] * 1000)).astype("Int64")

    t["lots"] = pd.to_numeric(t[lots_col], errors="coerce").fillna(0).astype(int)
    t = t[(t["lots"] > 0) & t[entry_date_col].notna() & t[exit_date_col].notna()].copy()

    # 交易金額（不含費用）
    t["entry_cash"] = t["lots"] * t[entry_price_col] * 1000
    t["exit_cash"]  = t["lots"] * t[exit_price_col]  * 1000

    # 費用：用 entry_cash 當基底扣一次（= round-trip 總成本簡化）
    # 若你的 cost_bps 是「單邊」，改成：t["fee"] = t["entry_cash"] * (2 * cost_bps)
    t["fee"] = t["entry_cash"] * float(cost_bps)

    # 已實現損益：賣出 - 成本 - 費用
    t["realized_pnl"] = (t["exit_cash"] - t["entry_cash"]) - t["fee"]

    # 排序（同天多筆要可重現）
    sort_cols = [entry_date_col]
    if "event_id" in t.columns:
        sort_cols.append("event_id")
    t = t.sort_values(sort_cols).reset_index(drop=True)
    t["trade_idx"] = np.arange(len(t))  # ✅ ledger 用這個當唯一鍵

    # 全期間交易日序列
    start = t[entry_date_col].min()
    end   = t[exit_date_col].max()
    all_days = pd.DatetimeIndex(sorted(
        px[(px[date_col_px] >= start) & (px[date_col_px] <= end)][date_col_px].unique()
    ))

    # 建 entry/exit 的索引清單
    enters, exits = {}, {}
    for i, r in t.iterrows():
        enters.setdefault(r[entry_date_col], []).append(i)
        exits.setdefault(r[exit_date_col], []).append(i)

    # 持倉狀態：key=列索引 i（對應 trade_idx），每筆事件獨立成一個倉位 unit
    pos_state = {}
    cash = float(initial_capital)

    realized_cum = 0.0
    fee_cum = 0.0
    prev_unreal_eod = 0.0
    running_max = -np.inf

    equity_rows = []
    pos_rows = []
    skipped_not_executed = []

    for d in all_days:
        realized_today = 0.0
        fee_today = 0.0

        # 1) 先進場（扣現金 + 費用）
        if d in enters:
            for idx in enters[d]:
                r = t.loc[idx]
                code = str(r["code"]).strip()
                lots = int(r["lots"])
                entry_price = float(r[entry_price_col])

                entry_cash = lots * entry_price * 1000
                fee = float(r["fee"])
                need_cash = entry_cash + fee

                if (not allow_leverage) and (cash < need_cash):
                    skipped_not_executed.append({
                        "date": d,
                        "trade_idx": int(r["trade_idx"]),
                        "event_id": r.get("event_id", None),
                        "code": code,
                        "need_cash": float(need_cash),
                        "cash": float(cash),
                    })
                    continue

                cash -= need_cash
                fee_today += fee
                fee_cum += fee

                pos_state[idx] = {
                    "code": code,
                    "lots": lots,
                    "cost_price": entry_price,
                    "entry_date": d,
                }

        # 2) 再出場（加回現金 + 認列已實現）
        if d in exits:
            for idx in exits[d]:
                if idx not in pos_state:
                    continue  # 可能因資金不足沒進場
                r = t.loc[idx]
                lots = pos_state[idx]["lots"]
                exit_price = float(r[exit_price_col])
                exit_cash = lots * exit_price * 1000

                cash += exit_cash

                realized = float(r["realized_pnl"])
                realized_today += realized
                realized_cum += realized

                del pos_state[idx]

        # 3) 期末估值（未實現 / 持倉市值）
        position_value = 0.0
        unreal_eod = 0.0

        for idx, p in pos_state.items():
            code = p["code"]
            lots = p["lots"]
            cost = p["cost_price"]

            px_d = get_price(code, d)
            if pd.isna(px_d):
                continue

            mv = lots * float(px_d) * 1000
            position_value += mv
            unreal = lots * (float(px_d) - cost) * 1000
            unreal_eod += unreal

            pos_rows.append({
                "date": d,
                "trade_idx": int(t.loc[idx, "trade_idx"]),
                "event_id": t.loc[idx, "event_id"] if "event_id" in t.columns else int(t.loc[idx, "trade_idx"]),
                "code": code,
                "lots": lots,
                "cost_price": cost,
                "close": float(px_d),
                "market_value_ntd": float(mv),
                "unrealized_pnl_ntd": float(unreal),
                "entry_date": p["entry_date"],
            })

        equity = cash + position_value
        running_max = max(running_max, equity)
        dd = equity - running_max

        unreal_change = unreal_eod - prev_unreal_eod
        prev_unreal_eod = unreal_eod

        equity_rows.append({
            "date": d,
            "cash_ntd": float(cash),
            "position_value_ntd": float(position_value),

            "unrealized_pnl_eod_ntd": float(unreal_eod),
            "unrealized_pnl_change_ntd": float(unreal_change),

            "realized_pnl_today_ntd": float(realized_today),
            "realized_pnl_cum_ntd": float(realized_cum),

            "fee_today_ntd": float(fee_today),
            "fee_cum_ntd": float(fee_cum),

            "equity_ntd": float(equity),
            "drawdown_ntd": float(dd),
            "n_positions": int(len(pos_state)),
        })

    equity_daily = pd.DataFrame(equity_rows)
    positions_daily = pd.DataFrame(pos_rows)
    skipped_not_executed = pd.DataFrame(skipped_not_executed)

    if len(equity_daily) > 0:
        equity_daily["running_max"] = equity_daily["equity_ntd"].cummax()
        equity_daily["drawdown_pct"] = np.where(
            equity_daily["running_max"] != 0,
            equity_daily["drawdown_ntd"] / equity_daily["running_max"],
            np.nan
        )

    return equity_daily, positions_daily, t, skipped_not_executed

def export_ledger_excel(path, summary_df, trades_df, equity_daily, positions_daily,
                       skipped_df=None, skipped_not_executed_df=None):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if summary_df is not None:
            summary_df.to_excel(writer, index=False, sheet_name="summary")
        if trades_df is not None:
            trades_df.to_excel(writer, index=False, sheet_name="trades")
        if skipped_df is not None and len(skipped_df) > 0:
            skipped_df.to_excel(writer, index=False, sheet_name="skipped")
        if skipped_not_executed_df is not None and len(skipped_not_executed_df) > 0:
            skipped_not_executed_df.to_excel(writer, index=False, sheet_name="skipped_not_executed")
        if equity_daily is not None:
            equity_daily.to_excel(writer, index=False, sheet_name="equity_daily")
        if positions_daily is not None:
            positions_daily.to_excel(writer, index=False, sheet_name="positions_daily")

###############################################################################
# Main
###############################################################################

def main():

    # =========================
    # ✅ 資金設定（初始 1000萬、每事件 100萬）
    # =========================
    INITIAL_CAPITAL = 10_000_000
    CASH_PER_EVENT  = 3_000_000

    print("\n" + "=" * 60)
    print("=" * 60)
    print(f"事件日定義: {EVENT_DATE_TYPE}")
    print(f"事件窗口: t={EVENT_WINDOW_START} 到 t={EVENT_WINDOW_END}")
    print(f"交易策略: t={ENTRY_DAY} 進場, t={EXIT_DAY} 出場")
    print(f"初始本金: NT$ {INITIAL_CAPITAL:,}")
    print(f"每次事件投入: NT$ {CASH_PER_EVENT:,}")
    print(f"交易成本: {TRANSACTION_COST_BPS * 10000:.0f} bps")
    print("=" * 60)

    # =========================
    # 1) 載入資料
    # =========================
    df_book_building = load_book_building_data()
    market_data = load_market_data()

    capm_factors, beta_all = load_capm_factors()
    capm_df = calculate_abnormal_returns(market_data, capm_factors, beta_all)

    # =========================
    # 2) CAR（你要留就留）
    # =========================
    event_panel = create_event_panel(df_book_building, capm_df)
    plot_average_car(event_panel)

    # =========================
    # 3) A/B 策略：時間序列權益曲線 + MDD（日頻）
    # =========================
    # 3) A/B/C 策略比較
    tradesA, tradesB, tradesC, equityA, equityB, equityC, skippedABC, summaryABC = run_ABC_time_equity(
      df_book_building=df_book_building,
      capm_df=capm_df,
      initial_cash=INITIAL_CAPITAL,
      cash_per_event=CASH_PER_EVENT
    )

    plot_equity_compare_time(
        equityA, equityB, equityC,
        title=f"Strategy Comparison: A vs B vs C"
    )

    print("\n===== A/B/C Summary =====")
    print(summaryABC)

    # 檔名改為 ABC 以示區別
    out_abc = os.path.join(BASE_PATH, "ABC_time_equity.xlsx")
    
    with pd.ExcelWriter(out_abc, engine="openpyxl") as writer:
        # 1. 總表
        summaryABC.to_excel(writer, index=False, sheet_name="summary")
        
        # 2. 交易明細 (A, B, C)
        tradesA.to_excel(writer, index=False, sheet_name="tradesA")
        tradesB.to_excel(writer, index=False, sheet_name="tradesB")
        tradesC.to_excel(writer, index=False, sheet_name="tradesC") # ✅ 新增
        
        # 3. 每日權益 (A, B, C)
        if equityA is not None: equityA.to_excel(writer, index=False, sheet_name="equityA_daily")
        if equityB is not None: equityB.to_excel(writer, index=False, sheet_name="equityB_daily")
        if equityC is not None: equityC.to_excel(writer, index=False, sheet_name="equityC_daily") # ✅ 新增
        
        # 4. 被跳過的交易
        skippedABC.to_excel(writer, index=False, sheet_name="skipped")
        
    print(f"✅ 已輸出：{out_abc}")

    # =========================
    # 4) In-sample：詢圈 + 濾網 + ✅已實/未實拆分 ledger（日頻）
    # =========================
    trades_is, skipped_is, summary_is, equity_daily, positions_daily = backtest_in_sample_bookbuilding_daily_equity(
        df_book_building=df_book_building,
        capm_df=capm_df,
        initial_capital=INITIAL_CAPITAL,
        trade_cash_per_event=CASH_PER_EVENT,
        liq_avg_value_th_ntd=30_000_000,
        limit_up_th=0.085,
        entry_day=0,
        exit_day=6,
        cost_bps=TRANSACTION_COST_BPS
    )

    print("\n===== In-sample Summary =====")
    print(summary_is)

    # ✅ 呼叫新加入的圖表函數：紅色的 DD%
    plot_equity_and_dd_time(
        equity_daily,
        title=f"In-sample Equity Curve (Red DD%)"
    )

    # ✅ 匯出
    out_xlsx = os.path.join(BASE_PATH, "in_sample_ledger_daily.xlsx")
    export_ledger_excel(
        out_xlsx,
        summary_df=summary_is,
        trades_df=trades_is,
        equity_daily=equity_daily,
        positions_daily=positions_daily,
        skipped_df=skipped_is
    )
    print("✅ Saved:", out_xlsx)

    return market_data, capm_df, event_panel, trades_is

if __name__ == "__main__":
    market_data, capm_df, event_panel, trades = main()




###############################################################################
# 進出場時間點優化
###############################################################################
DB_PATH = r"D:\我才不要走量化\Data_Warehouse\kbars_1m_event_window.db"

def _detect_volume_col(conn, table="kbars_1m"):
    """從 SQLite table 欄位中自動偵測 volume 欄位名稱。"""
    cols = pd.read_sql_query(f"PRAGMA table_info({table});", conn)["name"].tolist()
    cols_lower = [c.lower() for c in cols]

    # 常見成交量欄位候選（你可自己再加）
    candidates = [
        "volume", "vol", "qty", "quantity", "trade_volume", "tradevol",
        "v", "size", "shares", "turnover_volume"
    ]

    # 1) 完全相等優先
    for cand in candidates:
        if cand in cols_lower:
            return cols[cols_lower.index(cand)], cols

    # 2) 包含 volume/vol/qty 關鍵字
    keywords = ["volume", "vol", "qty", "quantity"]
    for i, c in enumerate(cols_lower):
        if any(k in c for k in keywords):
            return cols[i], cols

    # 找不到
    return None, cols


def fetch_kbars_tminus1_tplus6_with_volume(
    DB_PATH,
    start_time="09:00:00",
    end_time="13:30:00",
    table="kbars_1m",
):
    """
    從 DB 抓出 t-1 與 t+6 的 1-min kbars，並包含 volume。
    回傳 dataframe：code, event_date, trade_date, ts, close, volume, event_day
    """

    conn = sqlite3.connect(DB_PATH)

    # 偵測 volume 欄位
    vol_col, all_cols = _detect_volume_col(conn, table=table)
    if vol_col is None:
        conn.close()
        raise ValueError(
            f"❌ 在資料表 {table} 找不到成交量欄位。"
            f"\n目前欄位有：{all_cols}"
            f"\n你可以告訴我實際欄位名稱，我再幫你改。"
        )

    # 讀資料（把 vol_col 讀出來，並 alias 成 volume）
    df = pd.read_sql_query(f"""
        SELECT
            code,
            event_date,
            trade_date,
            ts,
            open,
            close,
            {vol_col} AS volume
        FROM {table}
    """, conn)
    conn.close()

    if df.empty:
        print("❌ DB 讀出來是空的")
        return df

    # =========================
    # 清理型別
    # =========================
    df["code"] = df["code"].astype(str).str.strip()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    df = df.dropna(subset=["code","event_date","trade_date","ts","open","close","volume"]).copy()

    # =========================
    # 只留交易時段（避免 13:30 進入下一個 bin）
    # =========================
    start_t = pd.to_datetime(start_time).time()
    end_t   = pd.to_datetime(end_time).time()
    df = df[(df["ts"].dt.time >= start_t) & (df["ts"].dt.time < end_t)].copy()

    # =========================
    # 算 event_day（同你原本）
    # =========================
    days = (
        df[["code","event_date","trade_date"]]
        .drop_duplicates()
        .sort_values(["code","event_date","trade_date"])
        .reset_index(drop=True)
    )
    days["td_rank"] = days.groupby(["code","event_date"]).cumcount()

    event_rank = (
        days[days["trade_date"] == days["event_date"]]
        [["code","event_date","td_rank"]]
        .rename(columns={"td_rank":"event_rank"})
        .drop_duplicates()
    )

    days2 = days.merge(event_rank, on=["code","event_date"], how="inner")
    days2["event_day"] = days2["td_rank"] - days2["event_rank"]

    # 一次抓 t-1 與 t+6
    target_days = (
        days2[days2["event_day"].isin([0, 6])]
        [["code","event_date","trade_date","event_day"]]
        .rename(columns={"trade_date":"target_trade_date"})
        .drop_duplicates()
    )

    out = df.merge(target_days, on=["code","event_date"], how="inner")
    out = out[out["trade_date"] == out["target_trade_date"]].copy()

    # 欄位整理
    out = out[["code","event_date","trade_date","ts","open","close","volume","event_day"]]
    out = out.sort_values(["code","event_date","event_day","ts"]).reset_index(drop=True)

    print("✅ 抓到資料筆數:", len(out))
    print("✅ event_day 分佈:\n", out["event_day"].value_counts().sort_index())
    print(f"✅ volume 欄位使用的是：{vol_col}")

    return out

df_tminus1_tplus6 = fetch_kbars_tminus1_tplus6_with_volume(DB_PATH)

print(df_tminus1_tplus6.head())

#######################################################
df_t1 = df_tminus1_tplus6[df_tminus1_tplus6["event_day"] == 0].copy()
df_t1 = df_t1.sort_values(["code", "event_date", "ts"])


first_open = (
    df_t1
    .groupby(["code", "event_date"], as_index=False)
    .first()[["code", "event_date", "open"]]
    .rename(columns={"open": "first_open"})
)


last_close = (
    df_t1
    .groupby(["code", "event_date"], as_index=False)
    .last()[["code", "event_date", "close"]]
    .rename(columns={"close": "last_close"})
)


oc = first_open.merge(last_close, on=["code", "event_date"], how="inner")

oc["direction"] = np.where(
    oc["last_close"] > oc["first_open"], "Close > Open",
    np.where(
        oc["last_close"] < oc["first_open"], "Open > Close",
        "Equal"
    )
)

count_table = oc["direction"].value_counts().reset_index()
count_table.columns = ["direction", "count"]

count_table["ratio"] = count_table["count"] / count_table["count"].sum()

print(count_table)


plot_df = count_table.set_index("direction").loc[
    ["Close > Open", "Open > Close", "Equal"]
].reset_index()

plt.figure(figsize=(6, 4))
plt.bar(plot_df["direction"], plot_df["ratio"])

plt.ylabel("Proportion")
plt.xlabel("T0 Day Price Direction")
plt.title("T0 Intraday Direction")

for i, r in enumerate(plot_df["ratio"]):
    plt.text(i, r + 0.01, f"{r:.2%}", ha="center", va="bottom")

plt.ylim(0, 1)
plt.tight_layout()
plt.show()

######################################################################
# 實盤交易
######################################################################
###6796晉宏######
api = sj.Shioaji()
api.login(api_key="C9S9Vrcw1jiCkXj3QRR6rJYwfg5MQXBoTzYBprqXFvj7",      
          secret_key="BpauMtipDtzCFWPHnmpjdzk99ansWrapyhUrc2xrAv7F")

contract = api.Contracts.Stocks["6796"]

dates = ["2025-12-08", "2025-12-15"]

all_ticks = []

for d in dates:
    print(f"抓取 6796 | {d}")

    ticks = api.ticks(
        contract=contract,
        date=d,
        query_type=sj.constant.TicksQueryType.RangeTime,
        time_start="09:00:00",
        time_end="13:30:00",  
        timeout=30000
    )

    if ticks is None or len(ticks.ts) == 0:
        print(f"⚠️ {d} 無 tick 資料")
        continue

    df = pd.DataFrame({
        "code": "6796",
        "trade_date": d,
        "ts": pd.to_datetime(ticks.ts),
        "price": ticks.close,
        "volume": ticks.volume,
        "tick_type": ticks.tick_type,
        "bid_price": ticks.bid_price,
        "ask_price": ticks.ask_price,
        "bid_volume": ticks.bid_volume,
        "ask_volume": ticks.ask_volume,
    })

    all_ticks.append(df)

api.logout()

tick_df_6796 = pd.concat(all_ticks, ignore_index=True)
print(tick_df_6796.head())

#####8431 匯鑽科######
x = "8431"
api = sj.Shioaji()
api.login(api_key="C9S9Vrcw1jiCkXj3QRR6rJYwfg5MQXBoTzYBprqXFvj7",      
          secret_key="BpauMtipDtzCFWPHnmpjdzk99ansWrapyhUrc2xrAv7F")

contract = api.Contracts.Stocks[x]

dates = ["2025-12-18","2025-12-26"] #2025/12/29要賣掉

all_ticks = []

for d in dates:
    print(f"抓取 {x} | {d}")

    ticks = api.ticks(
        contract=contract,
        date=d,
        query_type=sj.constant.TicksQueryType.RangeTime,
        time_start="09:00:00",
        time_end="13:30:00",  
        timeout=30000
    )

    if ticks is None or len(ticks.ts) == 0:
        print(f"⚠️ {d} 無 tick 資料")
        continue

    df = pd.DataFrame({
        "code": x,
        "trade_date": d,
        "ts": pd.to_datetime(ticks.ts),
        "price": ticks.close,
        "volume": ticks.volume,
        "tick_type": ticks.tick_type,
        "bid_price": ticks.bid_price,
        "ask_price": ticks.ask_price,
        "bid_volume": ticks.bid_volume,
        "ask_volume": ticks.ask_volume,
    })

    all_ticks.append(df)

api.logout()

tick_df_8431 = pd.concat(all_ticks, ignore_index=True)
print(tick_df_8431.head())

##### 2467 志聖 ######
x = "2467"
api = sj.Shioaji()
api.login(api_key="C9S9Vrcw1jiCkXj3QRR6rJYwfg5MQXBoTzYBprqXFvj7",      
          secret_key="BpauMtipDtzCFWPHnmpjdzk99ansWrapyhUrc2xrAv7F")

contract = api.Contracts.Stocks[x]

dates = ["2025-12-24"] #2025/12/31、1/2、1/5賣掉都可以，我覺得取決於投資人是否想增加隔夜風險?

all_ticks = []

for d in dates:
    print(f"抓取 {x} | {d}")

    ticks = api.ticks(
        contract=contract,
        date=d,
        query_type=sj.constant.TicksQueryType.RangeTime,
        time_start="09:00:00",
        time_end="13:30:00",  
        timeout=30000
    )

    if ticks is None or len(ticks.ts) == 0:
        print(f"⚠️ {d} 無 tick 資料")
        continue

    df = pd.DataFrame({
        "code": x,
        "trade_date": d,
        "ts": pd.to_datetime(ticks.ts),
        "price": ticks.close,
        "volume": ticks.volume,
        "tick_type": ticks.tick_type,
        "bid_price": ticks.bid_price,
        "ask_price": ticks.ask_price,
        "bid_volume": ticks.bid_volume,
        "ask_volume": ticks.ask_volume,
    })

    all_ticks.append(df)

api.logout()

tick_df_2467 = pd.concat(all_ticks, ignore_index=True)
print(tick_df_2467.head())

##################################################################
#樣本外實盤回測 

#進場價格：以詢圈開始前一天交易日 9:30～10:00 的均價買進要買之張數

#出場價格：以詢圈開始後六天交易日 9:00～9:30 的均價賣出
##################################################################
##買晉宏##
from datetime import time

df_1205 = tick_df_6796[
    tick_df_6796["ts"].dt.date
    == pd.to_datetime("2025-12-08").date()
]

df_rr_6796 = df_1205[
    df_1205["ts"].dt.time.between(
        time(11, 30, 0),
        time(12, 00, 0)
    )
]

df_rr_6796["day"] = 0

##買匯鑽科##
df_1205 = tick_df_8431[
    tick_df_8431["ts"].dt.date
    == pd.to_datetime("2025-12-18").date()
]

df_rr_8431 = df_1205[
    df_1205["ts"].dt.time.between(
        time(11, 30, 0),
        time(12, 00, 0)
    )
]
df_rr_8431["day"] = 0

##買志聖##
df_1205 = tick_df_2467[
    tick_df_2467["ts"].dt.date
    == pd.to_datetime("2025-12-24").date()
]

df_rr_2467 = df_1205[
    df_1205["ts"].dt.time.between(
        time(11, 30, 0),
        time(12, 00, 0)
    )
]
df_rr_2467["day"] = 0

##賣晉宏##
df_1205 = tick_df_6796[
    tick_df_6796["ts"].dt.date
    == pd.to_datetime("2025-12-15").date()
]

df_bb_6796 = df_1205[
    df_1205["ts"].dt.time.between(
        time(9, 0, 0),
        time(9, 30, 0)
    )
]
df_bb_6796["day"] = 6

##賣會鑽科
##賣晉宏##
df_1205 = tick_df_8431[
    tick_df_8431["ts"].dt.date
    == pd.to_datetime("2025-12-26").date()
]

df_bb_8431 = df_1205[
    df_1205["ts"].dt.time.between(
        time(9, 0, 0),
        time(9, 30, 0)
    )
]
df_bb_8431["day"] = 6

df_real = pd.concat(
    [
        df_rr_6796,
        df_bb_6796,
        df_rr_8431,
        df_bb_8431,
        df_rr_2467,
    ],
    ignore_index=True
)

print(df_real)

############################################
# 樣本外交易紀錄
###########################################
def calc_vwap_and_lots(df):

    df = df.dropna(subset=["price", "volume"])
    if len(df) == 0:
        return np.nan, 0

    total_lots = df["volume"].sum()
    vwap = (df["price"] * df["volume"]).sum() / total_lots

    return vwap, total_lots

def build_single_trade(
    df_entry,
    code,
    entry_date,
    target_cash=None,
    fixed_lots=None,
    df_exit=None,
    exit_date=None,
):
    # ===== 進場 =====
    entry_price, entry_lots_total = calc_vwap_and_lots(df_entry)

    if pd.isna(entry_price):
        return None

    if fixed_lots is not None:
        buy_lots = fixed_lots
    else:
        buy_lots = int(target_cash // (entry_price * 1000))

    entry_cash = buy_lots * entry_price * 1000

    trade = {
        "code": code,
        "entry_date": entry_date,
        "entry_price": entry_price,
        "buy_lots": buy_lots,
        "entry_cash": entry_cash,
        "entry_window_total_lots": entry_lots_total,
        "entry_lot_ratio": buy_lots / entry_lots_total if entry_lots_total > 0 else np.nan,
    }

    if df_exit is not None:
        exit_price, exit_lots_total = calc_vwap_and_lots(df_exit)

        if not pd.isna(exit_price):
            exit_cash = buy_lots * exit_price * 1000
            trade.update({
                "exit_date": exit_date,
                "exit_price": exit_price,
                "exit_cash": exit_cash,
                "pnl": exit_cash - entry_cash,
                "return": (exit_price - entry_price) / entry_price,
                "exit_window_total_lots": exit_lots_total,
                "status": "CLOSED",
            })
        else:
            trade.update({
                "exit_date": exit_date,
                "status": "OPEN",
            })
    else:
        trade.update({
            "exit_date": exit_date,
            "status": "OPEN",
        })

    return trade

trade_6796 = build_single_trade(
    df_entry=df_rr_6796,
    df_exit=df_bb_6796,
    code="6796",
    entry_date="2025-12-08",
    exit_date="2025-12-15",
    fixed_lots=1
)

trade_8431 = build_single_trade(
    df_entry=df_rr_8431,
    df_exit=df_bb_8431,
    code="8431",
    entry_date="2025-12-18",
    exit_date="2025-12-26",
    target_cash=5_000_000,
)

trade_2467 = build_single_trade(
    df_entry=df_rr_2467,
    code="2467",
    entry_date="2025-12-24",
    target_cash=5_000_000,
    exit_date=None
)

trades_real = pd.DataFrame([
    trade_6796,
    trade_8431,
    trade_2467
]).dropna(how="all")

print(trades_real)

trades_real.to_excel(r"D:\我才不要走量化\可轉換公司債\trades_real.xlsx", index=False)
