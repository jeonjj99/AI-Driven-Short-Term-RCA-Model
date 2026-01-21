import pandas as pd
import numpy as np

# TG
def tg_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/Target/Target Rank 2.0 w. Online.xlsx",
    sheet_name="Nail",
    header=5
):
    df_tg2 = pd.read_excel(path, sheet_name=sheet_name, header=header)

    # Door TY not 0
    df_tg2 = df_tg2[df_tg2['Door TY'] != 0]

    # select Material, Rank, Instock columns
    df_tg2_sel = df_tg2.iloc[:, [0, 82, 86, 90, 59, 58, 57]].copy()

    # change column names
    df_tg2_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]

    # drop NaN in Material
    df_tg2_sel = df_tg2_sel.dropna(subset=['Material'])

    # rank change flags
    df_tg2_sel['RankChange_lastweek'] = np.select(
        [
            df_tg2_sel['Rank_2WeeksAgo'] < df_tg2_sel['Rank_LastWeek'],
            df_tg2_sel['Rank_2WeeksAgo'] == df_tg2_sel['Rank_LastWeek']
        ],
        ['down', 'same'],
        default='up'
    )

    df_tg2_sel['RankChange_thisweek'] = np.select(
        [
            df_tg2_sel['Rank_LastWeek'] < df_tg2_sel['Rank_ThisWeek'],
            df_tg2_sel['Rank_LastWeek'] == df_tg2_sel['Rank_ThisWeek']
        ],
        ['down', 'same'],
        default='up'
    )
    df_tg2_sel['Customer'] = 'TG'
    df_tg2_sel = df_tg2_sel[['Material','Customer', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo','RankChange_thisweek', 'RankChange_lastweek']]

    return df_tg2_sel

# WM
def wm_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/Walmart (US)/WMT Rank.xlsx",
    header=5
):
    df_wm = pd.read_excel(path, header=header)

    # Door TY not 0 + Division Nail
    wm_criteria = (df_wm['Division'] == 'Nail') & (df_wm['Door TY'] != 0)
    df_wm = df_wm[wm_criteria]

    # select Material, Rank, Instock columns
    df_wm_sel = df_wm.iloc[:, [0, 77, 81, 85, 54, 53, 52]].copy()

    # change column names
    df_wm_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]

    df_wm_sel['Customer'] = 'WM'
    # --- Nail-only Rank 재정렬 (This/Last/2WeeksAgo 각각 재순위 부여) ---
    rank_cols = ['Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo']

    # Rank가 숫자가 아닐 수 있으니 안전하게 숫자로 변환
    for c in rank_cols:
        df_wm_sel[c] = pd.to_numeric(df_wm_sel[c], errors='coerce')

    # Nail subset(df_wm_sel) 안에서만 다시 1,2,3... 순위 매기기 (Rank 1이 최고라고 가정)
    for c in rank_cols:
        df_wm_sel[c + '_Nail'] = (
            df_wm_sel[c]
            .rank(method='min', ascending=True)  # 값이 작을수록 상위
            .astype('Int64')
        )

    # --- Rank Change Flag (Nail-only rank 기준) ---
    cond_up_last = (df_wm_sel['Rank_2WeeksAgo_Nail'] > df_wm_sel['Rank_LastWeek_Nail']).fillna(False)
    cond_same_last = (df_wm_sel['Rank_2WeeksAgo_Nail'] == df_wm_sel['Rank_LastWeek_Nail']).fillna(False)

    df_wm_sel['RankChange_lastweek'] = np.where(
        cond_up_last, 'up',
        np.where(cond_same_last, 'same', 'down')
    )

    cond_up_this = (df_wm_sel['Rank_LastWeek_Nail'] > df_wm_sel['Rank_ThisWeek_Nail']).fillna(False)
    cond_same_this = (df_wm_sel['Rank_LastWeek_Nail'] == df_wm_sel['Rank_ThisWeek_Nail']).fillna(False)

    df_wm_sel['RankChange_thisweek'] = np.where(
        cond_up_this, 'up',
        np.where(cond_same_this, 'same', 'down')
    )

    df_wm_sel = df_wm_sel[['Material','Customer', 'Rank_ThisWeek_Nail', 'Rank_LastWeek_Nail', 'Rank_2WeeksAgo_Nail',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo',
        'RankChange_thisweek','RankChange_lastweek']]

    df_wm_sel = df_wm_sel.rename(
        columns=lambda c: c[:-5] if isinstance(c, str) and c.endswith('_Nail') else c)


    return df_wm_sel

# ULTA
def ulta_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/ULTA/2025 ULTA 2.0 Ranking Report.xlsx",
    header=5
):
    df_ulta = pd.read_excel(path, header=header)


    # filtering out Nail PU and Door TY not 0
    df_ulta = df_ulta[(df_ulta['PU']=='Nail') & (df_ulta['Door TY']!=0)]

    # select Material, Rank, Instock columns
    df_ulta_sel = df_ulta.iloc[:, [0,82,86,90,59,58,57]]  

    # change columns names
    df_ulta_sel.columns = ['Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo', 'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo']

    # --- Nail-only Rank 재정렬 (This/Last/2WeeksAgo 각각 재순위 부여) ---
    rank_cols = ['Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo']

    # Rank가 숫자가 아닐 수 있으니 안전하게 숫자로 변환
    for c in rank_cols:
        df_ulta_sel[c] = pd.to_numeric(df_ulta_sel[c], errors='coerce')

    # Nail subset(df_wm_sel) 안에서만 다시 1,2,3... 순위 매기기 (Rank 1이 최고라고 가정)
    for c in rank_cols:
        df_ulta_sel[c + '_Nail'] = (
            df_ulta_sel[c]
            .rank(method='min', ascending=True)  # 값이 작을수록 상위
            .astype('Int64')
        )

    # --- Rank Change Flag (Nail-only rank 기준) ---
    cond_up_last = (df_ulta_sel['Rank_2WeeksAgo_Nail'] > df_ulta_sel['Rank_LastWeek_Nail']).fillna(False)
    cond_same_last = (df_ulta_sel['Rank_2WeeksAgo_Nail'] == df_ulta_sel['Rank_LastWeek_Nail']).fillna(False)

    df_ulta_sel['RankChange_lastweek'] = np.where(
        cond_up_last, 'up',
        np.where(cond_same_last, 'same', 'down')
    )

    cond_up_this = (df_ulta_sel['Rank_LastWeek_Nail'] > df_ulta_sel['Rank_ThisWeek_Nail']).fillna(False)
    cond_same_this = (df_ulta_sel['Rank_LastWeek_Nail'] == df_ulta_sel['Rank_ThisWeek_Nail']).fillna(False)

    df_ulta_sel['RankChange_thisweek'] = np.where(
        cond_up_this, 'up',
        np.where(cond_same_this, 'same', 'down')
    )

    df_ulta_sel['Customer'] = 'ULTA'
    df_ulta_sel = df_ulta_sel[['Material','Customer', 'Rank_ThisWeek_Nail', 'Rank_LastWeek_Nail', 'Rank_2WeeksAgo_Nail',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo',
        'RankChange_thisweek','RankChange_lastweek']]

    df_ulta_sel = df_ulta_sel.rename(
        columns=lambda c: c[:-5] if isinstance(c, str) and c.endswith('_Nail') else c
    )

    return df_ulta_sel

# FD

def fd_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/Family Dollar/Family Dollar Rank 2.0.xlsx",
    sheet_name="Artificial Nails",
    header=5
):
    df_fd = pd.read_excel(path, sheet_name=sheet_name, header=header)

    # filtering out Door TY not 0
    df_fd = df_fd[df_fd['Door TY'] != 0]
    df_fd = df_fd[df_fd['Brand']=='KISS']

    # select Material, Rank, Instock columns
    df_fd_sel = df_fd.iloc[:, [0, 81, 85, 89, 58, 57, 56]].copy()

    # change column names
    df_fd_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]

    df_fd_sel['Customer'] = 'FD'
# --- Nail-only Rank 재정렬 (This/Last/2WeeksAgo 각각 재순위 부여) ---
    rank_cols = ['Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo']

    # Rank가 숫자가 아닐 수 있으니 안전하게 숫자로 변환
    for c in rank_cols:
        df_fd_sel[c] = pd.to_numeric(df_fd_sel[c], errors='coerce')

    # Nail subset(df_wm_sel) 안에서만 다시 1,2,3... 순위 매기기 (Rank 1이 최고라고 가정)
    for c in rank_cols:
        df_fd_sel[c + '_Nail'] = (
            df_fd_sel[c]
            .rank(method='min', ascending=True)  # 값이 작을수록 상위
            .astype('Int64')
        )

    # --- Rank Change Flag (Nail-only rank 기준) ---
    cond_up_last = (df_fd_sel['Rank_2WeeksAgo_Nail'] > df_fd_sel['Rank_LastWeek_Nail']).fillna(False)
    cond_same_last = (df_fd_sel['Rank_2WeeksAgo_Nail'] == df_fd_sel['Rank_LastWeek_Nail']).fillna(False)

    df_fd_sel['RankChange_lastweek'] = np.where(
        cond_up_last, 'up',
        np.where(cond_same_last, 'same', 'down')
    )

    cond_up_this = (df_fd_sel['Rank_LastWeek_Nail'] > df_fd_sel['Rank_ThisWeek_Nail']).fillna(False)
    cond_same_this = (df_fd_sel['Rank_LastWeek_Nail'] == df_fd_sel['Rank_ThisWeek_Nail']).fillna(False)

    df_fd_sel['RankChange_thisweek'] = np.where(
        cond_up_this, 'up',
        np.where(cond_same_this, 'same', 'down')
    )

    df_fd_sel = df_fd_sel[['Material','Customer', 'Rank_ThisWeek_Nail', 'Rank_LastWeek_Nail', 'Rank_2WeeksAgo_Nail',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo',
        'RankChange_thisweek','RankChange_lastweek']]

    df_fd_sel = df_fd_sel.rename(
        columns=lambda c: c[:-5] if isinstance(c, str) and c.endswith('_Nail') else c
    )

    return df_fd_sel

# CVS
def cvs_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/CVS/2026 CVS Fashion Nails Rank.xlsx",
    header=3
):
    df_cvs = pd.read_excel(path, header=header)

    # filtering out Door TY not 0
    df_cvs = df_cvs[df_cvs["Door TY"] != 0]

    # select relevant columns
    df_cvs_sel = df_cvs[['Material.1','Rank.2', 'Rank.3', 'Rank.4', 'Instock %.7', 'Instock %.6', 'Instock %.5']].copy()

    # change column names
    df_cvs_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]

    # sort values by Rank of this week
    df_cvs_sel = df_cvs_sel.sort_values(by='Rank_ThisWeek')

    df_cvs_sel['Customer'] = 'CVS'

    # rank change flags
    df_cvs_sel['RankChange_lastweek'] = np.select(
        [
            df_cvs_sel['Rank_2WeeksAgo'] < df_cvs_sel['Rank_LastWeek'],
            df_cvs_sel['Rank_2WeeksAgo'] == df_cvs_sel['Rank_LastWeek']
        ],
        ['down', 'same'],
        default='up'
    )

    df_cvs_sel['RankChange_thisweek'] = np.select(
        [
            df_cvs_sel['Rank_LastWeek'] < df_cvs_sel['Rank_ThisWeek'],
            df_cvs_sel['Rank_LastWeek'] == df_cvs_sel['Rank_ThisWeek']
        ],
        ['down', 'same'],
        default='up'
    )
    df_cvs_sel = df_cvs_sel[['Material','Customer', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo','RankChange_thisweek','RankChange_lastweek']]
    return df_cvs_sel

# DG

def dg_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/Dollar General/Dollar General Rank 2.0.xlsx",
    sheet_name="Artificial Nails",
    header=5
):
    df_dg = pd.read_excel(path, sheet_name=sheet_name, header=header)

    # filtering out Door TY not 0 + Brand KS
    df_dg = df_dg[df_dg['Door TY'] != 0]
    df_dg = df_dg[df_dg['Brand'].str.upper() == 'KS']

    # select Material, Rank, Instock columns
    df_dg_sel = df_dg.iloc[:, [0, 81, 85, 89, 58, 57, 56]].copy()

    # change column names
    df_dg_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]
    df_dg_sel['Customer'] = 'DG'







   # --- Nail-only Rank 재정렬 (This/Last/2WeeksAgo 각각 재순위 부여) ---
    rank_cols = ['Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo']

    # Rank가 숫자가 아닐 수 있으니 안전하게 숫자로 변환
    for c in rank_cols:
        df_dg_sel[c] = pd.to_numeric(df_dg_sel[c], errors='coerce')

    # Nail subset(df_wm_sel) 안에서만 다시 1,2,3... 순위 매기기 (Rank 1이 최고라고 가정)
    for c in rank_cols:
        df_dg_sel[c + '_Nail'] = (
            df_dg_sel[c]
            .rank(method='min', ascending=True)  # 값이 작을수록 상위
            .astype('Int64')
        )

    # --- Rank Change Flag (Nail-only rank 기준) ---
    cond_up_last = (df_dg_sel['Rank_2WeeksAgo_Nail'] > df_dg_sel['Rank_LastWeek_Nail']).fillna(False)
    cond_same_last = (df_dg_sel['Rank_2WeeksAgo_Nail'] == df_dg_sel['Rank_LastWeek_Nail']).fillna(False)

    df_dg_sel['RankChange_lastweek'] = np.where(
        cond_up_last, 'up',
        np.where(cond_same_last, 'same', 'down')
    )

    cond_up_this = (df_dg_sel['Rank_LastWeek_Nail'] > df_dg_sel['Rank_ThisWeek_Nail']).fillna(False)
    cond_same_this = (df_dg_sel['Rank_LastWeek_Nail'] == df_dg_sel['Rank_ThisWeek_Nail']).fillna(False)

    df_dg_sel['RankChange_thisweek'] = np.where(
        cond_up_this, 'up',
        np.where(cond_same_this, 'same', 'down')
    )

    df_dg_sel = df_dg_sel[['Material','Customer', 'Rank_ThisWeek_Nail', 'Rank_LastWeek_Nail', 'Rank_2WeeksAgo_Nail',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo',
        'RankChange_thisweek','RankChange_lastweek']]

    df_dg_sel = df_dg_sel.rename(
        columns=lambda c: c[:-5] if isinstance(c, str) and c.endswith('_Nail') else c
    )
    return df_dg_sel


# WG
def wg_rank(
    path="C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/WALGREENS/Walgreens Rank 2.0.xlsx",
    header=5
):
    df_wg = pd.read_excel(path, header=header)

    # filtering out Door TY not 0 + PU Nail
    df_wg = df_wg[df_wg['Door TY'] != 0]
    df_wg = df_wg[df_wg['PU'] == 'Nail']

    # select Material, Rank, Instock columns
    df_wg_sel = df_wg.iloc[:, [0, 82, 86, 90, 59, 58, 57]].copy()

    # change column names
    df_wg_sel.columns = [
        'Material', 'Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo'
    ]

    df_wg_sel['Customer'] = 'WG'
    # --- Nail-only Rank 재정렬 (This/Last/2WeeksAgo 각각 재순위 부여) ---
    rank_cols = ['Rank_ThisWeek', 'Rank_LastWeek', 'Rank_2WeeksAgo']

    # Rank가 숫자가 아닐 수 있으니 안전하게 숫자로 변환
    for c in rank_cols:
        df_wg_sel[c] = pd.to_numeric(df_wg_sel[c], errors='coerce')

    # Nail subset(df_wm_sel) 안에서만 다시 1,2,3... 순위 매기기 (Rank 1이 최고라고 가정)
    for c in rank_cols:
        df_wg_sel[c + '_Nail'] = (
            df_wg_sel[c]
            .rank(method='min', ascending=True)  # 값이 작을수록 상위
            .astype('Int64')
        )

    # --- Rank Change Flag (Nail-only rank 기준) ---
    cond_up_last = (df_wg_sel['Rank_2WeeksAgo_Nail'] > df_wg_sel['Rank_LastWeek_Nail']).fillna(False)
    cond_same_last = (df_wg_sel['Rank_2WeeksAgo_Nail'] == df_wg_sel['Rank_LastWeek_Nail']).fillna(False)

    df_wg_sel['RankChange_lastweek'] = np.where(
        cond_up_last, 'up',
        np.where(cond_same_last, 'same', 'down')
    )

    cond_up_this = (df_wg_sel['Rank_LastWeek_Nail'] > df_wg_sel['Rank_ThisWeek_Nail']).fillna(False)
    cond_same_this = (df_wg_sel['Rank_LastWeek_Nail'] == df_wg_sel['Rank_ThisWeek_Nail']).fillna(False)

    df_wg_sel['RankChange_thisweek'] = np.where(
        cond_up_this, 'up',
        np.where(cond_same_this, 'same', 'down')
    )

    df_wg_sel = df_wg_sel[['Material','Customer', 'Rank_ThisWeek_Nail', 'Rank_LastWeek_Nail', 'Rank_2WeeksAgo_Nail',
        'Instock_ThisWeek', 'Instock_LastWeek', 'Instock_2WeeksAgo',
        'RankChange_thisweek','RankChange_lastweek']]

    df_wg_sel = df_wg_sel.rename(
        columns=lambda c: c[:-5] if isinstance(c, str) and c.endswith('_Nail') else c
    )
    return df_wg_sel






def run_rank_batch_simple():
    dfs = [
        tg_rank(),
        wm_rank(),
        ulta_rank(),
        fd_rank(),
        cvs_rank(),
        dg_rank(),
        wg_rank(),
    ]

    df_rank = pd.concat(dfs, ignore_index=True)
    df_date = pd.read_excel("C:/Users/jejeon/OneDrive - Kiss Products Inc/Category and Sales Strategy - 문서/Target/Target Rank 2.0 w. Online.xlsx", header=4)
    
    df_date_sel = df_date.iloc[:, [59, 58, 57]].copy()
    date_list = df_date_sel.columns.tolist()


    ymd_list = [s.split(' ')[0] for s in date_list]

    rename_map = {
        'ThisWeek': ymd_list[0],
        'LastWeek': ymd_list[1],
        '2WeeksAgo': ymd_list[2],
    }

    df_rank_rename = df_rank.rename(columns=lambda c: c
        .replace('ThisWeek', rename_map['ThisWeek'])
        .replace('LastWeek', rename_map['LastWeek'])
        .replace('2WeeksAgo', rename_map['2WeeksAgo'])
    )

    df_rank_rename = df_rank_rename.rename(columns={
        'RankChange_thisweek': f'RankChange_{ymd_list[1]}~{ymd_list[0]}',
        'RankChange_lastweek': f'RankChange_{ymd_list[2]}~{ymd_list[1]}'
    })

    instock_cols = [c for c in df_rank_rename.columns if c.startswith("Instock_")]

    df_rank_rename[instock_cols] = (
        df_rank_rename[instock_cols]
            .apply(pd.to_numeric, errors="coerce")
            .mul(100)
            .round(1)
            .astype(str) + "%"
    )



    
    return df_rank_rename
