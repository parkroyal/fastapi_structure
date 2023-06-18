from datetime import date, timedelta
from pickle import TRUE
from typing import List
import sys
import pandas as pd
import numpy as np
from datetime import date

# print(sys.path)
from package.utils import get_sql, dict_db_server, to_sql
from package.db_helper import get_data
from package.config_loader import config


class DataTP:
    """Parse DataTaipei DB Info"""

    def __init__(self, brand: str = None, detail: str = None) -> None:
        self.brand = brand
        self.detail = detail

    @staticmethod
    def get_brand(server: str, group: str, status: str, echo: bool = False, infinox=False) -> str:
        """
        用server, group, status判斷品牌監管。
        請注意vfsc1/vfsc2的時間性問題。2021/7/4的遷移後才有vfsc2，在此之前的皆為vfsc1
        Args:
            server (str): mt4/mt5的server name
            group (str): login對應的group
            status (str): login對應的status (I品牌才需要，V品牌可直接給予空值)
            echo (bool) : T 對不到的會就會print出來
            infinox (bool) : 所有英諾 直接return英諾 (for mona top 30)
        Returns:
            str: (brand(config['brand']))
        """
        pass


def get_login_detail(logins, schema, start_date=None, end_date=None, by_date=TRUE):

    logins = get_sql(logins, type="int")

    if start_date is not None and end_date is not None:
        extra_date = f"and date >= '{start_date}' and date <= '{end_date}'"
    elif start_date is not None:
        extra_date = f"and date >= '{start_date}'"
    elif end_date is not None:
        extra_date = f"and date <= '{end_date}'"
    else:
        extra_date = ""

    if by_date:
        query = f"""
        select db, login, date,
        sum(m_pnl + s_pnl) as client_pnl, 
        sum(deposit) as deposit, 
        sum(withdrawal) as withdrawal, 
        sum(deposit +withdrawal) as net_deposit, 
        sum(m_volume + s_volume)*2 as volume 
        from detail 
        where login in ({logins}) {extra_date} 
        group by db, login, date
        """
    else:
        query = f"""
        select db, login, 
        sum(m_pnl + s_pnl) as client_pnl, 
        sum(deposit) as deposit, 
        sum(withdrawal) as withdrawal, 
        sum(deposit +withdrawal) as net_deposit, 
        sum(m_volume + s_volume)*2 as volume 
        from detail 
        where login in ({logins}) {extra_date} 
        group by db, login 
        """

    df = get_data(query, db="datatp", schema=schema)
    mapping = dict_db_server("detail")
    df["server"] = df.db.map(mapping)
    return df


def get_db_detail(schema, start_date=None, end_date=None, by_date=False):

    if start_date is not None and end_date is not None:
        extra_date = f" date >= '{start_date}' and date <= '{end_date}'"
    elif start_date is not None:
        extra_date = f" date >= '{start_date}'"
    elif end_date is not None:
        extra_date = f" date <= '{end_date}'"
    else:
        extra_date = ""

    if by_date:
        query = f"""
        select db, login, date,
        sum(m_pnl + s_pnl) as client_pnl, 
        sum(deposit) as deposit, 
        sum(withdrawal) as withdrawal, 
        sum(deposit +withdrawal) as net_deposit, 
        sum(m_volume + s_volume)*2 as volume 
        from detail 
        where {extra_date} 
        group by db, login, date
        """
    else:
        query = f"""
        select db, login, 
        sum(m_pnl + s_pnl) as client_pnl, 
        sum(deposit) as deposit, 
        sum(withdrawal) as withdrawal, 
        sum(deposit +withdrawal) as net_deposit, 
        sum(m_volume + s_volume)*2 as volume 
        from detail 
        where {extra_date} 
        group by db, login  
        """

    df = get_data(query, db="datatp", schema=schema)
    mapping = dict_db_server("detail")
    df["server"] = df.db.map(mapping)
    df = df.drop(columns=["db"])
    return df


def get_volume_ticket(login: List, start_date: str or date = None, end_date=None, brand=None):

    logins = get_sql(login, type="int")

    if start_date is not None and end_date is not None:
        extra_date = f"and date >= '{start_date}' and date <= '{end_date}'"
    elif start_date is not None:
        extra_date = f"and date >= '{start_date}'"
    elif end_date is not None:
        extra_date = f"and date <= '{end_date}'"
    else:
        extra_date = ""

    if brand == "vantage":
        query = f"""
        select ticket, db, login, `group`, symbol, type, lots, vol * 2 as vol, date 
        from volume 
        where login in ({logins}) {extra_date} and transfer <> 'T' 
        union all 
        select deal as ticket, db, login, `group`, symbol, lower(type) as type, lots, vol * 2 as vol, date 
        from `au_mt5_volume` 
        where login in ({logins}) {extra_date} and transfer <> 'T' 
        """

    df = get_data(query, db="datatp", schema="allothers")
    mapping = dict_db_server("allothers", "volume")
    df["server"] = df.db.map(mapping)
    df = df[df.server.isin(config["brand"][brand]["server"])]
    return df


def get_login_volume(login: List, start_date: str or date = None, end_date=None, brand=None, by_date=True):

    logins = get_sql(login, type="int")

    if start_date is not None and end_date is not None:
        extra_date = f"and date >= '{start_date}' and date <= '{end_date}'"
    elif start_date is not None:
        extra_date = f"and date >= '{start_date}'"
    elif end_date is not None:
        extra_date = f"and date <= '{end_date}'"
    else:
        extra_date = ""

    if by_date:
        query = f"""
        select date, db, login, symbol, sum(lots) as lots, sum(vol * 2) as vol, count(*) as trades_num  
        from volume 
        where login in ({logins}) {extra_date} and transfer <> 'T' group by db, login, symbol, date
        union all 
        select date, db, login, symbol, sum(lots) as lots, sum(vol * 2) as vol, count(*) as trades_num  
        from `au_mt5_volume` 
        where login in ({logins}) {extra_date} and transfer <> 'T' group by db, login, symbol, date 
        """
    else:
        query = f"""
        select db, login, symbol, sum(lots) as lots, sum(vol * 2) as vol, count(*) as trades_num  
        from volume 
        where login in ({logins}) {extra_date} and transfer <> 'T' group by db, login, symbol  
        union all 
        select db, login, symbol, sum(lots) as lots, sum(vol * 2) as vol, count(*) as trades_num  
        from `au_mt5_volume` 
        where login in ({logins}) {extra_date} and transfer <> 'T' group by db, login, symbol 
        """

    df = get_data(query, db="datatp", schema="allothers")
    mapping = dict_db_server("allothers", "volume")
    df["server"] = df.db.map(mapping)
    df = df[df.server.isin(config["brand"][brand]["server"])]
    return df


# Sales Name
# User.daily_accounts_sales
def get_user_daily_accounts_sales(
    search_type="login",
    search_date=date.today(),
    logins: List = None,
    user=None,
    sales_name=None,
    sales_group=None,
    crm_server=None,
    rebate_account=None,
    cpa=None,
):
    """Return daily logins superior
    columns : [crm_server, user_id, server, login, rebate_account, cpa, ib_name, level_one_rebate_account, level_one_ib_id, level_one_ib_name]
    Keyword arguments:
    search_date : datetime.date object"""

    # extra sql
    if logins is not None:
        extra_logins = get_sql(logins)
        extra_logins = f" and login in ({extra_logins})"
    else:
        extra_logins = ""

    if user is not None:
        extra_users = get_sql(user)
        extra_users = f" and user_id in ({extra_users})"
    else:
        extra_users = ""

    if rebate_account is not None:
        extra_rebate_account = get_sql(rebate_account)
        extra_rebate_account = f" and rebate_account in ({extra_rebate_account})"
    else:
        extra_rebate_account = ""

    if cpa is not None:
        extra_cpa = get_sql(cpa)
        extra_cpa = f" and cpa in ({extra_cpa})"
    else:
        extra_cpa = ""

    if crm_server is not None:
        extra_crm_server = get_sql(crm_server, type="str")
        extra_crm_server = f" and crm_server in ({extra_crm_server})"
    else:
        extra_crm_server = ""

    if sales_name is not None:
        extra_sales_name = "|".join(sales_name)
        extra_sales_name = f" and sales_name in '{extra_sales_name}'"
    else:
        extra_sales_name = ""

    if sales_group is not None:
        extra_sales_group = "|".join(sales_group)
        extra_sales_group = f" and sales_group in '{extra_sales_group}'"
    else:
        extra_sales_group = ""
    if search_type == "login":
        query = f"""
        select crm_server, user_id, `server`, login, sales_name, sales_group, rebate_account, cpa, ib_name, level_one_rebate_account, level_one_ib_id, level_one_ib_name 
        from user.daily_accounts_sales 
        where date = '{search_date}' {extra_logins} {extra_rebate_account} {extra_crm_server} {extra_sales_name} {extra_sales_group} 
        """
    else:
        # daily_user_info
        query = f"""
        select crm_server, user_id, sales_name, sales_group, cpa ,ib_id, ib_name, level_one_ib_id 
        from user.daily_user_info 
        where date = '{search_date}' {extra_users} {extra_rebate_account} {extra_crm_server} {extra_cpa} {extra_sales_name} {extra_sales_group} 
        """

    df = get_data(query, db="datatp", schema="user")
    if len(df.index) == 0:
        search_date = search_date - timedelta(days=1)
        return get_user_daily_accounts_sales(
            search_type=search_type,
            search_date=search_date,
            logins=logins,
            user=user,
            sales_name=sales_name,
            sales_group=sales_group,
            crm_server=crm_server,
            rebate_account=rebate_account,
            cpa=cpa,
        )
    else:
        if "ib_id" in df.columns:
            df = df.assign(client_type=lambda x: np.where(x.cpa != "0", "CPA", np.where(x.ib_id != 0, "IB", "Retails")))
        elif "rebate_account" in df.columns:
            df = df.assign(client_type=lambda x: np.where(x.cpa != "0", "CPA", np.where(x.rebate_account != 0, "IB", "Retails")))

        # df.cpa = df.cpa.astype(int)
        return df


def get_ftd_info(servers: List, type="user", user_df: pd.DataFrame = None) -> pd.DataFrame:
    """Return login/user ftd info
    key args:
    server : mt4/mt5 servers
    type : login/user
    user_df: pd.DataFrame: crm_server, user_id, server, login
    """

    servers = get_sql(servers, type="str")
    query = f"""
    select server, login, currency, first_deposit_time, first_deposit_amount 
    from accounts_info_test 
    where server in ({servers})
    """
    df = get_data(query, "datatp", "user")

    if type == "user":
        user_df = user_df[["crm_server", "user_id", "server", "login"]]
        # 合併crm user_id, login資訊
        df = df.merge(user_df, how="left", on=["server", "login"])
        # 抓取客戶第一筆入金資訊
        # df = df[df['PRICE'] == df.groupby('ORIGIN')['PRICE'].transform('min')]
        df = df[df.first_deposit_time == df.groupby(["crm_server", "user_id"])["first_deposit_time"].transform("min")]
        # drop duplicate
        df = df.sort_values("first_deposit_time").drop_duplicates(["crm_server", "user_id"])
    return df


def get_login_equity(search_date: date = date.today()) -> pd.DataFrame:
    """Return login equity/ credit/ balance
    Parameters:
    search_date:date, default: date.today()
    Returns:
    pd.DataFrame:
    """

    query = f"""
    select db, login, 'v' as vi, mnt_sub, active, `group`, right(`group`, 3) as currency, equity as equity_raw, equity_in_usd as equity, credit_in_usd as credit, balance as balance_raw 
    from brad.bbook_v_1_2021 where date = '{search_date}' 
    union all 
    select db, login, 'v' as vi, mnt_sub, active,`group`, right(`group`, 3) as currency, equity as equity_raw, equity_in_usd as equity, credit_in_usd as credit, balance as balance_raw 
    from brad.bbook_v_2_2021 where date = '{search_date}' 
    union all 
    select db, login, 'i' as vi, mnt_sub, active,`group`, right(`group`, 3) as currency, equity as equity_raw, equity_in_usd as equity, credit_in_usd as credit, balance as balance_raw 
    from brad.bbook_i_1_2021 where date = '{search_date}' 
    union all 
    select db, login, 'i' as vi, mnt_sub, active,`group`, right(`group`, 3) as currency, equity as equity_raw, equity_in_usd as equity, credit_in_usd as credit, balance as balance_raw 
    from brad.bbook_i_2_2021 where date = '{search_date}'
    """
    df = get_data(query, "datatp", "brad")

    if df.shape[0] == 0:
        search_date = search_date - timedelta(days=1)
        return get_login_equity(search_date)
    else:
        return df
    # print(query)


def get_rate(
    currency: List = ["CAD", "CHF", "JPY", "NZD", "SGD", "AUD", "GBP", "EUR", "HKD", "TRY", "CNH", "NOK", "SEK"], search_date: date = date.today()
):

    """
    抓取匯率

    return df : {
        date,
        symbol
        currency,
        rate
    }
    """
    pairs = [x + "USD" for x in list(set(currency))]  # .extend(a)
    pairs.extend(["USD" + x for x in list(set(currency))])
    us_pairs = get_sql(pairs, type="str")

    query_new = f"""
    select distinct date, symbol, mid 
    from au_eod_prices_new 
    where date >= '{search_date}' and symbol in ({us_pairs})
    """

    query_old = f"""
    select distinct date, symbol, mid  
    from au_eod_prices  
    where date >= '{search_date}' and symbol in ({us_pairs})
    """

    query_allothers = f"""
    select distinct date, symbol, mid 
    from eod  
    where date >= '{search_date}' and symbol in ({us_pairs})
    """

    df = get_data(query_new, db="mt4", schema="eod_price")
    df1 = get_data(query_old, db="mt4", schema="eod_price")
    df2 = get_data(query_allothers, db="datatp", schema="allothers")
    # change datatime and string to date object
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date
    df1["date"] = pd.to_datetime(df1["date"], format="%Y-%m-%d").dt.date
    df2["date"] = pd.to_datetime(df2["date"], format="%Y-%m-%d").dt.date

    # 每日取平均
    df = df.groupby(["date", "symbol"], as_index=False)["mid"].agg("mean").assign(table="au_eod_prices_new")
    df1 = df1.groupby(["date", "symbol"], as_index=False)["mid"].agg("mean").assign(table="au_eod_prices")
    df2 = df2.groupby(["date", "symbol"], as_index=False)["mid"].agg("mean").assign(table="allothers")

    # 合併取優先
    df_summary = pd.concat([df, df1, df2])
    df_summary = df_summary.groupby(["date", "symbol"]).head(1)
    if df_summary.shape[0] == 0:
        return print("nothing")
    else:
        # df1.dtypes
        # USD/USC
        dates = pd.date_range(start=search_date, end=date.today(), freq="d").to_series().dt.date.to_frame(name="date")

        symbols = df_summary.symbol.tolist()
        symbols.extend(["USD", "USC"])
        symbols_df = pd.DataFrame({"symbol": symbols})
        # product
        test = dates.assign(key=1).merge(symbols_df.assign(key=1), on="key").drop("key", axis=1)
        test = test.merge(df_summary, on=["date", "symbol"], how="left")

        test["value"] = test.sort_values("date", ascending=False).groupby(["symbol"])["mid"].bfill()

        test = test.assign(
            rate=lambda x: np.where(
                x.symbol == "USD", 1, np.where(x.symbol.str[0:3] == "USD", 1 / x.value, np.where(x.symbol == "USC", 0.01, x.value))
            )
        )
        test = test.assign(
            currency=lambda x: np.where(x.symbol == "USD", "USD", np.where(x.symbol == "USC", "USC", x.symbol.str.replace("USD", "", regex=False)))
        )
        test = test.drop_duplicates()
        return test[["date", "currency", "symbol", "rate"]]


def get_test_list():
    # 測試帳號 from datataipei
    query = "select db, login, is_test from test_list where is_test = 1"
    df = get_data(query, "datatp", "allothers")
    mapping = dict_db_server(schema="allothers", table="allaccount")
    df["server"] = df.db.map(mapping)
    df = df.drop(columns=["db"])
    df = df.drop_duplicates()
    return df


def get_rate_new(currency: List = ["AUD"], search_date: date = date.today()):

    """
    抓取匯率
    Parameters:
    currency: List
    search_date:date, default: date.today()
    Returns
    pd.DataFrame: {
        date: date,
        currency: str,
        symbol: str,
        rate: float
    }
    """
    usd_currency_pairs = [x + "USD" for x in list(set(currency))]
    other_currency_pairs = ["USD" + x for x in list(set(currency))]
    usd_currency_pairs.extend(other_currency_pairs)
    pairs = to_sql(usd_currency_pairs, type=str)

    query = f"""
    select trade_date as date, price_time, symbol, mid 
    from tb_symbol_price  
    where trade_date >= '{search_date}' and symbol in ({pairs}) 
    """

    df = get_data(query, db="mt4", schema="eod_price")
    # change datatime and string to date object
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date
    # 依照報價順序取當日最晚報價
    df = df.sort_values(["price_time"], ascending=False).groupby(["date", "symbol"]).head(1)

    if df.shape[0] == 0:
        return print("nothing")
    else:
        # df1.dtypes
        # USD/USC
        dates = pd.date_range(start=search_date, end=date.today(), freq="d").to_series().dt.date.to_frame(name="date")

        symbols = df.symbol.tolist()
        symbols.extend(["USD", "USC"])
        symbols_df = pd.DataFrame({"symbol": symbols})
        # product
        df_product = dates.assign(key=1).merge(symbols_df.assign(key=1), on="key").drop("key", axis=1)
        df_product = df_product.merge(df, on=["date", "symbol"], how="left")

        df_product["value"] = df_product.sort_values("date", ascending=False).groupby(["symbol"])["mid"].bfill()
        df_product["value"] = df_product.sort_values("date", ascending=False).groupby(["symbol"])["mid"].ffill()
        df_product = df_product.assign(
            rate=lambda x: np.where(
                x.symbol == "USD", 1, np.where(x.symbol.str[0:3] == "USD", 1 / x.value, np.where(x.symbol == "USC", 0.01, x.value))
            )
        )
        df_product = df_product.assign(
            currency=lambda x: np.where(x.symbol == "USD", "USD", np.where(x.symbol == "USC", "USC", x.symbol.str.replace("USD", "", regex=False)))
        )
        df_product = df_product.drop_duplicates()
        return df_product[["date", "currency", "symbol", "rate"]]
