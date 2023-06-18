from typing import List
from datetime import date
import pandas as pd
import numpy as np
import sqlalchemy
import urllib3
from package.config_loader import config


def get_sql(list_x: List, type='int') -> str:

    list_x = list(set(list_x))
    # 排除nan值
    if type == 'str':
        string = ",".join(
            [f"'{str(x)}'" for x in list_x if pd.isnull(x) == False])
    elif type == 'int':
        string = ",".join([str(x) for x in list_x if pd.isnull(x) == False])
    return string


def get_report_engine(db, schema=None, db_type="mysql_db"):
    """Return a sqlalchemy connector."""

    user = config[db_type][db]["username"]
    password = config[db_type][db]["password"]
    host = config[db_type][db]["host"]
    port = config[db_type][db]["port"]

    # 針對會對Url連線造成影響(@)的特殊符號進行編碼
    password = urllib3.parse.quote_plus(password)

    if db_type == "mysql_db":
        connector = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    elif db_type == "maria_db":
        connector = f"mysql+pymysql://{user}:{password}@{host}:{port}"
    elif db_type == "mssql_db":
        connector = f"mssql+pyodbc://{user}:{password}@{host}:{port}"

    if schema is not None and db_type != 'mssql_db':
        connector = connector + (f"/{schema}?charset=utf8mb4")

    if schema is not None and db_type == "mssql_db":
        connector = connector + (f"/{schema}?driver=SQL+Server")

    # print(sqlalchemy.engine.url.make_url(name_or_url=connector))
    # print(f'{connector}')
    # return sqlalchemy.create_engine(urllib.parse.quote_plus(string=connector))

    return sqlalchemy.create_engine(sqlalchemy.engine.url.make_url(name_or_url=connector))


def get_data(query, db, schema=None, db_type="mysql_db") -> pd.DataFrame:
    """Return a pandas DataFrame."""
    if type(schema) is list:
        df = []
        for schema_name in schema:
            # logger.info(f"{schema_name}")
            conn = get_report_engine(db, schema_name, db_type)
            temp = pd.read_sql(query, conn)
            conn.dispose()
            df.append(temp)
        df = pd.concat(df)
    elif type(schema) is str:
        # logger.info(f"{schema}")
        conn = get_report_engine(db, schema, db_type)
        df = pd.read_sql(query, conn)
        conn.dispose()

    if "server" in df.columns:
        conditions = [(df.server.eq('mt5_vfx_live')),
                      (df.server.eq('mt5_vfx_live2')),
                      (df.server.eq('mt5_pug_live'))]
        results = ['MT5_VFX_Live', 'MT5_VFX_Live2', 'MT5_PUG_Live']
        df['server'] = np.select(conditions, results, default=df.server)
    return df


def get_rate(currency: List = ['CAD', 'CHF', 'JPY', 'NZD', 'SGD', 'AUD', 'GBP', 'EUR', 'HKD', 'TRY', 'CNH', 'NOK', 'SEK'], search_date: date = date.today()):

    pairs = [x+"USD" for x in list(set(currency))]  # .extend(a)
    pairs.extend(["USD"+x for x in list(set(currency))])
    us_pairs = get_sql(pairs, type='str')

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

    df = get_data(query_new, db='mt4', schema='eod_price')
    df1 = get_data(query_old, db='mt4', schema='eod_price')
    df2 = get_data(query_allothers, db='datatp', schema='allothers')
    # change datatime and string to date object
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d").dt.date
    df1['date'] = pd.to_datetime(df1['date'], format="%Y-%m-%d").dt.date
    df2['date'] = pd.to_datetime(df2['date'], format="%Y-%m-%d").dt.date

    # 每日取平均
    df = (df.groupby(['date', 'symbol'], as_index=False)['mid']
            .agg('mean')
            .assign(table='au_eod_prices_new')
          )
    df1 = (df1.groupby(['date', 'symbol'], as_index=False)['mid']
           .agg('mean')
           .assign(table='au_eod_prices')
           )
    df2 = (df2.groupby(['date', 'symbol'], as_index=False)['mid']
           .agg('mean')
           .assign(table='allothers')
           )

    # 合併取優先
    df_summary = pd.concat([df, df1, df2])
    df_summary = df_summary.groupby(['date', 'symbol']).head(1)
    if df_summary.shape[0] == 0:
        return print('nothing')
    else:
        # df1.dtypes
        # USD/USC
        dates = (pd.date_range(start=search_date, end=date.today(), freq='d')
                 .to_series()
                 .dt.date
                 .to_frame(name="date")
                 )

        symbols = df_summary.symbol.tolist()
        symbols.extend(['USD', 'USC'])
        symbols_df = pd.DataFrame({"symbol": symbols})
        # product
        test = (dates.assign(key=1)
                .merge(symbols_df.assign(key=1), on='key')
                .drop('key', axis=1))
        test = test.merge(df_summary, on=['date', 'symbol'], how='left')

        test['value'] = (test.sort_values('date', ascending=False)
                         .groupby(['symbol'])['mid'].bfill())

        test = test.assign(rate=lambda x: np.where(x.symbol == 'USD', 1,
                                                   np.where(x.symbol.str[0:3] == 'USD',  1/x.value,
                                                            np.where(x.symbol == 'USC', 0.01, x.value))))
        test = test.assign(CCY=lambda x: np.where(x.symbol == 'USD', 'USD',
                                                  np.where(x.symbol == 'USC', 'USC',
                                                           x.symbol.str.replace('USD', '', regex=False))))
        test = test.drop_duplicates()
        return test[['date', 'CCY', 'rate']]
