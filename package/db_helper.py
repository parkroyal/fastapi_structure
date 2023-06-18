import json
import sqlalchemy
import pandas as pd
import numpy as np
import mysql.connector
import urllib.parse
from package.logging_helper import logger, log_exception
from typing import List
from package.config_loader import config

archive_table_mapping = pd.DataFrame(
    {"archive_server": config["all_servers"]["archive_servers"].keys(), "mt4_server": config["all_servers"]["archive_servers"].values()}
)
# 由於歸檔庫vtm1report, kcmreport, mxtreport歸類在mxt歸檔table, 故刪除vtm1reporot, kcmreport.
archivedb_to_server = archive_table_mapping[~archive_table_mapping.mt4_server.isin(["vtm1report", "kcmreport"])]


@log_exception
def get_report_engine(db, schema=None, db_type="mysql_db"):
    """Return a sqlalchemy connector."""
    global config
    user = config[db_type][db]["username"]
    password = config[db_type][db]["password"]
    host = config[db_type][db]["host"]
    port = config[db_type][db]["port"]

    # 針對會對Url連線造成影響(@)的特殊符號進行編碼
    password = urllib.parse.quote_plus(password)

    if db_type == "mysql_db":
        connector = f"mysql+mysqldb://{user}:{password}@{host}:{port}"
    elif db_type == "maria_db":
        connector = f"mysql+mysqldb://{user}:{password}@{host}:{port}"
    elif db_type == "mssql_db":
        connector = f"mssql+pyodbc://{user}:{password}@{host}:{port}"

    if schema is not None and db_type != "mssql_db":
        connector = connector + (f"/{schema}?charset=utf8mb4")

    if schema is not None and db_type == "mssql_db":
        connector = connector + (f"/{schema}?driver=SQL+Server")

    return sqlalchemy.create_engine(sqlalchemy.engine.url.make_url(name_or_url=connector))


@log_exception
def get_data(query, db, schema=None, db_type="mysql_db", **kwargs) -> pd.DataFrame:
    """Return a pandas DataFrame."""
    query = query.format(**kwargs)
    if type(schema) is list:
        df = []
        for schema_name in schema:
            logger.info(f"{schema_name}")
            conn = get_report_engine(db, schema_name, db_type)
            temp = pd.read_sql(query, conn)
            # temp = cx.read_sql(conn = conn, query = query)
            conn.dispose()
            df.append(temp)
        df = pd.concat(df)
    elif type(schema) is str:
        logger.info(f"{schema}")
        conn = get_report_engine(db, schema, db_type)
        df = pd.read_sql(query, conn)
        # df = cx.read_sql(conn = conn, query = query)
        conn.dispose()

    if "server" in df.columns:
        conditions = [(df.server.eq("mt5_vfx_live")), (df.server.eq("mt5_vfx_live2")), (df.server.eq("mt5_pug_live"))]
        results = ["MT5_VFX_Live", "MT5_VFX_Live2", "MT5_PUG_Live"]
        df["server"] = np.select(conditions, results, default=df.server)
    return df


@log_exception
def write_table(df: pd.DataFrame, schema: str, table: str, col: List, db="datatp_manager", db_type="mysql_db"):
    # conn = get_report_engine(db, schema, db_type)
    global config
    if len(df) < 1:
        return "empty data"

    df = df.where(pd.notnull(df), None)
    df = df.replace(np.nan, None)

    try:
        col = [f"`{x}`" for x in col]
        conn = mysql.connector.connect(
            host=config[db_type][db]["host"],
            user=config[db_type][db]["username"],
            password=config[db_type][db]["password"],
            database=schema,
            auth_plugin="mysql_native_password",
        )
        cursor = conn.cursor(buffered=True)

        s = ["%s"] * len(col)  # col數量
        query = f"""
            REPLACE INTO `{schema}`.`{table.replace('`','')}`
                ({', '.join(col)})
            VALUES
                ({', '.join(s)})
        """
        print(query)
        print(df.columns)
        if len(df) > 1:
            values = df.values.tolist()
            for i in range(1 + len(values) // 10000):
                cursor.executemany(query, values[i * 10000 : (i + 1) * 10000])
                conn.commit()
        else:
            for val in df.values:
                val = tuple(val)
                cursor.execute(query, val)
            conn.commit()

        res = f"success : insert {schema}.{table}"
        logger.info(f"success : insert {schema}.{table}")
    except Exception as error:
        conn.rollback()
        res = f"failed database : insert {schema}.{table}"
        logger.exception(error)
    finally:
        cursor.close()
        conn.close()
        print(res)
        if "failed database" in res:
            logger.error(f"failed : insert {schema}.{table}")
            logger.exception(error)
            raise Exception(error)
        return res


@log_exception
def get_sync_data(
    query_mt4: str = None,
    query_mt5: str = None,
    query_archive: str = None,
    mt4_servers: List = None,
    mt5_servers: List = None,
    do_archive: bool = False,
    **kwargs,
):
    df = []
    # print("get_sync_data")
    # MT4
    if mt4_servers is not None:
        for server in mt4_servers:
            # print(f"{server}")
            query = query_mt4.format(**kwargs)
            temp = get_data(query, db="mt4", schema=server)
            df.append(temp)
    # Archive
    if do_archive:
        archive_servers = archive_table_mapping[archive_table_mapping.mt4_server.isin(mt4_servers)]["archive_server"]
        archive_servers = archive_servers.unique().tolist()
        temp = get_archive_data(query_archive, archive_servers, **kwargs)
        df.append(temp)
    # MT5
    if mt5_servers is not None:
        for server in mt5_servers:
            query = query_mt5.format(**kwargs)
            temp = get_data(query, db="mt5_with_archive", schema=server, db_type="maria_db")
            df.append(temp)
    df = pd.concat(df)
    df = df.drop_duplicates()
    return df


@log_exception
def get_archive_data(query_archive: str = None, archive_servers: List = None, **kwargs):
    """return data from archive_new database"""
    df = []
    for archive_server in archive_servers:

        query = query_archive.format(archive_server=archive_server, **kwargs)
        temp = get_data(query, db="archive_new", schema="archive_db")
        df.append(temp)

    df = pd.concat(df)

    if "archive_server" in df.columns and "group" in df.columns:
        df = df.merge(archivedb_to_server, on="archive_server", how="left")
        df = df.assign(
            server=lambda x: np.where(
                x.group.str.contains("MXT"),
                "mxtreport",
                np.where(x.group.str.contains("VT") & x.archive_server.eq("mxt"), "vtm1report", x.mt4_server),
            )
        )
        df = df.drop(columns=["mt4_server", "archive_server"])
    if "currency" in df.columns and "group" in df.columns:
        # 因新歸檔庫user表currency遺失，遺失的currency使用group替代
        df = df.assign(currency=lambda x: np.where(pd.isna(x.currency) | x.currency.eq(""), x.group.str[-3:], x.currency))

    return df
