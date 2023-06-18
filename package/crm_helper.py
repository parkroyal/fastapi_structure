from typing import List
from package.utils import get_sql, nowDate
from package.db_helper import get_data
from enum import Enum
import numpy as np
import pandas as pd

class CRM:
    """
    Parse CRM(au_crm/pug_crm/vt_crm/moneta_crm/opl_crm) Info.
    """
    def __init__(self, db) -> None:
        self.db = db

# 枚舉類
class Vantage_DataSourceEnum(Enum):
    enfukreport = 2
    mxtreport = 3
    enfaureport = 5
    MT5_VFX_Live = 8
    enfau2report = 11
    enfuk2report = 12
    enfau3report = 14
    enfuk3report = 15
    vuk2report = 17
    enfau4report = 200
    enfuk4report = 201
    unknown = 0


class WebsiteUserTypeEnum(Enum):
    DEMO = 1
    INDIVIDUAL = 2
    MASTER_JOINT = 3
    VICE_JOINT = 4
    IB_INDIVIDUAL = 5
    INNER = 6
    EXPERIENCE_DEMO = 7 # LEADS
    LEADS = 8
    REPEAT_LEADS = 9
    IMPORT_LEADS = 10
    IRREGULAR_LEADS = 11
    COMPANY = 13
    SMSF_DEMO = 14
    VICE_LEADS = 15
    IB_COMPANY = 16
    MIGRATION_LEADS = 17
    MIGRATION_DEMO = 18
    CONFLICT_ACCOUNT = 19
    SALES_NOTES = 20

def get_server(datasource_id, crm = "au_crm"):
    
    if crm == "au_crm":
        try:
            return Vantage_DataSourceEnum(int(datasource_id)).name
        except:
            return datasource_id

def get_user_type(datasource_id, crm = "au_crm"):
    
    if crm == "au_crm":
        try:
            return WebsiteUserTypeEnum(int(datasource_id)).name
        except:
            return datasource_id

def get_crm_info(crm:str = "au_crm", crm_server:List = ["vfsc_business", "vfsc2_business"], user:List = None, login:List = None, type = 'search'):
    "Return users info from crm"

    if type == 'search':
        
        if user is not None:
            user_ids = get_sql(list(set(user)), type = 'int')
            query = f"""
            select database() as crm_server, user.*, country_name_en, mt4_user_create_time, login, login_del, is_archive, mt4_datasource_id, mt4_group, register_date 
            from 
            (SELECT id AS user_id, is_del as user_del, real_name, email, phone_num, create_time, country_code, website_user_type FROM tb_user where id in ({user_ids}) and is_del = 0) user  
            left join 
            (SELECT user_id, mt4_account as login, create_time as mt4_user_create_time, register_date, is_del as login_del, is_archive, mt4_group , mt4_datasource_id FROM tb_account_mt4 where mt4_account <> 1) logins  
            on user.user_id = logins.user_id 
            left join 
            (SELECT id, country_name_en from tb_country) country  
            on user.country_code = country.id
            """
            df =  get_data(query, crm, crm_server)
        elif login is not None:
            logins = get_sql(list(set(login)), type = 'int')
            query = f"""
            select database() as crm_server, b.*, street, suburb, state, country_name_en, a.login, a.login_del, a.is_archive, mt4_group, a.register_date, a.mt4_datasource_id 
            from 
            (SELECT user_id, mt4_account as login, is_del as login_del, is_archive,  mt4_group, mt4_datasource_id, register_date FROM tb_account_mt4 where mt4_account in ({logins}) and mt4_account <> 1) a 
            join 
            (SELECT id AS user_id, create_time, is_del as user_del, real_name, email, phone_num, country_code, website_user_type FROM tb_user where is_del = 0) b 
            on a.user_id = b.user_id 
            left join 
            (SELECT user_id, street, suburb, state FROM tb_user_extends) ex 
            on a.user_id = ex.user_id 
            left join 
            (SELECT id, country_name_en from tb_country) country  
            on b.country_code = country.id
            """
            df =  get_data(query, crm, crm_server)
        else:
            query = """
            select database() as crm_server, u.*, country_name_en, street, suburb, state, place_of_birth, cpa from 
            (select id as user_id, email, phone_num, real_name, country_code, website_user_type,create_time from tb_user where is_del=0) u 
            left join 
            (select id, country_name_en from tb_country) c 
            on u.country_code = c.id 
            left join 
            (select user_id, street, suburb, state, place_of_birth, cpa from tb_user_extends) ex 
            on u.user_id = ex.user_id 
            """
            df =  get_data(query, crm, crm_server)
            
        df = df.dropna(subset=['user_id'])
  
    elif type == "all":
        query = """ 
            select database() as crm_server, user.*, country_name_en, mt4_user_create_time, login, login_del, is_archive, mt4_datasource_id, mt4_group, register_date 
            from 
            (SELECT id AS user_id, is_del as user_del, real_name, email, phone_num, create_time, country_code, website_user_type FROM tb_user where is_del = 0) user  
            left join 
            (SELECT user_id, mt4_account as login, create_time as mt4_user_create_time, register_date, is_del as login_del, is_archive, mt4_group , mt4_datasource_id FROM tb_account_mt4 where mt4_account <> 1) logins  
            on user.user_id = logins.user_id 
            left join 
            (SELECT id, country_name_en from tb_country) country  
            on user.country_code = country.id
        """
        df =  get_data(query, crm, crm_server)
        df = df.dropna(subset=['user_id'])

    # mapping server 
    if "mt4_datasource_id" in df.columns:
        df["server"] = df["mt4_datasource_id"].apply(get_server, crm = crm)
    # mapping user type
    if "website_user_type" in df.columns:
        df["website_user_type_en"] = df["website_user_type"].apply(get_user_type, crm = crm)  
    
    return df 


def get_commission_rules(ibs, database, schema, search_type = 'all'):
    """抓取反傭規則 from CRM 
    Parameters: 
    ibs:list, 欲尋找之反傭帳號
    database:crm庫. ['au_crm', 'moneta_crm_new', 'vt_crm', 'pu_crm']
    search_type: all: 尋找所有反傭鍊 else: 特定IB
    Returns:
    pd.DataFrame:  
    """
    
    ibs = get_sql(ibs, type = 'int')

    if search_type == 'all':
        query = f"""
        select r.id, r.agentAccount as rebate_account, c.agentAccount as agent_account, rule_name, mt4_group, settle_mode, coefficient 
        from 
        tb_commision_coefficient c, 
        tb_commission_rules r 
        where 
        c.agentAccount in ({ibs}) and 
        c.rule_id = r.id and 
        c.`status` = 0 and 
        r.is_del = 0 
        """
    else: 
        query = f"""
        select r.id, r.agentAccount as rebate_account, c.agentAccount as agent_account, rule_name, mt4_group, settle_mode, coefficient 
        from 
        tb_commision_coefficient c, 
        tb_commission_rules r 
        where 
        r.agentAccount in ({ibs}) and 
        c.rule_id = r.id and 
        c.`status` = 0 and 
        r.is_del = 0 
        """
    df = get_data(query, db = database, schema = schema)
    conditions = [(df.rule_name.str.contains('FX', case=False)),
                (df.rule_name.str.contains('CRYPTO', case=False)),
                (df.rule_name.str.contains('Index\\.r', case=False)),
                (df.rule_name.str.contains('Index', case=False)),
                (df.rule_name.str.contains('OIL', case=False)),
                (df.rule_name.str.contains('XAU', case=False)),
                (df.rule_name.str.contains('XAG', case=False)),
                (df.rule_name.str.contains('Future', case=False)),
                (df.rule_name.str.contains("CFD's Asia", case=False))]
    results = ['Forex', 'Cryptocurrency', 'Index.r', 'Index', 'Oil', 'Gold', 'Silver', 'Future', "CFD's Asia"]
    df['symbol_type'] = np.select(conditions, results, default=np.nan)
    df['coefficient'] = df['coefficient'].astype(float)
    

    return df 

# 抓取Deposit / Channel
def get_deposit_with_channel(start_time, end_time, db, schema):
    query = f""" 
    SELECT p.user_id, p.mt4_account as login,p.payment_type as payment_type_num ,p.payment_channel as payment_channel_num,p.Actual_amount as deposit,p.Currency as ccy, a.mt4_datasource_id, date(p.update_time) as date 
    FROM  
    tb_payment_deposit as p,
    tb_account_mt4 as a 
    where p.user_id = a.user_id 
    and p.mt4_account = a.mt4_account 
    and p.`status` = 5 
    and p.update_time between '{start_time}' and '{end_time}' 
    and p.is_del != 1 
    """
    df = get_data(query, db , schema)
    deposit_channel = pd.read_excel(r"\\192.168.1.20\Rmc\Data Analysis\Dropbox\power_bi\sales_report\payment_type_channel_new.xlsx")
    if "mt4_datasource_id" in df.columns:
        df["server"] = df["mt4_datasource_id"].apply(get_server, crm = db)
    df = df.merge(deposit_channel, on = ['payment_channel_num', 'payment_type_num'], how = 'left')
    return df

def get_failed_deposit(start_time, end_time, db, schema):
    query = f""" 
    SELECT p.user_id, p.mt4_account as login,p.payment_type as payment_type_num ,p.payment_channel as payment_channel_num,p.Actual_amount as deposit,p.Currency as ccy, a.mt4_datasource_id, processed_notes, date(p.update_time) as date 
    FROM  
    tb_payment_deposit as p,
    tb_account_mt4 as a 
    where p.user_id = a.user_id 
    and p.mt4_account = a.mt4_account 
    and p.`status` <> 5 
    and p.update_time between '{start_time}' and '{end_time}' 
    and p.is_del != 1 
    """
    df = get_data(query, db , schema)
    deposit_channel = pd.read_excel(r"\\192.168.1.20\Rmc\Data Analysis\Dropbox\power_bi\sales_report\payment_type_channel_new.xlsx")
    if "mt4_datasource_id" in df.columns:
        df["server"] = df["mt4_datasource_id"].apply(get_server, crm = db)
    df = df.merge(deposit_channel, on = ['payment_channel_num', 'payment_type_num'], how = 'left')
    return df
    