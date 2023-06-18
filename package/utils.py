# %%
from package.logging_helper import log_exception, logger
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Any, Union
import pandas as pd
import os
import numpy as np

from package.db_helper import get_data

# %%
# 今日日期
if date.today().weekday() == 0:
    nowDate = date.today() - timedelta(days=3)
elif date.today().weekday() == 6:
    nowDate = date.today() - timedelta(days=2)
else:
    nowDate = date.today() - timedelta(days=1)

# 昨日日期
if nowDate.weekday() == 0:
    nowDate_lag = nowDate - timedelta(days=3)
elif nowDate.weekday() == 6:
    nowDate_lag = date.today() - timedelta(days=2)
else:
    nowDate_lag = nowDate - timedelta(days=1)

# symbol_category = pd.read_excel(r"\\192.168.1.20\Rmc\Data Analysis\Dropbox\股票產品\Symble_Table\Symbol Table.xlsx", sheet_name="Symbol")
# symbol_category = symbol_category.rename(columns={"Symbol": "symbol", "fixed_symbol": "clean_symbol", "Type": "symbol_type"})
# symbol_category = symbol_category[["symbol", "clean_symbol", "symbol_type"]]
# symbol_category = symbol_category.drop_duplicates()

# %%
def get_sql(list_x: List, type="int") -> str:

    list_x = list(set(list_x))
    # 排除nan值
    if type == "str":
        string = ",".join([f"'{str(x)}'" for x in list_x if pd.isnull(x) == False])
    elif type == "int":
        string = ",".join([str(x) for x in list_x if pd.isnull(x) == False])
    return string


# %%
def to_sql(list_x: List, type=Union[str, int]) -> str:
    """
    Convert list to SQL Query format in where clause.
    Args:
        type: List
    Returns:
        A string by list merged by commas.
    """
    list_x = list(set(list_x))  # 去重
    if type == str:
        string = ",".join([f"'{str(x)}'" for x in list_x if pd.isnull(x) == False])  # 排除None值
    elif type == int:
        string = ",".join([str(x) for x in list_x if pd.isnull(x) == False])
    return string


# 日期物件
class Date:
    def __init__(self, search_date: date = None) -> None:
        # 最新日報日期
        if search_date:
            self.nowDate = search_date
        else:
            if date.today().isoweekday() == 1:
                self.nowDate = date.today() - timedelta(days=3)
            elif date.today().isoweekday() == 7:
                self.nowDate = date.today() - timedelta(days=2)
            else:
                self.nowDate = date.today() - timedelta(days=1)

    def get_week_date(self, type="current_week"):
        if type == "current":
            # 找禮拜一
            start_date = self.nowDate
            while start_date.isoweekday() != 1:
                start_date -= timedelta(days=1)
            end_date = start_date + timedelta(days=6)
        else:
            start_date = self.nowDate - timedelta(days=7)
            while start_date.isoweekday() != 1:
                start_date -= timedelta(days=1)
            end_date = start_date + timedelta(days=6)

        return start_date, end_date

    def get_month_date(self, type="current"):
        if type == "current":
            end_date = self.nowDate + relativedelta(months=1)
            end_date = end_date.replace(day=1) - timedelta(days=1)
            # end_date = self.nowDate.replace(month=self.nowDate.month + relativedelta(month=1), day=1) - timedelta(days = 1)
            # end_date = date.today().replace(month=date.today().month + 1, day=1) - timedelta(days = 1)
            start_date = end_date.replace(day=1)
        else:
            end_date = self.nowDate.replace(day=1) - timedelta(days=1)
            # end_date = date.today().replace(day = 1) - timedelta(days = 1)
            start_date = end_date.replace(day=1)

        return start_date, end_date

    def get_year_date(self, type="current"):
        if type == "current":
            start_date = self.nowDate.replace(month=self.nowDate.month + 1) - timedelta(days=1)
            end_date = self.nowDate.replace(day=1)
        else:
            end_date = self.nowDate.replace(day=1) - timedelta(days=1)
            start_date = end_date.replace(day=1)

        return start_date, end_date

    def get_month_time(self, type="current"):

        base = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_time = (base - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
        start_time = base - relativedelta(months=1)

        return start_time, end_time

    def get_date_time(self):

        start_time = datetime.combine(self.nowDate, datetime.min.time())
        end_time = start_time.replace(hour=23, minute=59, second=59, microsecond=999999)

        return start_time, end_time

    @staticmethod
    def get_month_date_new(date: date, type="current"):
        if type == "current":
            end_date = date.replace(month=date.month + 1, day=1) - timedelta(days=1)
            # end_date = date.today().replace(month=date.today().month + 1, day=1) - timedelta(days = 1)
            start_date = end_date.replace(day=1)
        else:
            end_date = date.replace(day=1) - timedelta(days=1)
            # end_date = date.today().replace(day = 1) - timedelta(days = 1)
            start_date = end_date.replace(day=1)

        return start_date, end_date


# %%
def write_to_excel(filename, df, output_folder, column=None, name="Report", date=date.today(), format=False):

    # output_folder = check_folder(output_folder)
    if len(df) > 1000000:
        file = []
        step = 700000
        start = 0
        i = 0
        while start < len(df):
            file.append(write_to_excel(filename + "_" + str(i), df[start : start + step]))
            i += 1
            start = start + step

        return file
    else:
        if date:
            file = os.path.join(output_folder, filename + "_" + date.strftime("%Y-%m-%d") + ".xlsx")
        else:
            file = os.path.join(output_folder, filename + ".xlsx")
        writer = pd.ExcelWriter(file, engine="xlsxwriter")
        if format:
            if column is None:
                df.to_excel(writer, index=False, encoding="utf-8", sheet_name=name, float_format="%.2f")
            else:
                df.to_excel(writer, columns=column, index=False, encoding="utf-8", sheet_name=name, float_format="%.2f")
        else:
            if column is None:
                df.to_excel(writer, index=False, encoding="utf-8", sheet_name=name)
            else:
                df = df.filter(items=column)
                df.to_excel(writer, columns=column, index=False, encoding="utf-8", sheet_name=name)
        writer.save()
        writer.close()

        return file


# %%
def write_multiple_df_to_excel(filename: str, data: dict, output_folder: str, date=date.today()):
    # output_folder = check_folder(output_folder)

    file = os.path.join(output_folder, filename + date.strftime("%Y-%m-%d") + ".xlsx")
    writer = pd.ExcelWriter(file, engine="xlsxwriter")

    for sheet, row in data.items():
        if "column" in row:
            _column = [x for x in row["column"] if x in row["data"].columns]
            row["data"].to_excel(writer, columns=_column, index=False, encoding="utf-8", sheet_name=sheet)
        else:
            row["data"].to_excel(writer, index=False, encoding="utf-8", sheet_name=sheet)

    writer.save()
    writer.close()

    return file


# %%
def get_symbol_info():

    query = """
    select * from symbol_table
    """
    df = get_data(query, "datatp", "symbol")
    df = df[["server", "symbol", "clean_symbol", "CCY", "symbol_type", "Digits", "ContractSize"]]
    conditions = [(df.server.eq("MT5 BHS")), (df.server.eq("MT5 PUG")), (df.server.eq("MT5")), (df.server.eq("VT1")), (df.server.eq("VT2"))]
    results = ["mt5_1", "MT5_PUG_Live", "MT5_VFX_Live", "vtm1report", "vtm2report"]
    df["server"] = np.select(conditions, results, default=df.server)
    df = df.assign(
        symbol_type=lambda x: np.where(
            x.symbol.str.contains("[\\.]+r$") & x.symbol_type.eq("Index"),
            "Index.r",
            np.where(x.symbol.str.contains("Nikkei225"), "CFD's Asia", x.symbol_type),
        )
    )
    df = df.drop_duplicates()
    return df


# %%
def dict_db_server(schema: str, table: str = None):
    excel = "//192.168.1.20/Rmc/Data Analysis/Dropbox/Model_AUTO/package/db_switch.xlsx"
    df_mapping = pd.read_excel(excel)

    if schema == "allothers" and table == "allaccount":
        df_mapping = df_mapping.drop_duplicates(subset=["allaccount"])
        mapping = dict(zip(df_mapping.allaccount, df_mapping.mt45))

    if schema == "allothers" and table == "volume":
        df_mapping = df_mapping.drop_duplicates(subset=["volume_db"])
        mapping = dict(zip(df_mapping.volume_db, df_mapping.mt45))

    if schema == "detail":
        df_mapping = df_mapping.drop_duplicates(subset=["detail"])
        mapping = dict(zip(df_mapping.detail, df_mapping.mt45))

    if schema == "symbol":
        df_mapping = df_mapping.drop_duplicates(subset=["symbol_db"])
        mapping = dict(zip(df_mapping.symbol_db, df_mapping.mt45))

    return mapping


def conv_dict_type(key_to: Union[str, int], value_to: Union[str, int], d: dict):
    d = {key_to(k): value_to(v) for k, v in d.items()}
    return d


def get_symbol_categpory():

    query = """
    select distinct symbol, clean_symbol, symbol_type from symbol_table
    """
    df = get_data(query, "datatp", "symbol")

    return df


def country_transfer(name: str) -> str:
    """
    統一國家名稱
    """
    if name in ["Bolivia", "Bolivia (plurinational State Of)", "Estado Plurinacional De Bolivia", "Bosnia & Herzegovina"]:  # 'Bosnia And Herzegovina'
        return "Bolivia"
    if name in [
        "Congo (the)",
        "Congo, Democratic Republic Of The",
        "Congo, Republic Of The",
        "Congo (kinshasa)",
        "Congo (the Democratic Republic Of The)",
        "Congo (brazzaville)",
        "Congo, Democratic Republic Of T",
    ]:
        return "Congo"
    if name in ["Cote D'ivoire", "Côte D'ivoire", "C?te D'Ivoire", "C?te D'ivoire", "Ivory Coast"]:
        return "Côte D'ivoire"
    if name in ["Ger", "Germany"]:
        return "Germany"
    if name in ["Hong Kong", "Hong Kong S.a.r.", "Hongkong"]:
        return "Hong Kong"
    if name in ["Macao", "Macao S.a.r."]:
        return "Macao"
    if name in ["Ina", "Indonesia"]:
        return "Indonesia"
    if name in ["Korea (the Republic Of)", "Korea, Republic Of", "Korea, South", "Republic Of Korea", "Korea"]:
        return "South Korea"
    if name in ["North Korea", "Democratic People's Republic Of Korea"]:
        return "North Korea"
    if name in ["Lao People's Democratic Republic", "Laos"]:
        return "Laos"
    if name in ["Lat", "Latvia"]:
        return "Latvia"
    if name in ["Mgl", "Mongolia"]:
        return "Mongolia"
    if name in ["Ned", "Netherlands"]:
        return "Netherlands"
    if name in ["Nep", "Nepal"]:
        return "Nepal"
    if name in ["Ngr", "Nigeria"]:
        return "Nigeria"
    if name in [
        "Palestinian Territory, Occupied",
        "Palestine",
        "Palestinian Authority",
        "Palestinian Territories",
        "Palestinian Territory",
        "State Of Palestine",
        "Palestine, State Of",
    ]:
        return "Palestine"
    if name in ["Phi", "Philippines", "Philippine"]:
        return "Philippines"
    if name in ["Rom", "Romania"]:
        return "Romania"
    if name in ["Russian Federation", "Russia", "Soviet Union"]:
        return "Russia"
    if name in ["Saint Vincent And The Grenadine", "Saint Vincent And The Grenadines", "St Vincent And The Grenadines"]:
        return "Saint Vincent And The Grenadines"
    if name in ["Rsa", "South Africa"]:
        return "South Africa"
    if name in ["Sri", "Sri Lanka"]:
        return "Sri Lanka"
    if name in ["Tanzania, United Republic Of", "United Republic Of Tanzania", "Tanzania, United Republic Of"]:
        return "Tanzania"
    if name in ["U.a.e.", "Uae", " United Arab Emirates", "United Arab Emirates"]:
        return "United Arab Emirates"
    if name in [
        "United Kingdom",
        "United Kingdom Of Great Britain And Northern Ireland",
        "Uk",
        "The United Kingdom Of Great Bri",
        "United Kingdon",
        "United Kngdom",
    ]:
        return "United Kingdom"
    if name in ["United States Of America", "United States", "America"]:
        return "United States"
    if name in ["Venezuela", "Venezuela (bolivarian Republic Of)"]:
        return "Venezuela"
    if name in ["Viet Nam", "Vietnam", "Vietna"]:
        return "Vietnam"
    if name in [
        "Virgin Islands (british)",
        "Virgin Islands, British",
        "British Virgin Islands",
        "Virgin Islands (brit",
        "Virgin Islands, British",
        "British Virgin Island",
        "British Virgin Isles",
    ]:
        return "British Virgin Islands"
    if name in ["Virgin Islands, Us", "Virgin Islands", "Virgin Islands, US", "Vgb", "Vir", "United States Virgin Islands"]:
        return "United States Virgin Islands"
    if name in ["中国", "People's Republic Of China"]:
        return "China"
    if name in ["澳大利亚", "Sydney"]:
        return "Australia"
    if name in ["Aland Islands", "Åland Islands"]:
        return "Åland Islands"
    if name in ["Moldova (the Republic Of)", "Moldova", "Republic Of Moldova"]:
        return "Moldova"
    if name in ["Iran (islamic Republic Of)", "Iran"]:
        return "Iran"
    if name in ["Brl", "Bra", "Brasil", "Brasil, Latam", "Brazil & Row", "Bra, Latam"]:  # BRL/ bra
        return "Brazil"
    if name in ["Brasil, Angola", "Angola", "Ago"]:
        return "Angola"
    if name in ["Twn", "Taiwan, Province Of China"]:
        return "Taiwan"
    if name in ["Tha", "Thai", "Thaialnd"]:
        return "Thailand"
    if name in ["Arg"]:  # Arg
        return "Argentina"
    if name in ["Swaziland", "Eswatini"]:
        return "Swaziland"
    if name in ["Spain And Latam", "Spa"]:
        return "Spain"
    if name in ["Egy"]:
        return "Egypt"
    if name in ["Czechia", "Czech Republic"]:
        return "Czechia"
    if name in ["Macedonia", "North Macedonia", "Republic Of North Macedonia"]:
        return "Macedonia"
    if name in ["Saint Barthélemy", "Saint Barthelemy"]:
        return "Saint Barthelemy"
    if name in ["Syria", "Syrian Arab Republic"]:
        return "Syria"
    if name in ["Cabo Verde", "Cape Verde"]:
        return "Cape Verde"
    if name in ["Slovakia", "Slovak Republic", "Slovak"]:
        return "Slovakia"
    if name in ["Sin"]:
        return "Singapore"
    if name in ["Nan", "Test"]:
        return None
    return name


def country_clean(x: str) -> str:
    """
    判斷國家字串(縮寫/代碼...) 返回 國家全名
    """
    if x == "":
        return None
    if x == "not_found":
        return x
    if x is None:
        return x
    x = x.title()
    x = x.replace(" (the)", "").replace("(the)", "")
    x = country_transfer(x)
    return x


@log_exception
def if_else(expression_result: Any, true_value: Any, false_value: Any, missing_value: np.nan = np.nan) -> Any:
    """
    Repeats logic from R function if_else
    :param expression_result: result of expressions, can be True, False, NA
    :param true_value: return its value if expression_result equals True
    :param false_value: return its value if expression_result equals False
    :param missing_value: return its value if expression_result equals NA
    :return: string value of true_value, false_value, missing_value
    """
    try:
        if pd.isna(expression_result):
            return missing_value
        elif expression_result:
            return true_value
        else:
            return false_value
    except Exception as e:
        logger.error("expression_result: {}".format(expression_result))
        logger.error("true_value: {}".format(true_value))
        logger.error("false_value: {}".format(false_value))
        raise e
