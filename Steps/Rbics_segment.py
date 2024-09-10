from pathlib import Path
from behave import *
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
import json
import logging
from decimal import Decimal
from pandas.testing import assert_frame_equal
from pandas.testing import assert_series_equal
from urllib.parse import urlencode
import pyodbc

from logging import basicConfig, DEBUG, info, debug

basicConfig(level=DEBUG, format="%(levelname)s %(asctime)s : %(message)s")

def con_decimal(values):
    try:
        return format(float(values),'.10f')
    except:
        return values


@given('Fetch the rbics_segment data from {rbics_segment}')
def fetch_data(context,rbics_segment:str):
    with open(Path(rbics_segment)) as fp:
        context.rbics_segment_query = fp.read()

@given('Fetch the data from api {base_url}')
def base_url(context,base_url:str):
    context.base_url = base_url

@when('Read the Following parameters')
def with_params(context):
    context.url_with_params = (f"{context.base_url}?{urlencode(dict(context.table))}")


@then('Make api requests')
def api_request(context):
    context.api_request = requests.get(context.url_with_params,verify=False)
    if context.api_request.status_code == 200:
        context.api_request_api = context.api_request.content
        logging.info(context.api_request.status_code)
    else:
        raise ValueError("NO response")

@then('save the api response')
def save_response(context):
    context.Api_Data=pd.read_csv(BytesIO(context.api_request_api),na_filter=False,sep='|')



@when('Fetch the Data for rbics_segment from DB at {Cutoff_date}')
def rbics_data(context,Cutoff_date:str):
    
    k = "','".join(context.Api_Data['internalId'].to_list())
    k= "'"+k+"'"
    context.rbics_segment_DB = context.rbics_segment_query.format(datetime.strptime(Cutoff_date,'%Y-%m-%d').strftime('%Y%m%d'),k)
    # context.Brutus_SIDEXT = ImportFromDatabase("brutus1.bat.ci.dom","SIDDB")
    # context.Brutus_SIDEXT.query = context.rbics_segment_DB
    query=context.rbics_segment_DB
    connection_string = "Driver=SQL Server;Server=brutus1.bat.ci.dom;Database=SIDDB;Trusted_Connection=yes"
    cnxn = pyodbc.connect(connection_string)
    df = pd.read_sql(query, cnxn)
    context.rbics_segment_Data = df.copy()
    # dff = context.rbics_segment_Data.groupby(['stoxxId','rbics2L6Id'])['revenuePercent'].sum().reset_index()
    # context.rbics_segment_Data['rbics2Segment'] = context.rbics_segment_Data['rbics2Segment'].apply(lambda x: x.strip())
   
    rbics_stoxxid_group_df=context.rbics_segment_Data.groupby(['stoxxId'])
    qa_data=[]
    for stoxxId,df in rbics_stoxxid_group_df:
        # if stoxxId[0] =='453456':
        #     # import ipdb;ipdb.set_trace()
        #     print("ewfgdj")

        df.revenuePercent=df.revenuePercent.astype(float).round(10)
        # df.revenuePercent = df.revenuePercent.apply(lambda x :f'{x:.10f}'.rstrip('0').rstrip('.'))
        # rbics_segment_list=df.apply(lambda x:f"""'{x["rbics2Segment"]}':{float(x["revenuePercent"])}{(10-len(format(x["revenuePercent"],'10f').split(".")[1]))*"0"}""",axis=1).to_list()

        rbics_segment_list=df.apply(lambda x:f"""'{x["rbics2Segment"]}':{format(float(x["revenuePercent"]),".10f")}""",axis=1).to_list()

        rbics_l6_incl_segment=[]
        for i in df['rbics2L6Id'].sort_values().unique():
            rbics_segment_list=df[df['rbics2L6Id']==i].apply(lambda x:f"""'{x["rbics2Segment"]}':{format(float(x["revenuePercent"]),".10f")}""",axis=1).to_list()
            # rbics_segment_list=df.apply(lambda x:f"""'{x["rbics2Segment"]}':{format(float(x["revenuePercent"]),".10f")}""",axis=1).to_list()
            internal_id_segment=df[df['rbics2L6Id']==i]['rbics2Segment'].tolist()
            # trailing_zeros=(10-len(format(df[df['rbics2L6Id']==i]["revenuePercent"].sum().round(10),"f").split('.')[1]))*'0'
            rbics_l6_string=f"'{i}':"+"{"+f"""segments :[{', '.join([i for i in rbics_segment_list for x in internal_id_segment if f"'{x}'" in i])}],total_revenue :{format(df[df['rbics2L6Id']==i]["revenuePercent"].sum().round(10),".10f")}"""+"}"
            rbics_l6_incl_segment.append(rbics_l6_string)
        rbics_l6_incl_segment="{"+",".join(rbics_l6_incl_segment)+"}"
        rbics2L6Id_revenue_sum_dict =df.astype({"rbics2L6Id":str,"revenuePercent":float}).groupby('rbics2L6Id')['revenuePercent'].sum().round(10).reset_index()
        # rbicsi6_sum_list=rbics2L6Id_revenue_sum_dict.apply(lambda x:f"""'{x["rbics2L6Id"].strip()}':{float(x["revenuePercent"])}{(10-len(format(x["revenuePercent"],'f').split(".")[1]))*"0"}""",axis=1).to_list()
        rbicsi6_sum_list=rbics2L6Id_revenue_sum_dict.apply(lambda x:f"""'{x["rbics2L6Id"].strip()}':{format(float(x["revenuePercent"]),".10f")}""",axis=1).to_list()
        rbics_i6="{"+",".join(rbicsi6_sum_list)+"}"
        qa_data.append([stoxxId[0],rbics_i6,rbics_l6_incl_segment.replace(" ","")])
    context.qa_data=pd.DataFrame(qa_data,columns=["internalId","rbics_l6","rbics_l6_incl_segment"])
    context.merge_df = context.qa_data.merge(context.Api_Data[["internalId","rbics_l6","rbics_l6_incl_segment"]],on='internalId',how='left',suffixes = ('_QA','_Dev'))
    assert_series_equal(context.merge_df['rbics_l6_QA'],context.merge_df['rbics_l6_Dev'],check_names=False)
    context.merge_df['rbics_l6_incl_segment_Dev'] = context.merge_df['rbics_l6_incl_segment_Dev'].str.replace(" ","")
    context.merge_df['rbics_l6_incl_segment_QA'] = context.merge_df['rbics_l6_incl_segment_QA'].str.replace(" ","")
    context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: x.replace("[","{"))
    context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: x.replace("]","}"))
    context.merge_df['rbics_l6_incl_segment_QA'] =context.merge_df['rbics_l6_incl_segment_QA'].apply(lambda x: x.replace("[","{"))
    context.merge_df['rbics_l6_incl_segment_QA'] =context.merge_df['rbics_l6_incl_segment_QA'].apply(lambda x: x.replace("]","}"))
    context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: x.replace("'",'"'))
    context.merge_df['rbics_l6_incl_segment_QA'] =context.merge_df['rbics_l6_incl_segment_QA'].apply(lambda x: x.replace("'",'"'))
    context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: x.replace("total_revenue",'"total_revenue"'))
    context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: x.replace("segments",'"segments"'))
    context.merge_df['rbics_l6_incl_segment_QA'] =context.merge_df['rbics_l6_incl_segment_QA'].apply(lambda x: x.replace("total_revenue",'"total_revenue"'))
    context.merge_df['rbics_l6_incl_segment_QA'] =context.merge_df['rbics_l6_incl_segment_QA'].apply(lambda x: x.replace("segments",'"segments"'))
    # context.merge_df['rbics_l6_incl_segment_Dev'] =context.merge_df['rbics_l6_incl_segment_Dev'].apply(lambda x: dict(x))
    # context.merge_df["rbics_l6_incl_segment_QA_sorted"]= context.merge_df["rbics_l6_incl_segment_QA"].apply(lambda x: ''.join(sorted(x)))
    # context.merge_df["rbics_l6_incl_segment_Dev_sorted"]= context.merge_df["rbics_l6_incl_segment_Dev"].apply(lambda x: ''.join(sorted(x)))
    # # context.merge_df = context.merge_df[context.merge_df['internalId']!='524918']
    # context.merge_df = context.merge_df[context.merge_df['internalId']!='612370']
    # context.merge_df = context.merge_df[context.merge_df['internalId']!='001016']

    context.merge_df['check']=(context.merge_df["rbics_l6_incl_segment_QA"]==context.merge_df["rbics_l6_incl_segment_Dev"])
    # context.merge_df['check']=(context.merge_df["rbics_l6_incl_segment_QA_sorted"]==context.merge_df["rbics_l6_incl_segment_Dev_sorted"])
    # assert_series_equal(context.merge_df['rbics_l6_incl_segment_QA_sorted'],context.merge_df['rbics_l6_incl_segment_Dev_sorted'],check_names=False)
    assert_series_equal(context.merge_df['rbics_l6_incl_segment_QA'],context.merge_df['rbics_l6_incl_segment_Dev'],check_names=False)
   
