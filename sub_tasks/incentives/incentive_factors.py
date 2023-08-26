import sys
sys.path.append(".")

#import libraries
import json
import psycopg2
import requests
import pygsheets
import io
import pandas as pd
from datetime import date
from airflow.models import Variable
from pangres import upsert, DocsExampleTable
from sqlalchemy import create_engine, text, VARCHAR
from pandas.io.json._normalize import nested_to_record 


from sub_tasks.data.connect import (pg_execute, pg_fetch_all, engine) 

def create_nps_summary():

    query = """
    truncate mabawa_mviews.branch_nps_summary;
    insert into mabawa_mviews.branch_nps_summary
    SELECT pk_date, branch_code, responses, promoters, passives, detractors, surveys_sent
    FROM mabawa_mviews.v_branch_nps_summary;
    """

    query = pg_execute(query)


def create_ins_rejections():

    query = """
    truncate mabawa_mviews.branch_rejections;
    insert into mabawa_mviews.branch_rejections
    SELECT doc_entry, odsc_date, odsc_status, odsc_remarks, doc_no, 
    ods_createdon, ods_outlet, ods_creator, user_name
    FROM mabawa_mviews.v_branch_rejections;
    """

    query = pg_execute(query)


def create_ins_rejection_summary():

    query = """
    truncate mabawa_mviews.branch_rejections_summary;
    insert into mabawa_mviews.branch_rejections_summary
    SELECT pk_date, branch_code, rejections, ins_orders
    FROM mabawa_mviews.v_branch_rejections_summary;
    """

    query = pg_execute(query)

def create_sop():
    
    query = """
    truncate mabawa_mviews.dim_branch_sop;
    insert into mabawa_mviews.dim_branch_sop
    SELECT sop_date, audit_person, branch, branch_code, srm, rm, sop, consider_for_incentive, no_of_times, times, 
    sap_count, perc_non_compliance, order_number, name_of_non_compliant_staff, remark_on_morning_cleaning, 
    time_of_check, designation, name_of_branch_staff, time_of_late_reporting, allocated_lunch_timings, time_gone_for_lunch, 
    time_of_unauthorized_exit, time_of_returning, time_of_late_opening, time_of_early_closure
    FROM mabawa_mviews.v_dim_branch_sop;
    """

    query = pg_execute(query)


def create_sop_summary():
    
    query = """
    truncate mabawa_mviews.branch_sop_summary;
    insert into mabawa_mviews.branch_sop_summary
    SELECT pk_date, branch_code, all_sop, sop_count, cust_count
    FROM mabawa_mviews.v_branch_sop_summary;
    """

    query = pg_execute(query)


def create_ins_feedback_conv():

    query = """
    truncate mabawa_mviews.ins_feedback_conversion;
    insert into mabawa_mviews.ins_feedback_conversion
    SELECT r, doc_entry, rq_sent, rq_date, req_time, rq_sent_timestamp, fb_rcd, fb_date, fb_time, fb_rcd_timestamp, 
    doc_no, ods_createdon, ods_creator, user_name, cust_code, ods_status, ods_outlet, creation_date, draft_orderno, order_canceled, same_converted, 
    diff_orderno, diff_order_canceled, diff_creation_date, cust_vendor_code, diff_createdon, ods_normal_repair_order, ods_insurance_order, diff_converted, converted
    FROM mabawa_mviews.v_ins_feedback_conversion2;
    """
    query = pg_execute(query)


def create_ins_direct_conv():
    
    query = """
    truncate mabawa_mviews.ins_direct_conversion;
    insert into mabawa_mviews.ins_direct_conversion
    SELECT r, doc_entry, dir_status, dir_type, dir_date, dir_time, dir_timestamp, doc_no, ods_createdon, ods_creator,
    user_name, cust_code, ods_status, ods_outlet, creation_date, draft_orderno, order_canceled, same_converted, diff_orderno, 
    diff_order_canceled, diff_creation_date, cust_vendor_code, diff_createdon, ods_normal_repair_order, ods_insurance_order, 
    diff_converted, converted
    FROM mabawa_mviews.v_ins_direct_conversion2;
    """
    query = pg_execute(query)


def create_all_activity():
    
    query = """
    truncate mabawa_mviews.all_activity;
    insert into mabawa_mviews.all_activity
    SELECT activity, id, "date", user_code, staff_name, dpt, branch, cust_code
    FROM mabawa_mviews.v_all_activity
    """
    query = pg_execute(query)



def refresh_lens_silh():

    query = """
    refresh materialized view mabawa_mviews.order_contents;
    refresh materialized view mabawa_mviews.lens_incentive;
    refresh materialized view mabawa_mviews.silh_incentive;
    """
    query = pg_execute(query)