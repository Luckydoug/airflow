from airflow.models import Variable
import pandas as pd

def fetch_eyetests_conversion(engine, start_date, end_date, users, users_table):
    et_q = f""" 
        select *,
        trim(to_char(create_date::date, 'Month')) as "Month",
        case when "RX" = 'High Rx' then 1 else 0 end as high_rx,
        case when "RX" = 'Low Rx' then 1 else 0 end as low_rx,
        case when days::int <=7 and "RX" = 'High Rx' then 1 else 0 end as high_rx_conversion,
        case when days::int <=7 and "RX" = 'Low Rx' then 1 else 0 end as low_rx_conversion
        from
        (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
        from report_views.ewc_conversion
        where status not in ('Cancel','Unstable', 'CanceledEyeTest', 'Hold')
        and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)) as a 
        left join {users}.{users_table} b on a.optom::text = b.se_optom::text
        where a.r = 1
        and a.create_date::date >=  %(From)s
        and a.create_date::date <= %(To)s
        and a.branch_code not in ('0MA','null', 'HOM')
        and a.cust_code not in ('U10000002', 'U10002522', 'U10002693', 'R10000888', 'R10000429', 'U10004417')
        and a.optom_name not in ('ERICK','Raghav M','BENTA OGOLLA','manager');
        """

    data = pd.read_sql_query(
        et_q, con=engine, params={
            'From': start_date, 
            'To': end_date
            }
        )
    
    data["create_date"] = pd.to_datetime(
        data["create_date"], format="%Y-%m-%d")
    return data


def fetch_registrations_conversion(engine, database, start_date, end_date, users, users_table, view):
    registration_conv_query = f"""
    select conv.cust_code as "Customer Code", 
    conv.cust_createdon as "CreateDate", 
    trim(to_char(conv.cust_createdon::date, 'Month')) as "Month",
    users.user_name as "Staff",
    "cust_outlet" as "Outlet",
    conv.cust_type as "Customer Type", 
    conv.draft_orderno as "Order Number", 
    conv.code as "Code",
    conv.days as "Days",
    case when conv.days <=7 then 1 else 0 end as "Conversion",
    conversion_remark as "Conversion Remarks"
    from {database}.{view} as conv
    left join {users}.{users_table} as users 
    on conv.cust_sales_employeecode::text = users.se_optom::text
    where
    conv.cust_createdon::date >=  %(From)s
    and conv.cust_createdon::date <= %(To)s
    and conv.cust_outlet not in ('0MA','null', 'HOM')
    and conv.cust_code not in ('U10000825', 'U10000002', 'R10000888','U10004417')
    and conv.cust_campaign_master <> 'GMC'
    """

    data = pd.read_sql_query(
        registration_conv_query, con=engine, 
        params={
            'From': start_date, 
            'To': end_date
        }
    )

    data["CreateDate"] = pd.to_datetime(data["CreateDate"], format="%Y-%m-%d")
    return data


def fetch_views_conversion(engine, database, start_date, end_date, users, users_table, view):
    view_rx_query = f"""
    select viewrx.doc_entry as "DocEntry", viewrx.view_date as "ViewDate", 
    trim(to_char(viewrx.view_date::date, 'Month')) as "Month",
    viewrx.creator as "Creator", users.user_name as "User Name",
    viewrx.cust_loyalty_code as "Customer Code", 
    viewrx.visit_id as "Code", viewrx.branch as "Branch",
    viewrx.ord_orderno as "Order Number", 
    viewrx.ord_ordercreation_date as "CreateDate", 
    viewrx.days as "Days",
    case when viewrx.days <=7 then 1 else 0
    end as "Conversion"
    from (select row_number() over(partition by view_date, cust_loyalty_code order by days, doc_entry) as r, *
    from {database}.{view}) as viewrx
    left join {users}.{users_table} as users 
    on viewrx.creator::text = users.user_code::text
    left join {database}.et_conv as ets on 
    viewrx.visit_id::text = ets.code::text
    where
    viewrx.r = 1
    and viewrx.view_date::date >=  %(From)s
    and viewrx.view_date::date <= %(To)s
    and viewrx.branch not in ('0MA','null', 'MUR', 'HOM')
    and viewrx.cust_loyalty_code <> 'U10000002'
    and ets."RX" = 'High Rx'
    """

    data = pd.read_sql_query(
        view_rx_query, 
        con=engine, 
        params={
            'From': start_date, 
            'To': end_date
        })
    data["ViewDate"] = pd.to_datetime(data["ViewDate"], format="%Y-%m-%d")
    return data


def fetch_branch_data(engine, database):
    query = f"""
    select branch_code as "Outlet",
    branch_name as "Branch",
    email as "Email",
    rm as "RM",
    rm_email as "RM Email",
    rm_group as "RM Group",
    srm as "SRM",
    srm_email as "SRM Email",
    branch_manager as "Branch Manager",
    front_desk as "Front Desk",
    zone as "Zone"
    from {database}.branch_data bd 
    """

    branch_data = pd.read_sql_query(query, con = engine)
    return branch_data


def fetch_ewc_conversion(engine):
    query = """
    select * from report_views.ewc_conversion
    """

    data = pd.read_sql_query(query, con=engine)
    return data


def fetch_submitted_insurance(engine, start_date, end_date):
    query = f"""
    select cust_code::text,
    odsc_status as request
    from report_views.pending_insurance 
    where odsc_date::date between '{start_date}' and '{end_date}'
    """

    data = pd.read_sql_query(query, con=engine)
    return data

