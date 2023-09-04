from airflow.models import Variable
import pandas as pd

def fetch_eyetests_conversion(engine, database, start_date, end_date, users, users_table):
    et_q = f""" 
        SELECT
        code, create_date, trim(to_char(create_date::date, 'Month')) as "Month", 
        create_time, optom, optom_name, rx_type, branch_code, cust_code, status, 
        patient_to_ophth, "RX",plano_rx, sales_employees, handed_over_to, view_date, view_creator, 
        last_viewed_by, branch_viewed, order_converted,  date_converted,
        days, on_after, on_after_createdon, on_after_cancelled, on_after_status,
        on_before_prescription_order, on_before_mode, reg_cust_type, mode_of_pay,
        case when "RX" = 'High Rx' then 1 else 0 end as high_rx,
        case when "RX" = 'Low Rx' then 1 else 0 end as low_rx,
        case when days is not null then 1 else 0 end as conversion,
        case when order_converted is not null and "RX" = 'High Rx' then 1 else 0 end as high_rx_conversion,
        case when order_converted is not null and "RX" = 'Low Rx' then 1 else 0 end as low_rx_conversion
        from
        (select row_number() over(partition by cust_code, create_date, code order by days, rx_type, code desc) as r, *
        from {database}.et_conv
        where status not in ('Cancel','Unstable', 'CanceledEyeTest', 'Hold')
        and (patient_to_ophth not in ('Yes') or patient_to_ophth is null)) as a 
        left join {users}.{users_table} b on a.optom::text = b.se_optom::text
        where a.r = 1
        and a.create_date::date >=  %(From)s
        and a.create_date::date <= %(To)s
        and a.branch_code not in ('0MA','null')
        and a.cust_code <> 'U10000002';
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
    case when conv.days is not null then 1 else 0 end as "Conversion"
    from {database}.{view} as conv
    left join {users}.{users_table} as users 
    on conv.cust_sales_employeecode::text = users.se_optom::text
    where
    conv.cust_createdon::date >=  %(From)s
    and conv.cust_createdon::date <= %(To)s
    and conv.cust_outlet not in ('0MA','null')
    and conv.cust_code <> 'U10000825'
    and conv.cust_code <> 'U10000002'
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
    case when viewrx.days is not null then 1 else 0
    end as "Conversion"
    from (select row_number() over(partition by view_date, cust_loyalty_code order by days, doc_entry) as r, *
    from {database}.{view}) as viewrx
    left join {users}.{users_table} as users 
    on viewrx.creator::text = users.user_code::text
    where
    viewrx.r = 1
    and viewrx.view_date::date >=  %(From)s
    and viewrx.view_date::date <= %(To)s
    and viewrx.branch not in ('0MA','null', 'MUR')
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

