from airflow.models import variable
import pandas as pd
import datetime
today = datetime.datetime.now().strftime("%Y-%m-%d")


class FetchData:
    def __init__(self, engine, database) -> None:
        self.engine = engine
        self.database = database

    def fetch_data(self, query) -> pd.DataFrame:
        with self.engine.connect() as connection:
            try:
                data = pd.read_sql_query(query, con=connection)
                return data
            except Exception as e:
                raise e

    def fetch_orderscreen(self, start_date) -> pd.DataFrame:
        orderscreen_query = f"""
        select orderscreen.doc_entry as "DocEntry", odsc_date::date  as "Date",
        case
            when length(odsc_time::text) in (1,2) then null
            else
                (left(odsc_time::text,(length(odsc_time::text)-2))||':'||right(odsc_time::text, 2))::time 
            end 
                as "Time",
        odsc_status as "Status", odsc_createdby as "Created User", 
        odsc_remarks as "Remarks", orders.doc_no::int as "Order Number"
        from {self.database}.source_orderscreenc1 orderscreen
        left join {self.database}.source_orderscreen as orders 
        on orderscreen.doc_entry = orders.doc_entry
        where odsc_date::date between '{start_date}' and '{today}'
        and orders.cust_code <> 'U10000002'
        """

        return self.fetch_data(orderscreen_query)


    def fetch_orders(self, start_date = '2022-08-01') -> pd.DataFrame:
        orders_query = f"""
        SELECT CAST(orders.doc_entry AS INT) AS "DocEntry", 
        CAST(orders.doc_no AS INT) AS "Order Number", 
        orders.presctiption_no::text as "Code",
        orders.ods_createdon::date AS "CreateDate",
        orders.ods_total_amt::decimal as "Order Amount",
        case when ods_insurance_totaprvamt2::decimal + ods__insurance_totaprvamt1::decimal <> 0 
        then ods_insurance_totaprvamt2::decimal + ods__insurance_totaprvamt1::decimal 
        else ods_insurance_patotaprvamt1::decimal + ods_insurance_patotaprvamt2::decimal end as "Approved Amount",
        CASE
            WHEN length(ods_createdat::text) in (1,2) THEN null
            ELSE (left(ods_createdat::text,(length(ods_createdat::text)-2))||':'||right(ods_createdat::text, 2))::time 
        END AS "CreateTime",
        orders.ods_status as "Status",
        orders.ods_status1 as "Current Status",
        CAST(orders.cust_code AS TEXT) AS "Customer Code", 
        orders.ods_outlet AS "Outlet", 
        orders.ods_insurance_order as "Insurance Order",
        orders.ods_creator AS "Creator", 
        users.user_name AS "Order Creator"
        FROM {self.database}.source_orderscreen orders
        LEFT JOIN {self.database}.source_users AS users ON CAST(orders.ods_creator AS TEXT) = CAST(users.user_code AS TEXT)
        WHERE orders.ods_createdon::date BETWEEN '{start_date}' AND '{today}'
        AND CAST(orders.cust_code AS TEXT) <> ''
        """
        data =  self.fetch_data(orders_query)
        data["% Approved"] = ((data["Approved Amount"] / data["Order Amount"]) * 100).fillna(0).round(0)
        return data


    def fetch_insurance_companies(self, start_date = '2023-01-01') -> pd.DataFrame:
        insurance_companies_query = f"""
        select orders.doc_no::int as "Order Number",
        orders.cust_code as "Customer Code",
            insurance_company.insurance_name as "Insurance Company",
            plan.plan_scheme_name as "Insurance Scheme", plan.plan_scheme_type as "Scheme Type",
            ods_insurance_feedback1 as "Feedback 1", ods_insurance_feedback2 as "Feedback 2"
        from {self.database}.source_orderscreen as orders
        left join {self.database}.source_users  AS users 
        ON CAST(orders.ods_creator AS TEXT) = CAST(users.se_optom AS TEXT)
        inner JOIN {self.database}.source_web_payments as payments 
        on orders.doc_entry::text = payments.unique_id::text and payments.payment_mode = 'Insurance'
        left join {self.database}.source_insurance_company as insurance_company 
        on payments.insurance_company_name::text = insurance_company.insurance_code::text 
        left join {self.database}.source_insurance_plan as plan 
        on insurance_company.id = plan.insurance_id and plan.plan_scheme_name = payments.insurance_scheme
        where orders.ods_createdon::date between '{start_date}' and '{today}'
        """

        return self.fetch_data(insurance_companies_query)


    def fetch_sales_orders(self, start_date) -> pd.DataFrame:
        query = f"""
        select internal_number as "DocEntry",
        draft_orderno::int as "Order Number"
        from {self.database}.source_orders_header
        where order_canceled <> 'Y'
        and creation_date::date between '{start_date}' and '{today}'
        and draft_orderno <> 'nan'
        """

        return self.fetch_data(query)
    

    def fetch_branch_data(self, database) -> pd.DataFrame:
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

        return self.fetch_data(query)

    def fetch_no_feedbacks(self, database, start_date) -> pd.DataFrame:
        query = f"""
        select order_number as "Order Number",
        user_name as "Order Creator",
        create_date as "CreateDate",
        cust_code as "Customer Code",
        branch as "Outlet",
        insurance_company as "Insurance Company",
        insurance_scheme as "Insurance Scheme",
        scheme_type as "Scheme Type",
        request_date "Request Date",
        feedback_date as "Feedback Date",
        month as "Month",
        feedback as "Feedback"
        from {database}.requests_feedbacks
        where due_date::date between '{start_date}' and '{today}';
        """

        return self.fetch_data(query)


    def fetch_working_hours(self) -> pd.DataFrame:
        query = f"""
        select warehouse_code as "Warehouse Code",
        warehouse_name as "Warehouse Name",
        docnum as "DocNum",
        days "Days",
        start_time as "Start Time",
        end_time as "End Time",
        auto_time as "Auto Time"
        from reports_tables.working_hours 
        """

        return self.fetch_data(query)
    
    def fetch_rejections(self, start_date) -> pd.DataFrame:
        query = f"""
        select ods_outlet as "Outlet",
        doc_no as "Order Number", user_name as "Order Creator",
        odsc_date::date as "Date", odsc_time as "Time", odsc_remarks as "Remarks"
        from mabawa_mviews.rejections_view rv
        where odsc_date::date between '{start_date}' and '{today}'
        """

        return self.fetch_data(query)

    
    def fetch_holidays(self, dw):
        query = f"""
        select holiday_date as "Date",
        holiday_name as "Holiday"
        from {dw}.dim_holidays;
        """
        to_drop =  pd.read_sql_query(query, con=self.engine)

        date_objects = pd.to_datetime(to_drop['Date'], dayfirst=True).dt.date
        holiday_dict = dict(zip(date_objects, to_drop['Holiday']))

        return holiday_dict 