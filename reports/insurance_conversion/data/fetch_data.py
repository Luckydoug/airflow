from airflow.models import variable
import pandas as pd
import numpy as np
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


    def fetch_orderscreen(self, start_date):
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


    def fetch_orders(self, start_date = '2023-01-01'):
        orders_query = f"""
        SELECT CAST(orders.doc_entry AS INT) AS "DocEntry", 
            CAST(orders.doc_no AS INT) AS "Order Number", 
            orders.presctiption_no::text as "Code",
            orders.ods_createdon::date AS "CreateDate",
            CASE
                WHEN length(ods_createdat::text) in (1,2) THEN null
                ELSE (left(ods_createdat::text,(length(ods_createdat::text)-2))||':'||right(ods_createdat::text, 2))::time 
            END AS "CreateTime",
            ods_status as "Status",
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

        return self.fetch_data(orders_query)


    def fetch_insurance_companies(self, start_date = '2023-01-01'):
        insurance_companies_query = f"""
        select orders.doc_no::int as "Order Number",
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


    def fetch_sales_orders(self, start_date):
        query = f"""
        select internal_number as "DocEntry",
        draft_orderno::int as "Order Number"
        from {self.database}.source_orders_header
        where order_canceled <> 'Y'
        and posting_date::date between '{start_date}' and '{today}'
        """

        return self.fetch_data(query)
