from airflow.models import variable
import pandas as pd

class FetchData:
    def __init__(self, engine, end_date, start_date, first_day) -> None:
        self.engine = engine
        self.end_date = end_date
        self.start_date = start_date
        self.first_day = first_day

    def fetch_data(self, query) -> pd.DataFrame:
        with self.engine.connect() as connection:
            try:
                data = pd.read_sql_query(query, con=connection)
                return data
            except Exception as e:
                raise e
            
    def return_query_format(self, query, data_query) -> str:
        mtd_query = query.format(
            start_date = self.first_day,
            end_date = self.end_date
        )

        pw_query = query.format(
            start_date = self.start_date,
            end_date = self.end_date
        )

        query_data = data_query.format(
            start_date = self.first_day,
            end_date = self.end_date
        )

        return mtd_query, pw_query, query_data



    def fetch_pw_eyetests(self, database, users, users_table, start_date) -> pd.DataFrame:
        query = f"""
            select conv.cust_code as "Customer Code", 
            conv.cust_createdon as "CreateDate", 
            trim(to_char(conv.cust_createdon::date, 'Month')) as "Month",
            users.user_name as "Staff",
            "cust_outlet" as "Outlet",
            conv.cust_type as "Customer Type", 
            conv.draft_orderno as "Order Number", 
            conv.code as "Code",
            conv.days as "Days",
            case when conv.days is null then 0
            when conv.days::int <= 14::int then 1
            else 0 end as "Conversion"
            from {database}.reg_conv as conv
            left join {users}.{users_table} as users 
            on conv.cust_sales_employeecode::text = users.se_optom::text
            where
            conv.cust_createdon::date between '{start_date}' and '{self.end_date}'
            and conv.cust_outlet not in ('0MA','HOM','null', 'MUR')
            and conv.cust_code <> 'U10000825'
        """

        return self.fetch_data(query)
        
            
    def fetch_staff_nps(self, view, start_date) -> pd.DataFrame:
        query = f"""
           SELECT a.outlet as "Outlet", a.staff as "Staff",
           ROUND((((sum(a.promoters) - sum(a.detractors)) / sum(a.responses))::decimal) * 100, 2) AS "NPS"
           from (select
           date,
           outlet,
           responses,
           detractors,
           promoters,
           staff
           FROM {view}.nps_view_2
           where date::date between '{start_date}' and '{self.engine}'
           ) as a
           group by a.outlet, a.staff;
        """

        return self.fetch_data(query)

    def fetch_google_reviews(self):
        query = """
            SELECT store_code AS "Outlet",
            ROUND(SUM(CASE
                    WHEN star_rating = 'THREE' THEN 3
                    WHEN star_rating = 'ONE' THEN 1
                    WHEN star_rating = 'FOUR' THEN 4
                    WHEN star_rating = 'FIVE' THEN 5
                    WHEN star_rating = 'TWO' THEN 2
                    ELSE 0
                    END)::DECIMAL / COUNT(star_rating), 2) AS average_rating
            FROM mabawa_staging.google_reviews
            WHERE createdat::date between '{start_date}' and '{end_date}'
            GROUP BY 1;
        """

        data_query = """
            SELECT
            store_code AS "Outlet",
            createdat AS "Review Date",
            INITCAP(reviewer) AS "Customer Name",
            star_rating AS "Rating",
            review_comment AS "Comment"
            FROM mabawa_staging.google_reviews
            where createdat::date between '{start_date}' and '{end_date}'
        """

        count_query = """
            SELECT store_code AS "Outlet",
            COUNT(star_rating) AS "Counts"
            FROM mabawa_staging.google_reviews
            WHERE createdat::date between '{start_date}' and '{end_date}'
            GROUP BY 1;
        """

        mtd, pw, data = self.return_query_format(query, data_query)
        mtd_count, pw_count, data = self.return_query_format(count_query, data_query)
        return (
            self.fetch_data(mtd),
            self.fetch_data(pw),
            self.fetch_data(data),
            self.fetch_data(mtd_count),
            self.fetch_data(pw_count)
        )
