import pandas as pd


def create_google_reviews_kpi(
    mtd_reviews,
    pw_reviews,
    reviews_data,
    mtd_counts,
    pw_counts,
    path
):
    mtd_reviews_pivot = pd.pivot_table(
        mtd_reviews,
        index="Outlet",
        values="average_rating",
        aggfunc="sum"
    ).rename(columns={"average_rating": "MTD"}, level=0)

    pw_reviews_pivot = pd.pivot_table(
        pw_reviews,
        index="Outlet",
        values="average_rating",
        aggfunc="sum"
    ).rename(columns={"average_rating": "PW"}, level=0)

    pw_mtd_google_reviews = pd.merge(
        mtd_reviews_pivot,
        pw_reviews_pivot,
        right_index=True,
        left_index=True,
        how="left"
    ).fillna("-")

    count_pw_pivot = pd.pivot_table(
        pw_counts,
        index="Outlet",
        values="Counts",
        aggfunc="sum"
    ).rename(columns={"Counts": "PW"}, level=0)

    count_mtd_pivot = pd.pivot_table(
        mtd_counts,
        index="Outlet",
        values="Counts",
        aggfunc="sum"
    ).rename(columns={"Counts": "MTD"}, level=0)

    review_count_pw_mtd = pd.merge(
        count_pw_pivot,
        count_mtd_pivot,
        right_index=True,
        left_index=True,
        how="right"
    ).fillna("-")

    with pd.ExcelWriter(f"{path}kpi/google_reviews.xlsx") as writer:
        pw_mtd_google_reviews.to_excel(writer, sheet_name="branch_mtd_pw")
        review_count_pw_mtd.to_excel(writer, sheet_name="count_mtd_pw")
        reviews_data.to_excel(writer, sheet_name = "data", index = False)

  
    
