from airflow.models import variable
import pandas as pd
import datetime
from sub_tasks.libraries.utils import (
    arrange_dateranges,
    first_week_start,
    fourth_week_end,
    get_comparison_months
)

first_month, second_month = get_comparison_months()


def return_conversion_daterange(selection):
    if selection == "Weekly":
        start_date = first_week_start,
        end_date = fourth_week_end
        return start_date, end_date

    elif selection == "Monthly":
        today = datetime.date.today()
        if today.month <= 2:
            target_month = today.month + 10
            target_year = today.year - 1
        else:
            target_month = today.month - 2
            target_year = today.year
        start_date = datetime.date(
            target_year, target_month, 1
        ).strftime("%Y-%m-%d")

        first_day_of_current_month = today.replace(day=1)
        end_date = first_day_of_current_month - datetime.timedelta(days=1)
        return start_date, end_date

    else:
        return


def manipulate_multiindex(dataframe, name, col1, col2, rename):
    stacked_dataframe = dataframe.stack()
    stacked_dataframe[name] = round(
        (stacked_dataframe[col1] / stacked_dataframe[col2]) * 100, 1).fillna(0).astype(str) + "%"
    unstacked_dataframe = stacked_dataframe.unstack()
    swapped_dataframe = unstacked_dataframe.swaplevel(0, 1, 1).sort_index(
        level=1, axis=1).reindex([col2, name], axis=1, level=1)

    sorted_columns = arrange_dateranges(swapped_dataframe)
    final_dataframe = swapped_dataframe.reindex(
        sorted_columns, axis=1, level=0)
    final_dataframe = final_dataframe.reindex([col2, name], level=1, axis=1)
    final_dataframe = final_dataframe.rename(columns={col2: rename}, level=1)

    return final_dataframe


def create_weeky_branch_conversion(conversions, index, week_range, values, cols):
    weekly_branches_conversion = pd.pivot_table(
        conversions,
        index=index,
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).fillna(0)

    return manipulate_multiindex(
        weekly_branches_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_country_conversion(conversions, week_range, values, country, cols):
    conversions["Country"] = country
    summary_weekly_conversion = pd.pivot_table(
        conversions,
        index="Country",
        columns=week_range,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    return manipulate_multiindex(
        summary_weekly_conversion,
        cols[0],
        cols[1],
        cols[2],
        cols[3]
    )


def create_branch_conversion(weekly_data, index, values, rename, cols_order):
    branch_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    branch_conversion["%Conversion"] = round(
        branch_conversion[values[1]] /
        branch_conversion[values[0]] * 100, 0
    ).fillna(0).astype(int).astype(str) + "%"

    return branch_conversion.rename(columns=rename)[cols_order]


def create_staff_conversion(weekly_data, index, values, rename, cols_order):
    staff_conversion = pd.pivot_table(
        weekly_data,
        index=index,
        values=[values[0], values[1]],
        aggfunc={values[0]: "count", values[1]: "sum"}
    ).reset_index()

    staff_conversion["%Conversion"] = round(
        staff_conversion[values[1]] /
        staff_conversion[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    return staff_conversion.rename(columns=rename)[cols_order]


def create_monthly_summary(data, values, rename, country):
    data["Country"] = country
    country = pd.pivot_table(
        data,
        index="Country",
        columns="Month",
        values=values,
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    country_stack = country.stack()
    country_stack["%Conversion"] = round(
        country_stack[values[1]] / country_stack[values[0]] * 100, 0).astype(int).astype(str) + "%"
    country_unstack = country_stack.unstack()
    country_unstack = country_unstack.swaplevel(0, 1, 1).reindex(
        [first_month, second_month], level=0, axis=1)
    country_unstack = country_unstack.reindex(
        [values[0], values[1], "%Conversion"], level=1, axis=1)
    country_unstack = country_unstack.rename(columns=rename, level=1)

    return country_unstack


def create_monthly_conversion(data, index, values, rename):
    month_pivot = pd.pivot_table(
        data,
        index=index,
        columns="Month",
        values=values,
        aggfunc={values[0]: "count", values[1]: "sum"}
    )

    month_pivot_stack = month_pivot.stack()
    month_pivot_stack["%Conversion"] = round(
        month_pivot_stack[values[1]] / month_pivot_stack[values[0]] * 100, 0
    ).astype(int).astype(str) + "%"

    month_unstack = month_pivot_stack.unstack()
    month_unstack = month_unstack.swaplevel(0, 1, 1).reindex(
        [first_month, second_month], level=0, axis=1)
    month_unstack = month_unstack.reindex(
        [values[0], values[1], "%Conversion"], level=1, axis=1)
    month_unstack = month_unstack.rename(columns=rename, level=1)
    return month_unstack
