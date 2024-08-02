import sys

sys.path.append(".")
import requests
import pandas as pd
from airflow.models import Variable
from pangres import upsert
from sub_tasks.data.connect_mawingu import engine
from sub_tasks.libraries.utils import return_session_id
from sub_tasks.libraries.utils import FromDate, ToDate


def fetch_sap_items():
    SessionId = return_session_id(country="Uganda")
    pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetItemDetails&pageNo=1&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
    pagecount_payload = {}
    pagecount_headers = {}

    pagecount_response = requests.request(
        "GET",
        pagecount_url,
        headers=pagecount_headers,
        data=pagecount_payload,
        verify=False,
    )
    data = pagecount_response.json()
    pages = data["result"]["body"]["recs"]["PagesCount"]

    print("Pages outputted", pages)

    itemsdf = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetItemDetails&pageNo={page}&FromDate={FromDate}&ToDate={ToDate}&SessionId={SessionId}"
        response = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        items = response.json()
        stripped_items = items["result"]["body"]["recs"]["Results"]
        items_df = pd.DataFrame.from_dict(stripped_items)
        items_df2 = items_df.T
        itemsdf = itemsdf.append(items_df2, ignore_index=True)

    itemsdf.rename(
        columns={
            "Item_No": "item_code",
            "Item_Description": "item_desc",
            "Foreign_Name": "item_foreign_name",
            "Item_Group": "item_group",
            "In_Stock": "item_in_stock",
            "Qty_Ordered_by_Customers": "item_qty_ordered_bycustomer",
            "Qty_Ordered_from_Vendors": "item_qty_orderde_from_vendor",
            "Purchasing_UoM": "item_purchasing_uom",
            "No_of_Items_per_Purchase_Unit": "item_no_ofitems_per_purchase_unit",
            "Minimum_Inventory_Level": "item_minimum_inventory_level",
            "Valuation_Method": "item_valuation_method",
            "Production_Date": "item_prod_date",
            "Active_To": "item_active_to",
            "ItemCategory": "item_category",
            "Supplier_Nme": "item_supplier",
            "Brand_Name": "item_brand_name",
            "Model_No": "item_model_no",
            "Sub_Group": "item_sub_group",
            "Rim_Type": "item_rim_type",
            "Shape": "item_shape",
            "Gender": "item_gender",
            "Size": "item_size",
            "Front_Colour": "item_front_colour",
            "Colour_No": "item_colour_no",
            "material": "item_material",
            "Lab": "item_lab",
            "Lens_Category": "item_lens_category",
            "Lens_Type": "item_lens_type",
            "LensType_SubCat": "item_lens_type_subcat",
            "Lens_Name": "item_lens_name",
            "Lens_Material": "item_lens_material",
            "Property": "item_property",
            "Coating": "item_coating",
            "Coating_SubCatg": "item_coating_subcatg",
            "Lens_MType": "item_lens_mtype",
            "CL_LensName": "item_cl_lensname",
            "CL_Type": "item_cl_tyoe",
            "CL_BC": "item_cl_bc",
            "CL_Dia": "item_cl_dia",
            "Front_Material": "item_front_material",
            "Temple_Material": "item_temple_material",
            "Temple_Colour": "item_temple_colour",
            "Lens_Colour": "item_lens_color",
            "Front_Size": "item_front_size",
            "Age": "item_age",
            "Bridge": "item_bridge",
            "Temple_Length": "item_temple_length",
            "Lens_Family": "item_lens_family",
            "Lens_Brand": "item_lens_brand",
            "Lens_prog_Design": "item_lens_prog_design",
            "Lens_SPH": "item_lens_sph",
            "Lens_CYL": "item_lens_cyl",
            "Lens_BASE": "item_lens_base",
            "Lens_ADD": "item_lens_add",
            "Lens_SPH_Transpose": "item_lens_sph_transpose",
            "Lens_CYL_Transpose": "item_lens_cyl_transpose",
            "Lens_EYE": "item_lens_eye",
            "Lens_DIA": "item_lens_dia",
            "Lens_Index": "item_lens_index",
            "Photo_Brand": "item_photo_brand",
            "Photo_Color": "item_photo_color",
            "Lens_Coating_Group": "item_lens_coating_group",
            "Manufacturer": "item_manufacturer",
            "Stock_Item": "item_stock_item",
            "LPO_Item": "item_lpo_item",
            "Overseas_Item": "item_overseas_item",
            "CL_Sell_Or_Trial": "item_cl_sell_or_trial",
            "CL_Frequency": "item_cl_frequency",
            "CL_Curve_Type": "item_cl_curve_type",
            "Design": "item_design",
            "CL_Family": "item_cl_family",
            "Default_Warehouse": "item_default_warehouse",
            "Lens_Sub_Brand": "item_lens_sub_brand",
            "Collection": "item_collection",
            "Front_Finish": "item_front_finish",
            "Temple_Finish": "item_temple_finish",
        },
        inplace=True,
    )

    print("TRANSFORMATION! Adding new columns")

    item_before_dup_calc = len(itemsdf)

    # remove duplicates based on the item_code and maintain the last
    itemsdf = itemsdf.drop_duplicates("item_code", keep="last")

    item_after_dup_calc = len(itemsdf)

    duplicates = item_before_dup_calc - item_after_dup_calc

    # am staging because there are a few duplicated on the
    # itemsdf.to_sql('source_items', con = engine, schema='mawingu_staging', if_exists = 'append', index=False)

    print("INFO! %d duplicates deleted" % (duplicates))

    print("INFO! %d rows" % (len(itemsdf)))

    print(itemsdf[itemsdf["item_code"] == "1OXY8103C546"])

    if itemsdf.empty:
        print("INFO! Items dataframe is empty!")
    else:
        itemsdf = itemsdf.set_index(["item_code"])
        print("INFO! Items upsert started...")

        upsert(
            engine=engine,
            df=itemsdf,
            schema="mawingu_staging",
            table_name="source_items",
            if_row_exists="update",
            create_table=False,
        )

        print("Update successful")


def fetch_item_groups():

    SessionId = return_session_id(country="Uganda")

    group_pagecount_url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetItemgroups&pageNo=1&SessionId={SessionId}"
    group_pagecount_payload = {}
    group_pagecount_headers = {}
    group_pagecount_response = requests.request(
        "GET",
        group_pagecount_url,
        headers=group_pagecount_payload,
        data=group_pagecount_headers,
        verify=False,
    )
    data = group_pagecount_response.json()
    pages = data["result"]["body"]["recs"]["PagesCount"]
    print("Pages outputted", pages)

    itemgroupdf = pd.DataFrame()
    payload = {}
    headers = {}
    for i in range(1, pages + 1):
        page = i
        url = f"https://10.40.16.9:4300/UGANDA_BI/XSJS/BI_API.xsjs?pageType=GetItemgroups&pageNo={page}&SessionId={SessionId}"
        itemsgroup = requests.request(
            "GET", url, headers=headers, data=payload, verify=False
        )
        itemsgroup = itemsgroup.json()
        itemsgroup = itemsgroup["result"]["body"]["recs"]["Results"]
        itemsgroup = pd.DataFrame.from_dict(itemsgroup)
        itemsgroup = itemsgroup.T
        itemgroupdf = itemgroupdf.append(itemsgroup, ignore_index=True)

    itemgroupdf.rename(
        columns={"ItmsGrpCod": "item_grp_code", "ItmsGrpNam": "item_grp_name"},
        inplace=True,
    )

    itemgroupdf = itemgroupdf.set_index(["item_grp_code"])

    upsert(
        engine=engine,
        df=itemgroupdf,
        schema="mawingu_staging",
        table_name="source_item_grp",
        if_row_exists="update",
        create_table=False,
        add_new_columns=True,
    )
