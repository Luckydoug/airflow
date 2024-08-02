import pandas as pd 
from reports.phone_calls.utils.utils import save_file


def online_orders_followups(path) -> None:
    online_orders = save_file(
        sender="",
        subject="",
        has_attachment=True,
        date_since="",
        path=path
    )

    online_orders = online_orders[online_orders["Order Number"] != 0]
    online_orders["Created Time (Ticket)"] = pd.to_datetime(online_orders["Created Time (Ticket)"], format="%d %b %Y %H:%M %p")
    online_orders["Online Order Date Called"] = pd.to_datetime(online_orders["Online Order Date Called"], format="%d %b %Y")
    online_orders["Ticket Day"] = online_orders["Created Time (Ticket)"].dt.day_name()


