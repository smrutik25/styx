from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


date_operator = Operator('date')

date_operator.set_analytical_schema([
    {
        "column_name": "DATEKEY",
        "data_type": "INT",
        "primary_key": True
    },
    {
        "column_name": "DATE",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "DATEOFWEEK",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "MONTH",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "YEAR",
        "data_type": "INT"
    },
    {
        "column_name": "YEARMONTHNUM",
        "data_type": "INT"
    },
    {
        "column_name": "YEARMONTH",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "DAYNUMINWEEK",
        "data_type": "INT"
    },
    {
        "column_name": "DAYNUMINMONTH",
        "data_type": "INT"
    },
    {
        "column_name": "DAYNUMINYEAR",
        "data_type": "INT"
    },
    {
        "column_name": "MONTHNUMINYEAR",
        "data_type": "INT"
    },
    {
        "column_name": "WEEKNUMINYEAR",
        "data_type": "INT"
    },
    {
        "column_name": "SELLINGSEASON",
        "data_type": "VARCHAR"
    },
    {
        "column_name": "LASTDAYINWEEKFL",
        "data_type": "BOOL"
    },
    {
        "column_name": "LASTDAYINMONTHFL",
        "data_type": "BOOL"
    },
    {
        "column_name": "HOLIDAYFL",
        "data_type": "BOOL"
    },
    {
        "column_name": "WEEKDAYFL",
        "data_type": "BOOL"
    }
])

@date_operator.register
async def get_date(ctx: StatefulFunction, order_key):
    date_val = ctx.get()
    ctx.call_remote_async(
        'line_order',
        'set_date',
        order_key,
        (date_val["DATE"],)
    )
