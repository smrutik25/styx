from styx.common.operator import Operator
from styx.common.stateful_function import StatefulFunction


new_order_txn_operator = Operator('new_order_txn')


@new_order_txn_operator.register
async def receive_part_extended_price(ctx: StatefulFunction, extended_price):
    txn_metadata = ctx.get()
    order_key = txn_metadata["OK"]
    line_numbers = txn_metadata["LN"]
    txn_metadata["RR"] += 1
    txn_metadata["TP"] += extended_price
    # Call the line_order functions to update the total price
    if txn_metadata["RR"] == len(line_numbers):
        for line_number in line_numbers:
            line_order_key: str = f"{order_key}:{line_number}"
            ctx.call_remote_async(
                'line_order',
                'receive_total',
                line_order_key,
                # needed to get back the reply
                (txn_metadata["TP"],)
            )
    ctx.put(txn_metadata)
    return ctx.key, txn_metadata["TP"]


@new_order_txn_operator.register
async def new_order_txn(ctx: StatefulFunction, params: dict):
    order_key = params['OK']
    line_numbers = list(params["LO"].keys())
    txn_metadata = {
        "OK": order_key,
        "LN": line_numbers,
        "RR": 0,
        "TP": 0
    }
    order_params = {'OD': params['OD'], 'CN': params['CN']}
    # Call the line_order functions
    for line_number in line_numbers:
        line_order_key: str = f"{order_key}:{line_number}"
        line_order_params = order_params | params['LO'][line_number]
        ctx.call_remote_async(
            'line_order',
            'new_order_txn',
            line_order_key,
            # needed to get back the reply
            (ctx.key, line_order_params)
        )
    ctx.put(txn_metadata)
