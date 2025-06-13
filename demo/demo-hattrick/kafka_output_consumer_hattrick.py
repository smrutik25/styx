import sys
import pandas as pd
import uvloop
import ast


async def consume(save_dir, txn_consumer=None, ana_consumer=None):
    records = []
    query_records = []
    freshness_records = []
    try:
        if txn_consumer:
            while True:
                data = await txn_consumer.getmany(timeout_ms=10_000)
                if not data:
                    break
                for messages in data.values():
                    for msg in messages:
                        records.append((msg.key, msg.value, msg.timestamp))

        if ana_consumer:
            freshness_queries = pd.read_csv(f"{save_dir}/freshness_queries.csv")["request_id"].tolist()
            freshness_queries = [ast.literal_eval(s) for s in freshness_queries]
            while True:
                data = await ana_consumer.getmany(timeout_ms=1_000)
                if not data:
                    break
                for messages in data.values():
                    for msg in messages:
                        if msg.key in freshness_queries:
                            freshness_records.append((msg.key, msg.value, msg.timestamp))
                        else:
                            query_records.append((msg.key, len(msg.value), msg.timestamp))
    finally:
        if records:
            pd.DataFrame.from_records(records,
                                      columns=['request_id', 'response', 'timestamp']).sort_values(by="timestamp").to_csv(
                            f'{save_dir}/output.csv', index=False)
        if query_records:
            pd.DataFrame.from_records(query_records,
                                      columns=['request_id', 'response_size', 'timestamp']).sort_values(by="timestamp").to_csv(
                f'{save_dir}/query_output.csv', index=False)

        if freshness_records:
            pd.DataFrame.from_records(freshness_records,
                                      columns=['request_id', 'response', 'timestamp']).sort_values(by="timestamp").to_csv(
                f'{save_dir}/freshness_output.csv', index=False)


async def main(save_dir=None, txn_consumer=None, ana_consumer=None):
    if save_dir is None:
        print("Save directory for the results not provided.")
        exit(1)
    await consume(save_dir, txn_consumer, ana_consumer)


if __name__ == "__main__":
    main(sys.argv[1])
