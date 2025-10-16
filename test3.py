for name, query in queries.items():
    arrow_table = con.execute(query).arrow()

    pq.write_to_dataset(
        table=arrow_table,
        root_path=parquet_output_path(name),
        partition_cols=['year', 'month', 'day'],
        compression='snappy'
    )

    csv.write_csv(
        arrow_table,
        f"{csv_output_path(name)}{name}_{year1}{month1:02}{day1:02}.csv"
    )
