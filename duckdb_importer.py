import os

import duckdb

data_dir = "data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
duckdb_file = os.path.join(data_dir, f"data.duckdb")
px_tbl = "prices"
px_pq_file = os.path.join(data_dir, f"{px_tbl}.parquet")
px_per_loc_tbl = "prices_loc"
px_per_loc_pq_file = os.path.join(data_dir, f"{px_per_loc_tbl}.parquet")
encrypt_key = os.environ["PARQUET_ENCRYPTION_KEY"]
add_encrypt_key = f"PRAGMA add_parquet_key('key256', '{encrypt_key}');"
encrypt_conf = "{footer_key: 'key256'}"

if __name__ == "__main__":
    prices_query = (
        lambda loc_type: f"""
            select year_month as date,
                   active_ads_qty as num_ads,
                   active_ads_avg_px_m2 as avg_price,
                   'rentals_flats' as type{', location' if loc_type else ''}
            from property_cro.analytics_rentals_flats_analysis.mv_inventory{loc_type}_rentals_flats
            union all
            select year_month as date,
                   active_ads_qty as num_ads,
                   active_ads_avg_px_m2 as avg_price,
                   'sales_flats' as type{', location' if loc_type else ''}
            from property_cro.analytics_sales_flats_analysis.mv_inventory{loc_type}_sales_flats
            union all
            select year_month as date,
                   active_ads_qty as num_ads,
                   active_ads_avg_px_m2 as avg_price,
                   'sales_houses' as type{', location' if loc_type else ''}
            from property_cro.analytics_sales_houses_analysis.mv_inventory{loc_type}_sales_houses
        """
    )

    export_to_duckdb_tbl_query = (
        lambda query, output: f"drop table if exists {output}; create table {output} as ( {query})"
    )
    export_to_parquet_query = (
        lambda query, output: f"COPY ({query}) TO '{output}' (ENCRYPTION_CONFIG {encrypt_conf});"
    )
    export_to_parquet_unencrypted_query = (
        lambda query, output: f"COPY ( SELECT * FROM ({query}) LIMIT 10) TO '{output}' ;"
    )

    pg_host = "127.0.0.1"
    dbname = "property_cro"

    with duckdb.connect(database=duckdb_file, read_only=False) as conn:
        conn.execute(add_encrypt_key)
        conn.execute(
            f"INSTALL postgres; LOAD postgres; "
            f"ATTACH 'dbname={dbname} host={pg_host}' AS property_cro (TYPE postgres);"
        )
        conn.execute(
            export_to_duckdb_tbl_query(query=prices_query(loc_type=""), output=px_tbl)
        )
        conn.execute(
            export_to_parquet_query(query=prices_query(loc_type=""), output=px_pq_file)
        )
        conn.execute(
            export_to_duckdb_tbl_query(
                query=prices_query(loc_type="_general_location"),
                output=px_per_loc_tbl,
            )
        )
        conn.execute(
            export_to_parquet_query(
                query=prices_query(loc_type="_general_location"),
                output=px_per_loc_pq_file,
            )
        ),
        conn.execute(
            export_to_parquet_unencrypted_query(
                query=prices_query(loc_type="_general_location"),
                output=px_per_loc_pq_file.replace(".parquet", "_unencrypted.parquet"),
            )
        ),
        conn.execute(
            export_to_parquet_unencrypted_query(
                query=prices_query(loc_type=""),
                output=px_pq_file.replace(".parquet", "_unencrypted.parquet"),
            )
        ),
