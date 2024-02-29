from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
import duckdb
import duckdb_importer as di
from streamlit import cache_data

charts_width: int = 600
duckdb_file: str = ":memory:"
cols_px: list[str] = ["date", "num_ads", "avg_price", "type"]
cols_px_per_loc: list[str] = cols_px + ["location"]
map_name_to_type: dict = {
    "Apartments for sale": "sales_flats",
    "Apartments for rent": "rentals_flats",
    "Houses for sale": "sales_houses",
}
distinct_locations: list[str] = [
    "Črnomerec",
    "Donja Dubrava",
    "Donji Grad",
    "Gornja Dubrava",
    "Gornji Grad - Medveščak",
    "Maksimir",
    "Novi Zagreb - Istok",
    "Novi Zagreb - Zapad",
    "Peščenica - Žitnjak",
    "Podsljeme",
    "Podsused - Vrapče",
    "Sesvete",
    "Stenjevec",
    "Trešnjevka - Jug",
    "Trešnjevka - Sjever",
    "Trnje",
    "Samobor",
    "Samobor - Okolica",
    "Dugo Selo",
    "Jastrebarsko",
    "Jastrebarsko - Okolica",
    "Velika Gorica",
    "Velika Gorica - Okolica",
    "Sveta Nedelja",
    "Sveti Ivan Zelina",
    "Ivanić-Grad",
    "Rugvica",
]
st.set_page_config(page_icon="🏠", page_title="Property market analytics")


def icon(emoji: str):
    """Shows an emoji as a Notion-style page icon."""
    st.write(
        f'<span style="font-size: 78px; line-height: 1">{emoji}</span>',
        unsafe_allow_html=True,
    )


_conn: duckdb.DuckDBPyConnection = None


def init_conn(file_name: str) -> duckdb.DuckDBPyConnection:
    global _conn
    _conn = duckdb.connect(database=file_name)
    _load_pq = (
        lambda tbl, file, enc: f"CREATE TEMP TABLE {tbl} AS SELECT * FROM read_parquet('{file}', encryption_config = {enc})"
    )
    _conn.execute(f"{di.add_encrypt_key}")
    _conn.execute(_load_pq(di.px_tbl, di.px_pq_file, di.encrypt_conf))
    _conn.execute(_load_pq(di.px_per_loc_tbl, di.px_per_loc_pq_file, di.encrypt_conf))
    return _conn


def get_conn() -> duckdb.DuckDBPyConnection:
    global _conn, duckdb_file
    if _conn is not None:
        return _conn
    else:
        return init_conn(duckdb_file)


def gen_where_clause_prices(
    data_type: str,
    location: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
) -> str:
    where_clause = []
    if data_type:
        where_clause.append(f"type = '{map_name_to_type[data_type]}'")
    if start_date:
        where_clause.append(f"date >= '{start_date.isoformat()}'")
    if end_date:
        where_clause.append(f"date <= '{end_date.isoformat()}'")
    if location:
        loc_str = "','".join(location)
        where_clause.append(f"location in ('{loc_str}') ")
    where_clause_str = ""
    if len(where_clause) > 0:
        where_clause_str = f"where {' and '.join(where_clause)}"
    return where_clause_str


@st.cache_data()
def get_prices(
    data_type: str = None,
    start_date: datetime.date = None,
    end_date: datetime.date = None,
    location: list[str] = None,
) -> pd.DataFrame:
    all_cols_px_string = ",".join(cols_px_per_loc if location else cols_px)
    table_name = di.px_per_loc_tbl if location else di.px_tbl
    # location pricing is quarterly, while general is monthly so alter the lookback
    prices_lag = 4 if location else 12
    opt_prices_partition = ", location" if location else ""

    where_clause_str = gen_where_clause_prices(
        data_type,
        location,
        start_date,
        end_date,
    )
    query = f"""
        select 
            {all_cols_px_string}, 
            (avg_price - lag_avg_price) / lag_avg_price AS annual_price_change_pct 
        from (
            select 
                {all_cols_px_string}, 
                LAG(avg_price, {prices_lag}) OVER (PARTITION BY type {opt_prices_partition} ORDER BY date) AS lag_avg_price 
            from {table_name}
        ) as inner_q 
        {where_clause_str} 
        order by "type" asc, "date" asc
    """
    return get_conn().execute(query).df()


@cache_data
def get_min_date_all(table_name: str) -> tuple[datetime.date, datetime.date]:
    query = f"select min(date) as min_date from {table_name}"
    return get_conn().execute(query).fetchall()[0][0]


def calculate_annual_cagr(total_percent_change: float, num_months: float):
    # Convert total percent change to a monthly CAGR
    monthly_cagr = ((1 + total_percent_change / 100) ** (1 / num_months)) - 1
    # Calculate annual CAGR
    annual_cagr = (1 + monthly_cagr) ** 12 - 1
    return annual_cagr


def get_percent_change(df: pd.DataFrame, col_name: str):
    first_value = df[col_name].iloc[0]
    last_value = df[col_name].iloc[-1]
    percent_change = ((last_value - first_value) / first_value) * 100
    return percent_change, first_value, last_value


def extract_metrics(
    df: pd.DataFrame, date_col: str = "date", price_col: str = "avg_price"
):
    price_chg_pct, first_price, last_price = get_percent_change(df, price_col)
    min_dt: datetime.date = df[date_col].min().date()
    max_dt: datetime.date = df[date_col].max().date()
    months_delta: float = (
        max_dt.year * 12 + max_dt.month - (min_dt.year * 12 + min_dt.month)
    )
    cagr: float = calculate_annual_cagr(
        total_percent_change=price_chg_pct, num_months=months_delta
    )
    return (
        cagr,
        min_dt,
        max_dt,
        months_delta,
        price_chg_pct,
        first_price,
        last_price,
    )


def plot_prices_general(df: pd.DataFrame, x_col="date", y_col="avg_price"):
    # Create a selection that chooses the nearest point & selects based on x-value
    hover = alt.selection_point(
        fields=[x_col],
        nearest=True,
        on="mouseover",
    )

    lines = (
        alt.Chart(df)
        .mark_line(point="transparent")
        .encode(
            x=alt.X(x_col, axis=alt.Axis(title="Date")),
            y=alt.Y(y_col, axis=alt.Axis(title="Average price (EUR per m2)")).scale(
                zero=False
            ),
        )
        .transform_calculate(
            color='datum.annual_price_change_pct < 0 ? "red" : "green"'
        )
        .properties(width=charts_width)
    )
    # Draw points on the line, highlight based on selection, color based on annual_price_change_pct
    points = (
        lines.transform_filter(hover)
        .mark_circle(size=65)
        .encode(color=alt.Color("color:N", scale=None))
    )

    # Draw an invisible rule at the location of the selection
    tooltips = (
        alt.Chart(df)
        .mark_rule(opacity=0)
        .encode(
            x=x_col,
            y=y_col,
            tooltip=[
                alt.Tooltip(x_col, title="Date"),
                alt.Tooltip(y_col, title="Average price (EUR per m2)"),
                alt.Tooltip(
                    "annual_price_change_pct",
                    format=".2%",
                    title="12 month price chg (%)",
                ),
                alt.Tooltip("num_ads", title="Number of ads"),
            ],
        )
        .add_params(hover)
    )

    chart = lines + points + tooltips
    return chart


def plot_prices_location(
    df: pd.DataFrame, x_col="date", y_col="avg_price", group_col="location"
):
    brush = alt.selection_interval(encodings=["x"], empty=True)

    hover = alt.selection_point(
        fields=[x_col],
        nearest=True,
        on="mouseover",
    )

    lines = (
        (
            alt.Chart(df)
            .mark_line(point=True)
            .encode(
                x=alt.X(x_col, axis=alt.Axis(title="Date")),
                y=alt.Y(y_col, axis=alt.Axis(title="Average price (EUR per m2)")).scale(
                    zero=False
                ),
                color=group_col,
            )
        )
        .add_params(brush)
        .transform_calculate(
            color='datum.annual_price_change_pct < 0 ? "red" : "green"'
        )
        .properties(width=charts_width)
    )
    # Draw points on the line, highlight based on selection, color based on annual_price_change_pct
    points = (
        lines.transform_filter(hover)
        .mark_circle(size=65)
        .encode(color=alt.Color("color:N", scale=None))
    )
    # Draw an invisible rule at the location of the selection
    tooltips = (
        alt.Chart(df)
        .mark_rule(opacity=0)
        .encode(
            x=x_col,
            y=y_col,
            tooltip=[
                alt.Tooltip(x_col, title="Date"),
                alt.Tooltip(y_col, title="Average price (EUR per m2)"),
                alt.Tooltip(
                    "annual_price_change_pct",
                    format=".2%",
                    title="12 month price chg (%)",
                ),
                alt.Tooltip(group_col, title="Municipality"),
                alt.Tooltip("num_ads", title="Number of ads"),
            ],
        )
        .add_params(hover)
    )

    chart = lines + points + tooltips
    return chart


def main():
    col1, col2, col3 = st.columns(3)
    min_date_possible: datetime.date = None
    max_date_possible: datetime.date = None

    if min_date_possible is None and max_date_possible is None:
        min_date_possible = get_min_date_all(table_name=di.px_tbl)

    with col1:
        start_date: datetime.date = st.date_input(
            "Select start date",
            value=min_date_possible,
            min_value=min_date_possible,
            max_value=datetime.today(),
            format="DD/MM/YYYY",
        )
    with col2:
        end_date: datetime.date = st.date_input(
            "Select end date",
            value=datetime.today(),
            min_value=min_date_possible,
            max_value=datetime.today(),
            format="DD/MM/YYYY",
        )
    with col3:
        type_: str = st.selectbox("Select type", (map_name_to_type.keys()))

    tab1, tab2 = st.tabs(["General market", "Location specific"])

    with tab1:
        with st.container():
            df_monthly_prices: pd.DataFrame = get_prices(
                data_type=type_, start_date=start_date, end_date=end_date, location=None
            )
            cagr, min_dt, max_dt, months_delta, price_chg_pct, _, _ = extract_metrics(
                df_monthly_prices, "date", "avg_price"
            )
            st.text(
                f"{price_chg_pct:.2f}% change over {months_delta} months from {min_dt} to {max_dt}. \n"
                f"Compound annual growth rate: {cagr * 100:.2f}%"
            )
            st.altair_chart(
                plot_prices_general(df_monthly_prices), use_container_width=True
            )

    with tab2:
        with st.container():
            selected_locs: list[str] = st.multiselect(
                label="Municipalities",
                options=distinct_locations,
            )
            df_quarterly_loc_prices = get_prices(
                data_type=type_,
                start_date=start_date,
                end_date=end_date,
                location=None if len(selected_locs) == 0 else selected_locs,
            )
            if len(selected_locs) > 0:
                st.altair_chart(
                    plot_prices_location(df_quarterly_loc_prices),
                    use_container_width=True,
                )
                data: list = []
                for loc in df_quarterly_loc_prices["location"].unique():
                    sub_df: pd.DataFrame = df_quarterly_loc_prices[
                        df_quarterly_loc_prices["location"] == loc
                    ]
                    (
                        cagr,
                        min_dt,
                        max_dt,
                        months_delta,
                        price_chg_pct,
                        first_price,
                        last_price,
                    ) = extract_metrics(sub_df, "date", "avg_price")
                    data.append(
                        {
                            "Municipality": loc,
                            "Start price": f"{first_price:.0f} €/m2",
                            "End price": f"{last_price:.0f} €/m2",
                            "Change": price_chg_pct,
                            "Time span": f"{months_delta} months",
                            "CAGR": cagr * 100,
                            "Start date": min_dt,
                            "End date": max_dt,
                        }
                    )
                df = pd.DataFrame(data)
                styled_df = df.style.format(
                    subset=["Change", "CAGR"], formatter="{:.2f}%"
                )
                st.dataframe(data=styled_df, hide_index=True)


st.title("Property analytics for Zagreb, Croatia")
st.markdown(
    """
        <style>
               .block-container {
                    padding-top: 3rem;
                    padding-bottom: 1rem;
                    padding-left: 5rem;
                    padding-right: 5rem;
                }
        </style>
        """,
    unsafe_allow_html=True,
)

st.write(
    "Average price per square meter for buying apartments and houses, alongside the average rents per square meter for apartments, all presented in euros, encompassing only the property market of Zagreb, Croatia."
)
main()
