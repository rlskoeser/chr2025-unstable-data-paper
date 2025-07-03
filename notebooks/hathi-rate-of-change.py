import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib

    import marimo as mo
    import polars as pl
    return pathlib, pl


@app.cell
def _(pathlib):
    hathi_data_dir = pathlib.Path("data/hathi")

    field_list = (hathi_data_dir / "hathi_field_list.txt").open().read().split()
    field_list
    return field_list, hathi_data_dir


@app.cell
def _(field_list, pl):
    # df = pl.scan_csv("data/hathi/hathi_full_20250701.txt.gz", has_header=False, new_columns=field_list, separator="\t")

    # there's a field that is causing errors, polars says it is malformed; but if we select specific
    # columns and ignore errors we can get the data


    df = (
        pl.scan_csv(
            # "data/hathi/hathi_upd_20250623.txt.gz",
            "data/hathi/hathi_full_20250701.txt.gz",
            has_header=False,
            new_columns=field_list,
            schema_overrides={"imprint": pl.datatypes.String},
            separator="\t",
            ignore_errors=True,
        )
        .select(["htid", "access", "rights", "collection_code"])
        .collect()
    )

    df.head()
    return (df,)


@app.cell
def _(df):
    print(f"{df.height:,} rows")
    return


@app.cell
def _():
    total_ht_vols = 16_719_190
    return (total_ht_vols,)


@app.cell
def _(field_list, hathi_data_dir, pl):
    import datetime

    update_data = []

    for update_file in hathi_data_dir.glob("hathi_upd_*.txt.gz"):
        update_date_str = update_file.stem.rsplit("_")[-1].replace(".txt", "")
        update_date = datetime.datetime.strptime(update_date_str, "%Y%m%d").date()

        update_df = (
            pl.scan_csv(
                update_file,
                has_header=False,
                new_columns=field_list,
                schema_overrides={"imprint": pl.datatypes.String},
                separator="\t",
                ignore_errors=True,
            )
            .select(["htid"])  # , "access", "rights", "collection_code"])
            .collect()
        )

        update_data.append({"date": update_date, "num_updated": update_df.height})
    return (update_data,)


@app.cell
def _(pl, total_ht_vols, update_data):
    update_data_df = pl.from_dicts(update_data)
    update_data_df = (
        update_data_df.with_columns(
            pct_updated=pl.col("num_updated").truediv(total_ht_vols)
        )
        .cast({"date": pl.Date})
        .sort("date")
    )
    update_data_df
    return (update_data_df,)


@app.cell
def _(update_data_df):
    # save totals calculated from these files
    update_data_df.write_csv("data/hathi_update_counts.csv")
    return


@app.cell
def _(update_data_df):
    import altair as alt

    num_chart = (
        alt.Chart(update_data_df)
        .mark_bar(width=10)
        .encode(
            x=alt.X("date", title=""), y=alt.Y("num_updated", title="# updated")
        )
    ).properties(width=500)

    pct_chart = (
        num_chart.mark_bar(width=10, color="orange")
        .encode(
            x=alt.X("date", title="Date"),
            y=alt.Y("pct_updated", title="% updated"),
        )
        .properties(height=100)
    )
    combined_chart = (num_chart & pct_chart).properties(
        title="HathiTrust updated items, June 2025"
    )
    combined_chart.save("figures/hathitrust_changes.pdf")
    combined_chart
    return


if __name__ == "__main__":
    app.run()
