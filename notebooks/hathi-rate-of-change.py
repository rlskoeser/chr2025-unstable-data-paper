import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib

    import marimo as mo
    import polars as pl

    return mo, pathlib, pl


@app.cell
def _(pathlib):
    hathi_data_dir = pathlib.Path("data/hathi/updates")

    field_list = (hathi_data_dir / "hathi_field_list.txt").open().read().split()
    field_list
    return field_list, hathi_data_dir


@app.cell
def _(field_list, hathi_data_dir, pl):
    # load most recent full dataset with specified field list; treat as TSV

    df = (
        pl.scan_csv(
            hathi_data_dir / "hathi_full_20250701.txt.gz",
            has_header=False,
            new_columns=field_list,
            separator="\t",
            quote_char=None,  # do not treat " as escape character, since it is used in some content
            encoding="utf8",
        )
        .select(["htid", "access", "rights", "collection_code", "access_profile_code"])
        .collect()
    )

    df.head(10)
    return (df,)


@app.cell
def _(df):
    total_ht_vols = df.height

    print(f"{total_ht_vols:,} total volumes")
    return (total_ht_vols,)


@app.cell
def _(df):
    df["rights"].value_counts()
    return


@app.cell
def _(df):
    df["access"].value_counts()
    return


@app.cell
def _(df):
    df["access_profile_code"].value_counts()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Load PPA data so we can get counts for items in PPA changing as well as all of HT.""")
    return


@app.cell
def _(pl):
    # load PPA work-level metadata and limit to HathiTrust content
    ppa_ht_df = pl.read_csv("data/ppa/ppa_work_metadata.csv").filter(
        pl.col("ppa_source").eq("HathiTrust")
    )
    # get the total number of unique HT volumes in PPA; use unique since a few excerpts are from the same volume
    ppa_ht_total = ppa_ht_df["ppa_source_id"].unique().len()

    ppa_ht_df
    return ppa_ht_df, ppa_ht_total


@app.cell
def _(field_list, hathi_data_dir, pl, ppa_ht_df):
    import datetime

    from polars.exceptions import NoDataError

    update_data = []

    for update_file in hathi_data_dir.glob("hathi_upd_*.txt.gz"):
        update_date_str = update_file.stem.rsplit("_")[-1].replace(".txt", "")
        update_date = datetime.datetime.strptime(update_date_str, "%Y%m%d").date()

        # at least one file is actually empty; handle the error and skip
        try:
            update_df = (
                pl.scan_csv(
                    update_file,
                    has_header=False,
                    new_columns=field_list,
                    separator="\t",
                    quote_char=None,  # do not treat " as escape character, since it is used in smoe content
                    encoding="utf8",
                )
                .select(["htid"])  # , "access", "rights", "collection_code"])
                .collect()
            )
            # join with ppa data so we can count # ppa volumes that changed
            ppa_updates_df = update_df.join(
                ppa_ht_df, left_on="htid", right_on="ppa_source_id", how="inner"
            )

            update_data.append(
                {
                    "date": update_date,
                    "num_updated": update_df.height,
                    "ppa_updated": ppa_updates_df.height,
                }
            )
        except NoDataError as err:
            print(f"Error parsing {update_file.name} : {err}")
    return datetime, update_data


@app.cell
def _(pl, ppa_ht_total, total_ht_vols, update_data):
    update_data_df = pl.from_dicts(update_data)
    update_data_df = (
        # calculate percentages for all of hathitrust and then all of ppa
        update_data_df.with_columns(
            pct_updated=pl.col("num_updated").truediv(total_ht_vols),
            pct_ppa_updated=pl.col("ppa_updated").truediv(ppa_ht_total),
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

    # get date range for the data
    earliest = update_data_df["date"].min()
    latest = update_data_df["date"].max()

    num_chart = (
        alt.Chart(update_data_df)
        .mark_bar(width=10, color="#4661ac")
        .encode(
            x=alt.X("date", title="Date").axis(
                format="%B %d", tickCount="week"
            ),  # suppress labels since will be combined
            y=alt.Y("num_updated", title="# updated"),
        )
    ).properties(
        width=840,
        height=100,
        # title=f"Updated volumes in all of HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}",
    )

    # can we set y-domain to force the axes to align?
    pct_chart = (
        num_chart.mark_bar(width=10, color="#ff7f0e", opacity=0.5)
        .encode(
            x=alt.X("date", title="Date").axis(format="%B %d", tickCount="week"),
            y=alt.Y("pct_updated", title="% updated"),
        )
        .properties(
            height=100,
        )
    )

    #     )

    num_chart.save("images/hathitrust_changes_countonly.png", ppa=300)
    # num_chart.save("images/hathitrust_changes_countonly.pdf")

    num_chart
    # can we combine and fix the scale so % is one side and count is the other?
    # right now they don't match exactly
    # combined_chart = (
    #     (num_chart + pct_chart)
    #     .properties(
    #         title=f"Updated volumes in all of HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}"
    #     )
    #     .resolve_scale(x="shared", y="independent")
    # )

    # combined_chart = (
    #     (num_chart & pct_chart)
    #     .properties(
    #         title=f"Updated volumes in all of HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}"
    #     )
    #     .resolve_scale(x="shared")
    # )
    # combined_chart.save("images/hathitrust_changes.pdf")

    # combined_chart
    return alt, earliest, latest, num_chart


@app.cell
def _(earliest, latest, mo):
    mo.md(f"""Updated volumes in all of HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}""")
    return


@app.cell
def _(mo):
    mo.md(r"""What is the largest number of updates in a single day during this time period?""")
    return


@app.cell
def _(update_data_df):
    update_data_df.sort("num_updated", descending=True).head(5)
    return


@app.cell
def _(mo, update_data_df):
    max_update = update_data_df.sort("num_updated", descending=True).row(0, named=True)

    max_num_updated = max_update["num_updated"]
    max_pct_updated = max_update["pct_updated"]
    max_updates_day = max_update["date"]
    mo.md(
        f"Largest number updated: {max_num_updated:,} ({max_pct_updated * 100:.1f}%) on {max_updates_day}"
    )
    return


@app.cell
def _(mo):
    mo.md(r"""What is the average daily change across this time period?""")
    return


@app.cell
def _(mo, update_data_df):
    avg_num_updated = update_data_df["num_updated"].mean()
    avg_pct_updated = update_data_df["pct_updated"].mean()
    mo.md(
        f"Average daily update for this period is {int(avg_num_updated):,} ({avg_pct_updated * 100:.1f}%)"
    )
    return


@app.cell
def _(alt, earliest, latest, num_chart, update_data_df):
    # ppa charts equivalent to above

    ppa_num_chart = (
        alt.Chart(update_data_df)
        .mark_bar(width=10, color="#f05b69")
        .encode(
            x=alt.X("date", title="").axis(
                format="%B %d",
                tickCount="week",  # labels=False to suppress for combination
            ),  # suppress labels since displayed together
            y=alt.Y("ppa_updated", title="# volumes updated"),
        )
        .properties(
            height=100,
            width=840,
            # title=f"Updates to PPA volumes in HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}",
        )
    )

    ppa_pct_chart = (
        num_chart.mark_bar(width=10, color="#57c4c4")
        .encode(
            x=alt.X("date", title="Date").axis(format="%B %d", tickCount="week"),
            y=alt.Y("pct_ppa_updated", title="% updated"),
        )
        .properties(height=100)
    )

    ppa_combined_chart = (
        (ppa_num_chart & ppa_pct_chart)
        .properties(
            title=f"Updates to PPA volumes in HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}"
        )
        .resolve_scale(x="shared")
    )
    # ppa_combined_chart.save("images/ppa_hathitrust_changes.pdf")
    ppa_combined_chart
    return (ppa_num_chart,)


@app.cell
def _():
    return


@app.cell
def _(ppa_num_chart):
    # num_chart.save("images/hathitrust_changes_countonly.pdf")
    # ppa_num_chart.save("images/ppa_hathitrust_changes_countonly.pdf")
    ppa_num_chart.save("images/ppa_hathitrust_changes_countonly.png", ppi=300)
    ppa_num_chart
    return


@app.cell
def _(alt, earliest, latest, num_chart, ppa_num_chart):
    ppa_ht_numchart = (
        alt.vconcat(ppa_num_chart, num_chart)
        .resolve_axis(x="shared")
        .properties(
            title=f"Updates to PPA HathiTrust volumes and all of HathiTrust, {earliest.strftime('%B %d')} to {latest.strftime('%B %d %Y')}"
        )
    )

    # saving this chart fails for smoe reason
    # ppa_ht_numchart.save("images/ppa_and_hathitrust_changes.pdf")

    ppa_ht_numchart
    return


@app.cell
def _(mo):
    mo.md(r"""## deletions from public domain dataset""")
    return


@app.cell
def _(datetime, pathlib, pl, total_ht_vols):
    hathi_deletion_dir = pathlib.Path("data/hathi/deletions")

    deletion_count = []

    start_str = "===BEGIN ID LIST==="
    end_str = "===END ID LIST==="

    for deletion_email in hathi_deletion_dir.glob("*.txt"):
        deletion_date = datetime.datetime.strptime(
            deletion_email.stem, "%Y-%m-%d"
        ).date()
        contents = deletion_email.open().read()
        # use begin/end strings to isolate content of interest
        deleted_id_list = contents.split(start_str)[1].split(end_str)[0]
        # split on newlines and filter out any empty strings
        deleted_ids = [id for id in deleted_id_list.split("\n") if id.strip() != ""]
        deletion_count.append({"date": deletion_date, "count": len(deleted_ids)})

    deletion_df = (
        pl.from_dicts(deletion_count)
        .sort("date")
        .with_columns(percent=pl.col("count").truediv(total_ht_vols))
    )
    deletion_df
    return


if __name__ == "__main__":
    app.run()
