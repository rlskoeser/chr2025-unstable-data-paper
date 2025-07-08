import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(pl):
    # load PPA work-level metadata and limit to HathiTrust content
    ppa_ht_df = pl.read_csv("data/ppa/ppa_work_metadata.csv").filter(
        pl.col("ppa_source").eq("HathiTrust")
    )
    ppa_ht_df
    return (ppa_ht_df,)


@app.cell
def _(ppa_ht_df):
    import altair as alt

    alt.Chart(ppa_ht_df).mark_bar().encode(
        x=alt.X("ppa_work_year", bin=True).axis(format="r"), y="count()"
    )
    return


@app.cell
def _(pl):
    from zipfile import ZipFile

    genre_all_meta_df = pl.read_csv(
        ZipFile("/Users/rkoeser/Downloads/1279201/allmeta.csv.zip").read(
            "allmeta.csv"
        ),
        infer_schema_length=10000,
        ignore_errors=True,
    )
    genre_all_meta_df
    return (genre_all_meta_df,)


@app.cell
def _(genre_all_meta_df, ppa_ht_df):
    ppa_genre_meta = ppa_ht_df.join(
        genre_all_meta_df, left_on="ppa_source_id", right_on="htid", how="inner"
    )
    ppa_genre_meta
    return (ppa_genre_meta,)


@app.cell
def _(genre_all_meta_df, mo, ppa_genre_meta, ppa_ht_df):
    ppa_genre_meta.write_csv("data/ppa/ppa_genre_metadata_overlap.csv")


    mo.md(f"""
    - {genre_all_meta_df.height:,} records in page-level genre all-metadata
    - {ppa_ht_df.height:,} records in PPA from HathiTrust (excerpts included)
    - {ppa_genre_meta.height:,} PPA HathiTrust records matched in page-level genre metadata.
    """)
    return


if __name__ == "__main__":
    app.run()
