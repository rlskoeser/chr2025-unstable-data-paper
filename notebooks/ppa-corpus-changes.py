import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import pathlib

    import marimo as mo
    import polars as pl
    return mo, pathlib, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # PPA full-text corpus changes

    Comparison of two different versions of the PPA page-level full-text corpus. 

    Both were exported February 2025, but one was based on older data and one on a fresh rsync copy.

    What are the high level differences?
    """
    )
    return


@app.cell
def _(pathlib, pl):
    PPA_DATA_DIR = pathlib.Path("data/ppa/")

    ppa_corpus_newer = pl.read_ndjson(
        PPA_DATA_DIR / "ppa_corpus_2025-02-19/ppa_pages.jsonl.gz"
    )
    # limit to HathiTrust content only, based on source id; non-hathi ids have known patterns
    ppa_corpus_newer = ppa_corpus_newer.filter(
        # EEBO-TPC work id is A followed by numbers
        # Gale/ECCO work id is CW/CB followed by numbers
        # both could have an optional -p## for excerpt start page
        ~pl.col("work_id").str.contains(r"^((A\d+)|(C[WB]\d+))(-p.+)?$")
    )

    ppa_corpus_newer.head(10)
    return PPA_DATA_DIR, ppa_corpus_newer


@app.cell
def _(PPA_DATA_DIR, pl):
    ppa_corpus_frozen = pl.read_ndjson(
        PPA_DATA_DIR / "ppa_corpus_2025-02-03_1308/ppa_pages.jsonl.gz"
    )
    ppa_corpus_frozen = ppa_corpus_frozen.with_columns(
        work_id_prefix=pl.col("work_id").str.slice(0, 2)
    ).filter(~pl.col("work_id").str.contains(r"^((A\d+)|(C[WB]\d+))(-p.+)?$"))

    ppa_corpus_frozen.head()
    return (ppa_corpus_frozen,)


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""How many pages in these two versions? Any difference by raw page count?"""
    )
    return


@app.cell
def _(mo, ppa_corpus_frozen, ppa_corpus_newer):
    total_ht_pages_frozen = ppa_corpus_frozen.height
    total_ht_pages_newer = ppa_corpus_newer.height

    mo.md(f"""
    - {total_ht_pages_frozen:,} pages in frozen production data
    - {total_ht_pages_newer:,} pages in 2025-02-19 rsync copy

    {total_ht_pages_frozen - total_ht_pages_newer:,} more pages in the earlier version

    """)
    return (total_ht_pages_frozen,)


@app.cell
def _(mo):
    mo.md(r"""Are there changes in the number of volumes ?""")
    return


@app.cell
def _(mo, ppa_corpus_frozen, ppa_corpus_newer):
    total_works_frozen = ppa_corpus_frozen["work_id"].unique().len()
    total_works_newer = ppa_corpus_newer["work_id"].unique().len()

    mo.md(f"""
    - {total_works_frozen:,} total unique HT ids in frozen production data
    - {total_works_newer:,} total unique HT ids in rsync copy

    difference: {total_works_frozen - total_works_newer} volumes
    """)
    # print(total_works_202404, total_works_202502)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""First compare by page ids. How many match, and of those how many have the same text content?"""
    )
    return


@app.cell
def _(pl, ppa_corpus_frozen, ppa_corpus_newer):
    pageid_join = ppa_corpus_frozen.join(
        ppa_corpus_newer, on="id", how="inner"
    ).with_columns(text_equal=pl.col("text").eq(pl.col("text_right")))

    pageid_join.head(10)
    return (pageid_join,)


@app.cell
def _(mo, pageid_join, pl):
    total_shared_pageids = pageid_join.height
    total_pages_text_eq = pageid_join.filter(pl.col("text_equal")).height

    mo.md(f"""{total_shared_pageids:,} page ids in both versions

    {total_pages_text_eq:,}  pages with unchanged text content — {(total_pages_text_eq / total_shared_pageids) * 100:.1f}%!

    for {total_shared_pageids - total_pages_text_eq:,} pages ({((total_shared_pageids - total_pages_text_eq) / total_shared_pageids) * 100:.1f}%) the text has changed
    """)
    return


@app.cell
def _(pl, ppa_corpus_newer):
    # how many pages are null? 38,505
    # ppa_corpus_202404.filter(pl.col("text").is_null())
    # how many are empty string? 12,469
    ppa_corpus_newer.filter(pl.col("text").str.strip_chars().eq(""))
    return


@app.cell
def _(pl, ppa_corpus_frozen, ppa_corpus_newer):
    # what if we join on exact text?
    # DUH filter out empty pages so they don't all match each other
    # join on matching page and same work id
    pagetext_join = (
        ppa_corpus_frozen.filter(~pl.col("text").is_null())
        .filter(~pl.col("text").str.strip_chars().eq(""))
        .join(
            ppa_corpus_newer.filter(~pl.col("text").is_null()).filter(
                ~pl.col("text").str.strip_chars().eq("")
            ),
            on=["text", "work_id"],
            how="inner",
        )
        .with_columns(id_equal=pl.col("id").eq(pl.col("id_right")))
    )

    pagetext_join.head(10)
    return (pagetext_join,)


@app.cell
def _(mo, pagetext_join, pl, total_ht_pages_frozen):
    total_shared_pagetext = pagetext_join.height
    total_pagetext_id_eq = pagetext_join.filter(pl.col("id_equal")).height

    mo.md(f"""{total_shared_pagetext:,} pages with exactly matching text in both versions

    {total_pagetext_id_eq:,}  pages with matching text have unchanged page ids — {(total_pagetext_id_eq / total_shared_pagetext) * 100:.1f}% of this set; {(total_pagetext_id_eq / total_ht_pages_frozen) * 100:.1f}% of frozen corpus
    
    for {total_shared_pagetext - total_pagetext_id_eq:,} pages ({((total_shared_pagetext - total_pagetext_id_eq) / total_shared_pagetext) * 100:.1f}%) the id has changed
    """)
    return


if __name__ == "__main__":
    app.run()
