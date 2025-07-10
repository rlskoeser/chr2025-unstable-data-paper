import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(mo):
    mo.md(
        r"""
    # PPA excerpt changes


    A spreadsheet from 2023-09 was used to adjust the page ranges for excerpts that had shifted due to updates in HathiTrust content.  That manual data work provides a window into the kinds of changes seen in PPA content.
    """
    )
    return


@app.cell
def _(pl):
    excerpt_df = pl.read_csv("data/ppa/excerpts-2023-09-20.csv")
    excerpt_df
    return (excerpt_df,)


@app.cell
def _(excerpt_df, mo, pl):
    changed = excerpt_df.filter(pl.col("new digital range").ne("correct"))
    total_excerpts = excerpt_df.height
    total_changed = changed.height
    total_unchanged = total_excerpts - total_changed
    percent_unchanged = (total_unchanged / total_excerpts) * 100
    percent_changed = (total_changed / total_excerpts) * 100

    mo.md(f"""
    Of {total_excerpts:,} total excerpts, there were {total_unchanged:,} excerpts ({percent_unchanged:.1f}%) where the digital page range was unchanged.

    {total_changed:,} excerpts had changed ({percent_changed:.1f}%).
    """)
    return (changed,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""For the excerpts that changed, how much did they change?""")
    return


@app.cell
def _(changed, pl):
    from intspan import intspan

    # limit to page ranges and page count
    # parse new and old digital page range with int span so we can compare
    changed_pages = (
        changed.filter(pl.col("new digital range").ne("SUPPRESS"))
        .select(
            "source_url",
            "title",
            "author",
            "pages_orig",
            "pages_digital",
            "new digital range",
            "page_count",
        )
        .with_columns(
            pl.col("pages_digital")
            .map_elements(lambda x: intspan(x), return_dtype=object)
            .alias("pages_digital_orig"),
            pl.col("new digital range")
            .map_elements(lambda x: intspan(x), return_dtype=object)
            .alias("pages_digital_new"),
        )
    )

    # calculate page count for new/old and compare
    changed_pages = (
        changed_pages.with_columns(
            # convert intspans into list of pages, to simplify further calculations,
            # since polars can work easily with lists
            orig_pages=pl.col("pages_digital_orig").map_elements(
                lambda x: list(x),
                return_dtype=pl.datatypes.List(pl.datatypes.Int32),
            ),
            new_pages=pl.col("pages_digital_new").map_elements(
                lambda x: list(x),
                return_dtype=pl.datatypes.List(pl.datatypes.Int32),
            ),
        )
        .with_columns(
            orig_page_count=pl.col("orig_pages").list.len(),
            new_page_count=pl.col("new_pages").list.len(),
            orig_start_page=pl.col("orig_pages").list.first(),
            new_start_page=pl.col("new_pages").list.first(),
        )
        .with_columns(
            # cast to same integer type to avoid getting a weird signed/unsigned int math bogus result
            page_count_diff=pl.col("orig_page_count")
            .cast(pl.Int32)
            .sub(pl.col("new_page_count").cast(pl.Int32))
            .abs(),
            start_page_diff=pl.col("orig_start_page")
            .sub(pl.col("new_start_page"))
            .abs(),
            # use set intersection to determine # overlapping pages between old and new page range
            common_pages=pl.col("orig_pages").list.set_intersection(
                pl.col("new_pages")
            ),
        )
        # then count the pages included in both
        .with_columns(
            num_common_pages=pl.col("common_pages").list.len(),
        )
        .with_columns(
            # also calculate as a percentage of the excerpt, since they vary in size; use the new/corrected page count
            pct_common_pages=pl.col("num_common_pages").truediv(
                pl.col("new_page_count")
            )
        )
    )

    changed_pages
    return (changed_pages,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    For a few excerpts, the number of pages included in the excerpt changed.

    There are a couple of outliers with very large changes (25 and 16 page differences); omit them for reporting, since they are likely due to data errors in the original excerpt.
    """
    )
    return


@app.cell
def _(changed_pages, pl):
    page_count_changed = changed_pages.filter(
        pl.col("page_count_diff").ne(0)
    ).filter(pl.col("page_count_diff").lt(15))
    page_count_changed["page_count_diff"].describe()
    return (page_count_changed,)


@app.cell
def _(mo, page_count_changed):
    mo.md(
        ", ".join(
            [str(n) for n in page_count_changed["page_count_diff"].to_list()]
        )
    )
    return


@app.cell
def _(mo, page_count_changed):
    mo.md(
        f"""
    For {page_count_changed.height} excerpts, the number of pages changed.

    - The largest change was {page_count_changed["page_count_diff"].max()} pages.
    - The average change was {page_count_changed["page_count_diff"].mean()} pages.
    - The most common change was {page_count_changed["page_count_diff"].mode().first()} pages.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""When excerpts shifted, how many pages did they typically shift?""")
    return


@app.cell
def _(changed_pages):
    changed_pages["start_page_diff"].describe()
    return


@app.cell
def _(changed_pages):
    import altair as alt

    alt.Chart(changed_pages).mark_bar(width=10).encode(
        x=alt.X("start_page_diff", title="Difference in start pages"), y="count()"
    )
    return (alt,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Maybe a box plot will be an easier way to show the distribution of changes."""
    )
    return


@app.cell
def _(alt, changed_pages):
    alt.Chart(changed_pages).mark_boxplot().encode(
        alt.X("start_page_diff", title="Difference in start pages")
    )
    return


@app.cell
def _(alt, changed_pages, pl):
    alt.Chart(
        changed_pages.filter(pl.col("start_page_diff").lt(100))
    ).mark_boxplot().encode(
        alt.X(
            "start_page_diff",
            title="Difference in start pages (240 page difference excluded)",
        )
    )
    return


@app.cell
def _(changed_pages, mo):
    mo.md(
        f"""
    Differences in start page:

    - The largest change was {changed_pages["start_page_diff"].max()} pages.
    - The average change was {changed_pages["start_page_diff"].mean():.1f} pages.
    - The most common change was {changed_pages["start_page_diff"].mode().first()} pages.
    """
    )
    return


@app.cell
def _(changed_pages, pl):
    changed_pages.filter(pl.col("start_page_diff").gt(10))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Nine volumes have a difference of more than 10 pages.""")
    return


@app.cell
def _(changed_pages, pl):
    changed_pages.filter(pl.col("start_page_diff").gt(20))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Five volumes have a difference of more than 20 pages; one has a difference of 240."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""How many pages do the old and new ranges typically have in common?"""
    )
    return


@app.cell
def _(changed_pages):
    changed_pages["num_common_pages"].describe()
    return


@app.cell
def _(alt, changed_pages):
    alt.Chart(changed_pages).mark_bar(width=10).encode(
        x=alt.X(
            "num_common_pages",
            title="Number of pages in common between new and old digital page range",
        ),
        y="count()",
    )
    return


@app.cell
def _(alt, changed_pages):
    alt.Chart(changed_pages).mark_boxplot().encode(
        x=alt.X(
            "num_common_pages",
            title="# pages in common",
        )
    ).properties(
        title="Number of pages in common between new and old digital page range"
    )
    return


@app.cell
def _(alt, changed_pages):
    alt.Chart(changed_pages).mark_bar(width=10).encode(
        x=alt.X(
            "pct_common_pages",
            title="Percent of pages in common between new and old digital page range",
        ),
        y="count()",
    )
    return


@app.cell
def _(changed_pages, mo, pl):
    no_pages_in_common = changed_pages.filter(
        pl.col("num_common_pages").eq(0)
    ).height


    mo.md(
        f"{no_pages_in_common} excerpts have NO pages in common with the updated digital page range."
    )
    return


if __name__ == "__main__":
    app.run()
