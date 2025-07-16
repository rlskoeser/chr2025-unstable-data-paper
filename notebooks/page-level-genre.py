import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(mo):
    mo.md(
        r"""
    # HathiTrust page-level genre predictions and PPA

    This analysis is based on:

    > Underwood, Ted (2014). Page-Level Genre Metadata for English-Language Volumes in HathiTrust, 1700-1922. figshare. Dataset. https://doi.org/10.6084/m9.figshare.1279201.v1

    To replicate this analysis, download the full zip file and expand it in the `data/` directory.
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Identify overlap between page-level genre volumes and PPA

    First, we need to find which PPA volumes are included in this dataset.
    """
    )
    return


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
        ZipFile("data/1279201/allmeta.csv.zip").read("allmeta.csv"),
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
def _(ppa_ht_df):
    ppa_ht_df.columns
    return


@app.cell
def _(genre_all_meta_df, mo, ppa_genre_meta, ppa_ht_df):
    ppa_genre_meta.write_csv("data/ppa/ppa_genre_metadata_overlap.csv")


    mo.md(f"""
    - {genre_all_meta_df.height:,} records in page-level genre all-metadata
    - {ppa_ht_df.height:,} records in PPA from HathiTrust (excerpts included)
    - {ppa_genre_meta.height:,} PPA HathiTrust records matched in page-level genre metadata.
    """)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Test cases

    Based on the documented overlap between the datasets, we identified three test cases that would be interesting and helpful to look at.


    ```
    njp.32101068158847-p223 – excerpt didn’t change
    Each page has some poetry on it (OG 223-228, digital 267-272)


    njp.32101076530979-p482 – excerpt did change
    OG page 496 and digital page 504 – quoted poetry 
    OG page 498 and digital page 506 – quoted poetry
    OG page 499 and digital page 507 – big block of quoted poetry
    OG page 501 and digital page 509 – quoted poetry
    OG page 502 and digital page 510 – quoted poetry
    OG page 505 and digital page 513 – quoted poetry
    OG page 507 and digital page 515 – ALL POETRY
    OG page 508 and digital page 516 – almost all poetry
    OG page 509 and digital page 517 – quoted poetry
    OG page 510 and digital page 518 – quoted poetry
    OG page 511 and digital page 519 – quoted poetry
    OG page 512 and digital page 520 – about half quoted poetry

    nnc1.0035529865 – not an excerpt, but updated 2023-09-14
    Lots of poetry interspersed throughout with prose
    Section toward the end with just poems
    ```
    """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""To determine which data file of page-level genre predictions to look at, we need to know when our test cases were published.  It turns out that they are all 1700s, so we can find predictions for all of them in a single data file of the page-level genre dataset."""
    )
    return


@app.cell
def _(pl, ppa_ht_df):
    # ppa work ids for three test cases of interest
    test_cases = [
        "njp.32101068158847-p223",
        "njp.32101076530979-p482",
        "nnc1.0035529865",
    ]

    # page-level genre data is segmented by century, so to determine which data file we need to know the publication years

    ppa_test_cases = ppa_ht_df.filter(
        pl.col("ppa_work_id").is_in(test_cases)
    ).select("ppa_work_id", "ppa_source_id", "ppa_work_title", "ppa_work_year")
    ppa_test_cases
    return (ppa_test_cases,)


@app.cell
def _(ppa_test_cases):
    import pathlib
    import json

    genre_data_dir = pathlib.Path("data/1279201")
    all1700_dir = genre_data_dir / "all" / "1700-99"

    genre_data_by_work = {}


    for row in ppa_test_cases.iter_rows(named=True):
        print(f"\n{row['ppa_work_id']} : {row['ppa_work_title']}")

        # page-level genre data is in a json file named based on the hathi id
        # current test sets don't include any ARKs; if we expand, translate ids to filenames
        with open(all1700_dir / f"{row['ppa_source_id']}.json") as jsonfile:
            genre_data = json.load(jsonfile)

        # display summary information
        genre_counts = genre_data["added_metadata"]["genre_counts"]
        most_common_genre = genre_data["added_metadata"]["maxgenre"]
        print(f"most common genre: {most_common_genre}")
        print("pages by genre:")
        for genre, total in genre_counts.items():
            print(f"\t{genre}: {total}")

        # genre data also includes accuracy scores, could output them also perhaps

        # save the loaded genre data so we can look at pages of interest for our test cases
        genre_data_by_work[row["ppa_work_id"]] = genre_data
    return (genre_data_by_work,)


@app.cell
def _():
    # define a utility method so we can easily see sequential ranges of pages in the same genre

    from intspan import intspan


    def page_genre_chunks(page_genres):
        chunks = []
        current_chunk = []
        current_genre = None

        for page, genre in page_genres.items():
            # special case for the first round
            if current_genre is None:
                current_genre = genre
            # if genre matches, append to current chunk
            if genre == current_genre:
                current_chunk.append(int(page))
            # if genre has changed, save the last chunk and start a new one
            else:
                chunks.append((current_genre, current_chunk))
                current_genre = genre
                current_chunk = [int(page)]

        return chunks


    def print_page_chunks(chunks):
        for chunk_genre, page_range in chunks:
            print(f"{chunk_genre}: {intspan(page_range)}")
    return page_genre_chunks, print_page_chunks


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Test case 1: njp.32101068158847-p223 

    This excerpt didn’t change.

    Each page has some poetry on it (OG 223-228, digital 267-272)
    """
    )
    return


@app.cell
def _(genre_data_by_work):
    # page genre is a dictionary with numeric keys, presumably for the digital page index (starts with zero)
    page_genres = genre_data_by_work["njp.32101068158847-p223"]["page_genres"]

    # filter to our page range of interest
    {
        page: genre
        for page, genre in page_genres.items()
        # if int(page) >= 267 and int(page) <= 272
        if int(page) >= 250 and int(page) <= 280
    }
    # page_genres
    return (page_genres,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    The genre predictions for this are all "non"; even if we expand the page range, all the pages nearby are "non". 

    There is a preceding section predicted as "bio"; if we could figure out what this is, we might be able to use it to orient ourselves to the changes (but perhaps unhelpful since no poetry predictions).

    ---

    When we look at chunks of sequential pages in the same genre, there's a fair bit of variety.
    """
    )
    return


@app.cell
def _(page_genre_chunks, page_genres, print_page_chunks):
    print_page_chunks(page_genre_chunks(page_genres))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Test Case 2: njp.32101076530979-p482 

    This excerpt did change.

    - OG page 496 and digital page 504 – quoted poetry 
    - OG page 498 and digital page 506 – quoted poetry
    - OG page 499 and digital page 507 – big block of quoted poetry
    - OG page 501 and digital page 509 – quoted poetry
    - OG page 502 and digital page 510 – quoted poetry
    - OG page 505 and digital page 513 – quoted poetry
    - OG page 507 and digital page 515 – ALL POETRY
    - OG page 508 and digital page 516 – almost all poetry
    - OG page 509 and digital page 517 – quoted poetry
    - OG page 510 and digital page 518 – quoted poetry
    - OG page 511 and digital page 519 – quoted poetry
    - OG page 512 and digital page 520 – about half quoted poetry
    """
    )
    return


@app.cell
def _(genre_data_by_work):
    # page genre is a dictionary with numeric keys, presumably for the digital page index (starts with zero)
    page_genres2 = genre_data_by_work["njp.32101076530979-p482"]["page_genres"]

    # filter to page range of interest
    {
        page: genre
        for page, genre in page_genres2.items()
        if int(page) >= 504 and int(page) <= 520
    }
    return (page_genres2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    When we look at the pages in range for this excerpt, we see only two pages predicted with the genre of poetry (510 and 511).

    We suspect that these correspond to the two pages that are all or almost all poetry (515 and 516 in our enumeration), but we're not sure if there's any way to be sure.
    """
    )
    return


@app.cell
def _(page_genre_chunks, page_genres2, print_page_chunks):
    print_page_chunks(page_genre_chunks(page_genres2))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""When we look at sequences of pages in the same genre, we see that those two pages of poetry were the only content pages flagged with any genre."""
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Test Case 3: nnc1.0035529865 

    This volume is not an excerpt, but one we know was updated 2023-09-14.


    - Lots of poetry interspersed throughout with prose
    - Section toward the end with just poems
    """
    )
    return


@app.cell
def _(genre_data_by_work, page_genre_chunks, print_page_chunks):
    # page genre is a dictionary with numeric keys, presumably for the digital page index (starts with zero)
    page_genres3 = genre_data_by_work["nnc1.0035529865"]["page_genres"]

    # this is a long volume with sequences of pages of the same predicted genre
    # can we aggregate them into chunks so it's easier to scan?

    print_page_chunks(page_genre_chunks(page_genres3))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Reporting on pages in the same genre indicates a substantial portion of the volume is predicted to be poetry."""
    )
    return


if __name__ == "__main__":
    app.run()
