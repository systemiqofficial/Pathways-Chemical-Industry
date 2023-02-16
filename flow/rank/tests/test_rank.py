import pandas as pd

from flow.rank.rank_technologies import bin_ranking, rank_per_year


def _make_options_df(options: list[tuple], binned=True):
    columns = [
        "origin",
        "type_of_tech_origin",
        "destination",
        "type_of_tech_destination",
        "emissions_scope_1_2_delta",
        "emissions_scope_3_upstream_delta",
        "lcox",
    ]

    df = pd.DataFrame(data=options, columns=columns)
    df["emissions_scope_1_2_3_upstream_delta"] = (
        df["emissions_scope_1_2_delta"] + df["emissions_scope_3_upstream_delta"]
    )

    for col in df.columns:
        if (
            col
            not in [
                "origin",
                "destination",
                "type_of_tech_destination",
                "type_of_tech_origin",
            ]
            and binned
        ):
            df.rename(columns={col: col + "_binned"}, inplace=True)
    return df


def _test_rank_template(
    options: list[tuple], rank_type: str, pathway: str, get_one=True, year=None
):
    """
    Template to test ranking;
    Args:
        options:

    Returns:

    """

    df = _make_options_df(options)
    df_rank = rank_per_year(
        df_rank=df,
        rank_type=rank_type,
        pathway=pathway,
        year=year or 2030,
        initial_tech_allowed_until_year=2025,
    )
    results = df.loc[df_rank["rank"] == df_rank["rank"].min(), "destination"].values
    if get_one:
        return results[0]
    return results


def test_same_rank():
    kwargs = dict(rank_type="new_build", pathway="me")

    # Same values should get same rank
    options = [
        ("Non existent", 1, "same", 1, 100, 100, 90),
        ("Non existent", 1, "samesame", 1, 100, 100, 90),
        ("Non existent", 1, "different", 1, 100, 100, 100),
    ]

    assert len(_test_rank_template(options, **kwargs, get_one=False)) == 2


def test_new_build_initial_tech():
    kwargs = dict(rank_type="new_build", pathway="me", year=2020)

    # Should not care about rank type because we are in 2020 which is before initial tech cutoff
    options = [
        ("Non existent", 1, "initial", 1, 100, 100, 90),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "initial"


def test_new_build_most_economic():
    kwargs = dict(rank_type="new_build", pathway="me")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # ..then LCOX
    options = [
        ("Non existent", 1, "cheap", 1, 110, 100, 90),
        ("Non existent", 1, "expensive", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"

    # .. then on scope 1/2
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "dirty", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "clean"

    # .. then on scope 3 upstream
    options = [
        ("Non existent", 1, "cleanest", 1, 100, 90, 100),
        ("Non existent", 1, "clean", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "cleanest"


def test_new_build_fast_abatement():
    kwargs = dict(rank_type="new_build", pathway="fa")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # .. then on scope 1/2
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "dirty", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "clean"

    # .. then on scope 3 upstream
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "cleanest", 1, 90, 90, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "cleanest"

    # .. then on lcox
    options = [
        ("Non existent", 1, "expensive", 1, 100, 100, 100),
        ("Non existent", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_new_build_no_fossil():
    kwargs = dict(rank_type="new_build", pathway="nf")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # .. then on scope 1/2/3_upstream
    options = [
        ("Non existent", 1, "high_scope_3", 1, 90, 120, 100),
        ("Non existent", 1, "low_scope_3", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "low_scope_3"

    # .. then on lcox
    options = [
        ("Non existent", 1, "expensive", 1, 100, 100, 100),
        ("Non existent", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_retrofit_most_economic():
    kwargs = dict(rank_type="retrofit", pathway="me")

    # Favor retrofitting old tech first
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("transition", 2, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # .. then favor going to end state
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("initial", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # .. then rank on LCOX
    options = [
        ("x", 1, "cheap", 1, 100, 100, 90),
        ("x", 1, "expensive", 1, 110, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"

    # .. then on scope 1/2 -> biggest delta wins
    options = [
        ("x", 1, "most_abated", 1, 100, 100, 100),
        ("x", 1, "least_abated", 1, 90, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated"

    # .. then on scope 3 upstream
    options = [
        ("x", 1, "most_abated", 1, 100, 100, 100),
        ("x", 1, "least_abated", 1, 100, 90, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated"


def test_retrofit_fast_abatement():
    kwargs = dict(rank_type="retrofit", pathway="fa")

    # Favor retrofitting old tech first
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("transition", 2, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # .. then favor going to end state
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("initial", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # Then rank on scope 1/2
    options = [
        ("x", 1, "most_abated", 1, 100, 100, 100),
        ("x", 1, "least_abated", 1, 90, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated"

    # .. then on scope 3 upstream
    options = [
        ("x", 1, "most_abated", 1, 90, 100, 100),
        ("x", 1, "least_abated", 1, 90, 90, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated"

    # .. then on lcox
    options = [
        ("x", 1, "expensive", 1, 100, 100, 100),
        ("x", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_retrofit_no_fossil():
    kwargs = dict(rank_type="retrofit", pathway="nf")

    # Favor retrofitting old tech first
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("transition", 2, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # .. then favor going to end state
    options = [
        ("initial", 1, "transition", 2, 100, 100, 100),
        ("initial", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "end_state"

    # then rank on scope 1/2/3_upstream
    options = [
        ("x", 1, "most_abated_scope_3", 1, 90, 120, 100),
        ("x", 1, "least_abated_scope_3", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated_scope_3"

    # .. then on lcox
    options = [
        ("x", 1, "expensive", 1, 100, 100, 100),
        ("x", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_decommission_most_economic():
    kwargs = dict(rank_type="decommission", pathway="me")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # Then on LCOX
    options = [
        ("Non existent", 1, "cheap", 1, 100, 100, 90),
        ("Non existent", 1, "expensive", 1, 120, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"

    # .. then on scope 1/2
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "dirty", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "dirty"

    # .. then on scope 3 upstream
    options = [
        ("Non existent", 1, "cleanest", 1, 100, 90, 100),
        ("Non existent", 1, "clean", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "clean"


def test_decommission_fast_abatement():
    kwargs = dict(rank_type="decommission", pathway="fa")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # Rank on scope 1/2 first
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "dirty", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "dirty"

    # .. then on scope 3 upstream
    options = [
        ("Non existent", 1, "clean", 1, 90, 100, 100),
        ("Non existent", 1, "cleanest", 1, 90, 90, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "clean"

    # .. then on lcox
    options = [
        ("Non existent", 1, "expensive", 1, 100, 100, 100),
        ("Non existent", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_decommission_no_fossil():
    kwargs = dict(rank_type="decommission", pathway="nf")

    # Rank on tech type first
    options = [
        ("Non existent", 1, "transition", 2, 100, 100, 100),
        ("Non existent", 1, "end_state", 3, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "transition"

    # Rank on scope 1/2/3_upstream first
    options = [
        ("x", 1, "most_abated_scope_3", 1, 90, 120, 100),
        ("x", 1, "least_abated_scope_3", 1, 100, 100, 100),
    ]

    assert _test_rank_template(options, **kwargs) == "most_abated_scope_3"

    # .. then on lcox
    options = [
        ("Non existent", 1, "expensive", 1, 100, 100, 100),
        ("Non existent", 1, "cheap", 1, 100, 100, 90),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap"


def test_bau():
    """BAU shouldn't care about tech type"""
    kwargs = dict(rank_type="new_build", pathway="bau")

    # Rank on cost first
    options = [
        ("Non existent", 1, "cheap_initial", 1, 100, 100, 100),
        ("Non existent", 1, "expensive_end_state", 3, 100, 100, 150),
    ]

    assert _test_rank_template(options, **kwargs) == "cheap_initial"


def test_bin_ranking():
    emissions_options = [
        ("Non existent", 1, "clean", 1, 99.8, 100, 100),
        ("Non existent", 1, "almost_as_clean", 1, 100, 100, 100),
        ("Non existent", 1, "dirty", 1, 120, 100, 100),
    ]
    rank_var = "emissions_scope_1_2_delta"
    df_rank = _make_options_df(emissions_options, binned=False)
    df_rank[rank_var + "_binned"] = bin_ranking(rank_array=df_rank[rank_var])

    # Close numbers end up in the same bin
    assert (
        df_rank.loc[df_rank.destination == "clean", rank_var + "_binned"].values[0]
        == df_rank.loc[
            df_rank.destination == "almost_as_clean", rank_var + "_binned"
        ].values[0]
    )

    # Numbers that are far apart don't
    assert (
        df_rank.loc[df_rank.destination == "clean", rank_var + "_binned"].values[0]
        != df_rank.loc[df_rank.destination == "dirty", rank_var + "_binned"].values[0]
    )
