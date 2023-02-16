import pandas as pd


def select_best_transition(df_rank):
    """
    Based on the ranking, select the best transition

    Args:
        df_rank:

    Returns:
        The highest ranking technology transition

    """
    return (
        df_rank[df_rank["rank"] == df_rank["rank"].min()]
        .sample(n=1)
        .to_dict(orient="records")
    )[0]
