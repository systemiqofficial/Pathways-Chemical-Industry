import pandas as pd

STEAM_CRACKER_TECH = [
    "Naphtha steam cracking",
    "Naphtha steam cracking + by-product upgrade",
    "Naphtha steam cracking + CCS",
    "Naphtha electric steam cracking",
    "Naphtha H2 steam cracking",
    "Pyrolysis oil steam cracking",
    "Bio-oils steam cracking",
    "Naphtha steam cracking + CCS + by-product upgrade",
    "Naphtha electric steam cracking + by-product upgrade",
    "Naphtha H2 steam cracking + by-product upgrade",
    "Pyrolysis oil steam cracking + CCS + by-product upgrade",
    "Pyrolysis oil electric steam cracking + by-product upgrade",
    "Pyrolysis oil electric steam cracking + CCS + by-product upgrade",
    "Pyrolysis oil H2 steam cracking + by-product upgrade",
    "Pyrolysis oil H2 steam cracking + CCS + by-product upgrade",
    "Bio-oils steam cracking + by-product upgrade",
    "Bio-oils electric steam cracking + by-product upgrade",
    "Bio-oils H2 steam cracking + by-product upgrade",
]


def convert_df_to_steam_crackers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a dataframe that has "Steam cracker" and others as technology to the different steam cracker tech.

    Args:
        df: Dataframe with mixed values

    Returns:
        Dataframe with only regional values
    """

    # Separate cracker and non cracker df
    cracker_idx = df.technology == "Steam cracker"
    df_cracker = df[cracker_idx].copy()
    df_other_tech = df[~cracker_idx]

    # Convert 'Steam cracker' to all steam cracker techs
    df_cracker.drop(columns="technology", inplace=True)
    df_tech = pd.DataFrame({"technology": list(STEAM_CRACKER_TECH)})
    df_cracker_tech = df_cracker.merge(df_tech, how="cross")

    # Return the df with all tech
    return pd.concat([df_cracker_tech, df_other_tech])


REGIONS = {
    "Africa",
    "China",
    "Europe",
    "Latin America",
    "Middle East",
    "North America",
    "Japan",
    "Russia",
    "India",
    "Rest of Asia and Pacific",
}


def convert_df_to_regional(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a dataframe that has both regional and global values to one that just has regional values.

    Args:
        df: Dataframe with mixed values

    Returns:
        Dataframe with only regional values
    """

    # Separate world and regional df
    world_idx = df.region == "World"
    df_world = df[world_idx].copy()
    df_regional_1 = df[~world_idx]

    # Regionalize the world df
    df_world.drop(columns="region", inplace=True)
    df_regions = pd.DataFrame({"region": list(REGIONS)})
    df_regional_2 = df_world.merge(df_regions, how="cross")

    # Return the region df
    return pd.concat([df_regional_1, df_regional_2])
