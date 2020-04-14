import pandas as pd
import numpy as np
from sqlalchemy import Integer, String, BigInteger


def load_data(raw_root, years):
    results = [
        pd.read_excel(raw_root + "results/" + str(year)
                      + "GeneralPrecinctResults.xlsx")
        for year in years
        ]
    return results


def clean_data(raw_results):
    formatted_results = [format_results(df) for df in raw_results]
    cand = []
    yesno = []
    for res in formatted_results:
        c, yn = break_df_by_race_type(res)
        cand.append(c)
        yesno.append(yn)
    cand = pd.concat(cand).reset_index(drop=True)
    yesno = pd.concat(yesno).reset_index(drop=True)
    return cand, yesno


def write_data(cand, yesno, engine):
    cand_dtype = {
        'year': Integer,
        'county': String,
        'precinct': BigInteger,
        'issue': String,
        'candidate': String,
        'party': String,
        'candidates_votes': Integer
    }
    with engine.connect() as conn:
        cand.to_sql('candidate_vote_results', conn, if_exists='replace',
                    index=True, index_label=None,
                    dtype=cand_dtype)

    yesno_dtype = {
        'year': Integer,
        'county': String,
        'precinct': BigInteger,
        'issue': String,
        'candidate': String,
        'yes_votes': Integer,
        'no_votes': Integer
    }
    with engine.connect() as conn:
        cand.to_sql('issue_vote_results', conn, if_exists='replace',
                    index=True, index_label=None,
                    dtype=yesno_dtype)


def format_results(df):
    new_df = (df.rename(columns=str.lower)
                .drop(columns=["election type", "state"])
                .rename(columns={"office/issue/judgeship": "issue",
                                 "candidate votes": "candidate_votes",
                                 "yes votes": "yes_votes",
                                 "no votes": "no_votes"}))
    mask = new_df.precinct.map(lambda x: x == 'Provisional')
    provisional_precincts = new_df[mask].index
    new_df = new_df.drop(index=provisional_precincts)
    return new_df


def is_race_candidate(df, race, race_col="issue",
                      cand_col="candidate_votes", yes_col="yes_votes",
                      no_col="no_votes"):
    data = df[df[race_col] == race]
    cand_votes = np.sum(data[cand_col])
    yesno_votes = np.sum(data[yes_col]) + np.sum(data[no_col])
    if cand_votes == 0 and yesno_votes > 0:
        return False
    elif cand_votes > 0 and yesno_votes == 0:
        return True
    elif cand_votes == 0 and yesno_votes == 0:
        raise ValueError("Ambiguous")
    else:
        raise ValueError("Super weird")


def break_df_by_race_type(df, race_col="issue",
                          cand_col="candidate_votes", yes_col="yes_votes",
                          no_col="no_votes"):

    races = list(set(df[race_col].values))

    candidate_races = [race for race in races
                       if is_race_candidate(df, race, race_col,
                                            cand_col, yes_col, no_col)]
    yesno_races = [race for race in races
                   if not is_race_candidate(df, race, race_col,
                                            cand_col, yes_col, no_col)]
    mask = df[race_col].map(lambda x: x in candidate_races)
    cand_df = df[mask].drop(columns=[yes_col, no_col])
    mask = df[race_col].map(lambda x: x in yesno_races)
    yesno_df = df[mask].drop(columns=['party', cand_col])
    return cand_df, yesno_df
