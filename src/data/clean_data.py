from sqlalchemy import create_engine

import clean_precincts as pre
import clean_registration as reg
import clean_results as res


if __name__ == '__main__':
    raw_root = "../../data/raw/"
    interim_root = "../../data/interim/"

    engine = create_engine('postgresql+psycopg2:///interim')

    # PRECINCT SHAPEFILES
    print("Loading precinct shapefiles")
    counties, precincts = pre.load_data(raw_root)
    print("Cleaning precinct shapefiles")
    clean_precincts = pre.clean_data(raw_root, counties, precincts)
    print("Writing precinct shapefile data to database")
    pre.write_data2(engine, counties, clean_precincts)

    # VOTER REGISTRATION DATA
    print("Loading voter registration data")
    raw_registrations = reg.load_data(raw_root)
    print("Cleaning voter registration data")
    co_voters_table, voters_per_year = reg.clean_data(raw_registrations)
    print("Writing voter registration data to database")
    reg.write_data(engine, co_voters_table, voters_per_year)

    # VOTING RESULTS
    print("Loading vote results")
    raw_results = res.load_data(raw_root, [2016, 2018])
    print("Cleaning vote results")
    cand, yesno = res.clean_data(raw_results)
    print("Writing vote results to database")
    res.write_data(cand, yesno, engine)
