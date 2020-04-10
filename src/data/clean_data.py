import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from postgis.psycopg import register

import clean_precincts as pre
import clean_registration as reg


if __name__ == '__main__':
    raw_root = "../../data/raw/"
    interim_root = "../../data/interim/"

    # PRECINCT SHAPEFILES
    print("Loading precinct shapefiles")
    counties, precincts = pre.load_data(raw_root)
    print("Cleaning precinct shapefiles")
    clean_precincts = pre.clean_data(raw_root, counties, precincts)

    # CLEAN VOTER REGISTRATION DATA
    print("Loading voter registration data")
    raw_registrations = reg.load_data(raw_root)
    print("Cleaning voter registration data")
    co_voters_table, voters_per_year = reg.clean_data(raw_registrations)

    # WRITE TO DB
    conn = psycopg2.connect(database="interim")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    register(conn)
    cur = conn.cursor()

    print("Writing precinct shapefile data to database")
    pre.write_data(cur, counties, clean_precincts)
    print("Writing voter registration data to database")
    reg.write_data(cur, co_voters_table, voters_per_year)

    cur.close()
    conn.close()
