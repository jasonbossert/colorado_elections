import geopandas as gpd
import clean_precincts as pre


if __name__ == '__main__':
    raw_root = "../../data/raw/"
    interim_root = "../../data/interim/"

    """
    CLEAN PRECINCT SHAPEFILES

    """

    with open(raw_root + "state/counties_response.txt", 'r') as file:
        counties_raw = file.read()
    counties_df = pre.parse_wiki_counties_to_df(counties_raw)
    counties = pre.clean_wiki_counties_df(counties_df)
    counties['countynum'] = [str(z+1).zfill(2) for z in counties.index.values]
    county_mapping = {}

    precincts_2016 = gpd.read_file(raw_root + "precincts/co_2016/")
    clean_2016 = pre.clean_precincts_2016(precincts_2016, counties)

    precincts_2018 = gpd.read_file(raw_root + "precincts/CO_precincts/")
    clean_2018 = pre.clean_precincts_2018(precincts_2018, counties)

    state_senate = pre.read_and_fmt_state_vars(raw_root
                                           + "state/state_senate_districts/")

    state_house = pre.read_and_fmt_state_vars(raw_root
                                          + "state/state_house_districts/")

    federal_house = (gpd.read_file(raw_root + "state/tl_2016_us_cd115/")
            .to_crs(epsg=4269)
            .rename(columns=str.lower)
            .rename(columns={'statefp': 'statefips',
                             'cd115fp': 'congressional_district_115fips'})
            .drop(columns=['geoid', 'namelsad', 'lsad', 'cdsessn',
                           'mtfcc', 'funcstat', 'aland', 'awater',
                           'intptlat', 'intptlon']))
    co_fed_house = federal_house[federal_house.statefips == '08']

    # clean_2016 = add_districts(clean_2016, state_senate,
    #                            state_house, co_fed_house)
    # clean_2018 = add_districts(clean_2018, state_senate,
    #                            state_house, co_fed_house)

    county_mapping = pre.get_county_name_to_number_mapping(raw_root)
    clean_2016 = pre.update_county_vtdst(clean_2016, county_mapping)
    clean_2018 = pre.update_county_vtdst(clean_2018, county_mapping)

    """
    OPEN DATABASE CONNECTION
    """
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from postgis.psycopg import register

    conn = psycopg2.connect(database="interim")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    register(conn)
    cur = conn.cursor()

    """
    WRITE COUNTIES TO DB
    """
    cur.execute("""DROP TABLE IF EXISTS colorado_counties""")
    cur.execute("""
        CREATE TABLE colorado_counties (
            "id" INT PRIMARY KEY,
            "county" TEXT NOT NULL UNIQUE,
            "fips" TEXT NOT NULL UNIQUE,
            "established" DATE NOT NULL,
            "population" INT NOT NULL,
            "area" FLOAT NOT NULL,
            "countynum" TEXT NOT NULL
        )""")
    for _, row in counties.iterrows():
        vals = [row.name] + list(row.iloc[[0, 1, 3, 4, 5, 6]])
        cur.execute("""INSERT INTO colorado_counties
                (id, county, fips, established, population, area, countynum)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", vals)

    """
    WRITE PRECINCT SHAPEFILES TO DB
    """

    cur.execute("""DROP TABLE IF EXISTS precinct_shapefiles_2016""")
    cur.execute("""
        CREATE TABLE precinct_shapefiles_2016 (
            "index" INT PRIMARY KEY,
            "countyfips" TEXT NOT NULL,
            "countyname" TEXT NOT NULL,
            "countynum" TEXT NOT NULL,
            "vtdst3" TEXT NOT NULL,
            "vtdst5" TEXT NOT NULL UNIQUE,
            "vtdst" TEXT NOT NULL UNIQUE,
            "congressional_district_115fips" TEXT NOT NULL,
            "state_legisture_upper" TEXT NOT NULL,
            "state_legisture_lower" TEXT NOT NULL,
            "geom" geometry)
        """)
    cur.execute("""DROP TABLE IF EXISTS precinct_shapefiles_2018""")
    cur.execute("""
        CREATE TABLE precinct_shapefiles_2018 (
            like precinct_shapefiles_2016 including all)
        """)

    def write_precinct_shapefile_table(df, table_name):
        srid_str = f"SRID={int(df.crs['init'][-4:])};"
        for _, row in df.iterrows():
            vars = ["countyfips", "countyname", "countynum", "vtdst3",
                    "vtdst5", "vtdst", "congressional_district_115fips",
                    "state_legisture_upper", "state_legisture_lower",
                    "geometry"]
            vals = [row.name] + list(row[vars].values)[:-1]
            geom = srid_str + list(row[vars].values)[-1].wkt
            vals.append(geom)
            sql = """INSERT INTO """ + table_name + """
                     (index, countyfips, countyname, countynum, vtdst3, vtdst5,
                      vtdst, congressional_district_115fips,
                      state_legisture_upper, state_legisture_lower, geom)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s::geometry)"""
            cur.execute(sql, vals)

    write_precinct_shapefile_table(clean_2016, 'precinct_shapefiles_2016')
    write_precinct_shapefile_table(clean_2018, 'precinct_shapefiles_2018')





    cur.close()
    conn.close()
