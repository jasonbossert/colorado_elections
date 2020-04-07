import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup
import maup


def parse_wiki_counties_to_df(response):
    """

    """
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': "wikitable sortable"})
    body = table.select('tr')
    header = body[0]
    values = body[1:]

    col_names = [th.text.strip().split('[')[0] for th in header.select('th')]
    col_names = col_names[:-1]

    def parse_table_entry(data):
        county_name = data.select('th')[0].text
        county_name = county_name.replace("City and County of ", "")
        county_name = county_name.replace(" County", "")
        county_name = county_name.strip()

        data_vals = [d.text.strip().replace(u'\xa0', u' ')
                     for d in data.select('td')]
        # Get rid of the empty string from failing to parse the map
        data_vals = list(filter(None, data_vals))
        # Only take the value and not the units
        data_vals[-1] = data_vals[-1].split()[0]
        data_vals[-1] = data_vals[-1].replace(',', '')
        data_vals[-2] = data_vals[-2].replace(',', '')

        return [county_name] + data_vals

    data = [parse_table_entry(value) for value in values]
    data = list(map(list, zip(*data)))
    data = {col: dat for col, dat in zip(col_names, data)}

    return pd.DataFrame(data)


def clean_wiki_counties_df(df):
    """

    """
    counties = (df.rename(columns=str.lower)
                  .rename(columns={'fips code': 'fips',
                                   'est.': 'established'})
                  .drop(columns=['formed from', 'etymology'])
                  .assign(fips=lambda x: x.fips.str.pad(3, fillchar='0'),
                          established=lambda x: pd.to_datetime(x.established),
                          population=lambda x: pd.to_numeric(x.population),
                          area=lambda x: pd.to_numeric(x.area)))
    return counties


def parse_NAMELSAD(value):
    """


    """
    county = None
    vtr_dst = None
    full_vtr_dst = None

    try:
        full_vtr_dst = int(value)
    except ValueError:
        full_vtr_dst = int(value.split()[-1])
        county = ' '.join(value.split()[:-1])

    vtr_dst = full_vtr_dst % 1000
    vtr_dst = str(vtr_dst).zfill(3)

    return (county, vtr_dst, full_vtr_dst)


def lookup_countyfips(fips, counties):
    """

    """
    val = counties[counties.fips == fips].county.values
    return val[0]


def buffer_if_invalid(geom):
    """


    """
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


def parse_precinct_NAMELSAD(precincts):
    """

    """
    parsed = [parse_NAMELSAD(value) for value in precincts.NAMELSAD.values]
    parsed = list(map(list, zip(*parsed)))
    return parsed


def clean_precincts_2016(df):
    parsed_2016 = parse_precinct_NAMELSAD(df)
    clean_2016 = (gpd.GeoDataFrame()
                     .assign(
        countyfips=df["COUNTYFP"].values,
        countyname=[lookup_countyfips(f, counties)
                    for f in df["COUNTYFP"].values],
        countynum=[None] * len(parsed_2016[1]),
        vtdst3=parsed_2016[1],
        vtds5=parsed_2016[1],
        vtdst=parsed_2016[2],
        congressional_district_115fips=[None] * len(parsed_2016[1]),
        state_legisture_upper=[None] * len(parsed_2016[1]),
        state_legisture_lower=[None] * len(parsed_2016[1]),
        geometry=df['geometry'].map(buffer_if_invalid)
                        )
                  )

    clean_2016 = clean_2016.sort_values(
        by=['countyfips', 'vtdst3'],
        ignore_index=True)
    clean_2016.crs = {'init': 'epsg:4269'}

    return clean_2016


def clean_precincts_2018(df):
    parsed_2018 = parse_precinct_NAMELSAD(df)
    clean_2018 = (gpd.GeoDataFrame().assign(
        countyfips=df["COUNTYFP"].values,
        countyname=[lookup_countyfips(fips, counties)
                    for fips in df["COUNTYFP"].values],
        countynum=[None] * len(parsed_2018[1]),
        vtdst3=((df["VTDST"].values.astype(int) % 1000)
                .map(lambda x: str(x).zfill(3))),
        vtdst5=[None] * len(parsed_2018[1]),
        vtdst=df["PRECID"],
        congressional_district_115fips=[None] * len(parsed_2018[1]),
        state_legisture_upper=[None] * len(parsed_2018[1]),
        state_legisture_lower=[None] * len(parsed_2018[1]),
        geometry=df['geometry'].map(buffer_if_invalid)
                        )
                 )
    clean_2018 = clean_2018.sort_values(
        by=['countyfips', 'vtdst3'],
        ignore_index=True)
    clean_2018.crs = {'init': 'epsg:4269'}

    return clean_2018


def read_and_fmt_state_vars(name):
    vals = (gpd.read_file(name)
               .to_crs(epsg=4269)
               .rename(columns=str.lower)
               .assign(
        geometry=lambda x: x.geometry.map(buffer_if_invalid),
        district=lambda x: x.district.map(lambda x: str(x).zfill(2))
               )
            )
    vals.crs = {'init': vals.crs['init']}
    return vals


def get_assigned_values(source, target, var):
    return target.loc[maup.assign(source, target)][var].values


def add_districts(clean, state_senate,
                  state_house, co_fed_house):
    clean['congressional_district_115fips'] = get_assigned_values(
        clean, co_fed_house, 'congressional_district_115fips')
    clean['congressional_district_115fips'] = (
        clean['congressional_district_115fips']
        .map(lambda x: x.strip('0')))

    clean['state_legisture_upper'] = get_assigned_values(
        clean, state_senate, 'district')
    clean['state_legisture_lower'] = get_assigned_values(
        clean, state_house, 'district')
    return clean


def get_county_name_to_number_mapping():
    results_2018 = pd.read_excel(raw_root + 'results/' + str(2018)
                                 + "GeneralPrecinctResults.xlsx")
    county_nums = results_2018.Precinct.map(lambda x: str(x)[-5:-3]).values
    county_names = results_2018.County.values
    mapping = sorted(list(set(zip(county_nums, county_names))))
    mapping = {county: num for (num, county) in mapping}
    return mapping


def generate_long_precinct_number(df):
    congress = df.CD115FP.values
    state_senate = df.SLDUST.values
    state_rep = df.SLDLST.values
    county = df.COUNTYNUM.values
    precinct = df.VTDST3.values
    long = []
    for (a, b, c, d, e) in zip(congress, state_senate,
                               state_rep, county, precinct):
        long.append(a+b+c+d+e)
    return long


def update_county_vtdst(df):
    df.countynum = df.countyname.map(lambda x: county_mapping[x])
    df['vtdst'] = generate_long_precinct_number(df)
    df['vtdst5'] = df['vtdst'].map(lambda x: x[-5:])
    return df


if __name__ == '__main__':
    raw_root = "../../data/raw/"
    interim_root = "../../data/interim/"

    """
    PRECINCT SHAPEFILES

    """

    with open(raw_root + "state/counties_response.txt", 'r') as file:
        counties_raw = file.read()
    counties_df = parse_wiki_counties_to_df(counties_raw)
    counties = clean_wiki_counties_df(counties_df)
    counties.countynum = [str(z+1).zfill(2) for z in counties.index.values]
    county_mapping = {}

    precincts_2016 = gpd.read_file(raw_root + "precincts/co_2016/")
    clean_2016 = clean_precincts_2016(precincts_2016)

    precincts_2018 = gpd.read_file(raw_root + "precincts/CO_precincts/")
    clean_2018 = clean_precincts_2018(precincts_2018)

    state_senate = read_and_fmt_state_vars(raw_root
                                           + "state/state_senate_districts/")

    state_house = read_and_fmt_state_vars(raw_root
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

    clean_2016 = add_districts(clean_2016, state_senate,
                               state_house, co_fed_house)
    clean_2018 = add_districts(clean_2018, state_senate,
                               state_house, co_fed_house)

    county_mapping = get_county_name_to_number_mapping()
    clean_2016 = update_county_vtdst(clean_2016)
    clean_2018 = update_county_vtdst(clean_2018)

    """
    Open database connection
    """
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from postgis.psycopg import register

    conn = psycopg2.connect(database="interim")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    register(conn)
    cur = conn.cursor()

    """
    Write Counties
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
            "countynum" TEXT NOT NULL,
        )""")
    for _, row in counties.iterrows():
        vals = [row.name] + list(row.iloc[[0, 1, 3, 4, 5, 6]])
        cur.execute("""INSERT INTO colorado_counties
                (id, county, fips, established, population, area, countynum)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""", vals)

    """
    Write Precinct Shapefiles to Database
    """

    cur.execute("""DROP TABLE IF EXISTS precinct_shapefiles_2016""")
    cur.execute("""
        CREATE TABLE precinct_shapefiles_2016 (
            "id" INT PRIMARY KEY
            "countyfips" TEXT NOT NULL,
            "countyname" TEXT NOT NULL,
            "countynum" TEXT NOT NULL,
            "vtdst3" TEXT NOT NULL,
            "vtdst5" TEXT NOT NULL UNIQUE,
            "vtdst" TEXT NOT NULL UNIQUE,
            "congressional_district_115fips" TEXT NOT NULL,
            "state_legisture_upper" TEXT NOT NULL,
            "state_legisture_lower" TEXT NOT NULL,
            "geom" POLYGON NOT NULL)
        """)
    cur.execute("""
        CREATE TABLE precinct_shapefiles_2018 LIKE precinct_shapefiles_2016)
        """)

    def write_precinct_shapefile_table(df, table_name):
        srid_str = f"SRID={int(df.crs['init'][-4:])};"
        for _, row in df.iterrows():
            vals = [table_name, row.name] + list(row.values)[:-1]
            geom = srid_str + list(row.values)[-1].wkt
            vals.append(geom)
            sql = """INSERT INTO %s
                     (id, countyfips, countyname, countynum, vtdst3, vtdst5,
                      vtdst, congressional_district_115fips,
                      state_legisture_upper, state_legisture_lower, geom)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            cur.execute(sql, vals)

    write_precinct_shapefile_table(clean_2016, 'precinct_shapefiles_2016')
    write_precinct_shapefile_table(clean_2018, 'precinct_shapefiles_2018')

    cur.close()
    conn.close()
