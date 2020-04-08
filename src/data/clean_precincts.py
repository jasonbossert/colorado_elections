import pandas as pd
import geopandas as gpd
from bs4 import BeautifulSoup
from rtree import index
import numpy as np


def parse_wiki_counties_to_df(response):
    """

    """
    soup = BeautifulSoup(response, 'html.parser')
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


def parse_precinct_NAMELSAD(name_var):
    """

    """
    parsed = [parse_NAMELSAD(value) for value in name_var]
    parsed = list(map(list, zip(*parsed)))
    return parsed


def clean_precincts_2016(df, counties):
    parsed_2016 = parse_precinct_NAMELSAD(df.NAMELSAD.values)
    clean_2016 = (gpd.GeoDataFrame()
                     .assign(
        countyfips=df["COUNTYFP"].values,
        countyname=[lookup_countyfips(f, counties)
                    for f in df["COUNTYFP"].values],
        countynum=['none'] * len(parsed_2016[1]),
        vtdst3=parsed_2016[1],
        vtds5=parsed_2016[1],
        vtdst=parsed_2016[2],
        congressional_district_115fips=['none'] * len(parsed_2016[1]),
        state_legisture_upper=['none'] * len(parsed_2016[1]),
        state_legisture_lower=['none'] * len(parsed_2016[1]),
        geometry=df['geometry'].map(buffer_if_invalid)
                        )
                  )

    clean_2016 = clean_2016.sort_values(
        by=['countyfips', 'vtdst3'],
        ignore_index=True)
    clean_2016.crs = {'init': 'epsg:4269'}

    return clean_2016


def clean_precincts_2018(df, counties):
    parsed_2018 = parse_precinct_NAMELSAD(df.NAME.values)
    clean_2018 = (gpd.GeoDataFrame().assign(
        countyfips=df["COUNTYFP"].values,
        countyname=[lookup_countyfips(fips, counties)
                    for fips in df["COUNTYFP"].values],
        countynum=['none'] * len(parsed_2018[1]),
        vtdst3=pd.Series(df["VTDST"].values.astype(int) % 1000)
            .map(lambda x: str(x).zfill(3)),
        vtdst5=['none'] * len(parsed_2018[1]),
        vtdst=df["PRECID"],
        congressional_district_115fips=['none'] * len(parsed_2018[1]),
        state_legisture_upper=['none'] * len(parsed_2018[1]),
        state_legisture_lower=['none'] * len(parsed_2018[1]),
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


def add_districts(clean, state_senate,
                  state_house, co_fed_house):
    clean['congressional_district_115fips'] = assign_source_to_target(
        clean, co_fed_house, 'congressional_district_115fips')
    clean['congressional_district_115fips'] = (
        clean['congressional_district_115fips']
        .map(lambda x: x.strip('0')))

    clean['state_legisture_upper'] = assign_source_to_target(
        clean, state_senate, 'district')
    clean['state_legisture_lower'] = assign_source_to_target(
        clean, state_house, 'district')
    return clean


def get_county_name_to_number_mapping(raw_root):
    results_2018 = pd.read_excel(raw_root + 'results/' + str(2018)
                                 + "GeneralPrecinctResults.xlsx")
    county_nums = results_2018.Precinct.map(lambda x: str(x)[-5:-3]).values
    county_names = results_2018.County.values
    mapping = sorted(list(set(zip(county_nums, county_names))))
    mapping = {county: num for (num, county) in mapping}
    return mapping


def generate_long_precinct_number(df):
    congress = df.congressional_district_115fips.values
    state_senate = df.state_legisture_upper.values
    state_rep = df.state_legisture_lower.values
    county = df.countynum.values
    precinct = df.vtdst3.values
    long = []
    for (a, b, c, d, e) in zip(congress, state_senate,
                               state_rep, county, precinct):
        long.append(a+b+c+d+e)
    return long


def update_county_vtdst(df, county_mapping):
    df['countynum'] = df.countyname.map(lambda x: county_mapping[x])
    df['vtdst'] = generate_long_precinct_number(df)
    df['vtdst5'] = df['vtdst'].map(lambda x: x[-5:])
    return df


def df_to_rtree_index(df):
    idx = index.Index()
    for _, row in df.bounds.iterrows():
        idx.insert(row.name, (row.minx, row.miny, row.maxx, row.maxy))
    return idx


def assign_source_to_target(source, target, target_var, thresh=0.99):
    """
    Assign a variable from target to source
    """

    if not source.crs['init'] == target.crs['init']:
        raise TypeError(
            f"The source and target geometries must have the same CRS. "
            f"source CRS: {source.crs['init']}, "
            f"target CRS: {target.crs['init']}"
            )

    idx = df_to_rtree_index(target)

    assignment = []
    for i, bound in source.bounds.iterrows():
        source_geo = source.iloc[i].geometry
        bounding_box = (bound.minx, bound.miny, bound.maxx, bound.maxy)
        possible = list(idx.intersection(bounding_box))

        overlaps = []
        for j, target_shape in target.iloc[possible].iterrows():
            target_geo = target_shape.geometry
            over = source_geo.intersection(target_geo).area
            overlaps.append(over)

        overlaps = np.array(overlaps)
        overlaps = overlaps/np.max(overlaps)
        if max(overlaps) < thresh:
            print(f"These districts don't align so well "
                  f"for source={i}, target={j}")
            print(possible)
            print(overlaps)
        greatest_overlap = np.argmax(np.array(overlaps))
        assignment.append(target[target_var].iloc[possible[greatest_overlap]])
    return np.array(assignment)
