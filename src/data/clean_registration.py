from collections import defaultdict

import pandas as pd
import numpy as np


to_drop = ["index", "ADDRESS_LIBRARY_ID", "SPLIT", "PHONE_NUM", "MAIL_ADDR1",
           "MAIL_ADDR2", "MAIL_ADDR3", "MAILING_CITY", "MAILING_STATE",
           "MAILING_ZIP_CODE", "MAILING_ZIP_PLUS", "MAILING_COUNTRY",
           "SPL_ID", "ID_REQUIRED", "PARTY_AFFILIATION_DATE", "PREFERENCE"]

to_datetime = ["EFFECTIVE_DATE", "REGISTRATION_DATE"]

to_category = ["COUNTY_CODE", "COUNTY", "NAME_SUFFIX", "STATUS_CODE",
               "PRECINCT_NAME", "RESIDENTIAL_STATE", "RESIDENTIAL_ZIP_CODE",
               "STATUS", "STATUS_REASON", "BIRTH_YEAR", "GENDER", "PRECINCT",
               "VOTER_STATUS_ID", "PARTY", "PERMANENT_MAIL_IN_VOTER",
               "CONGRESSIONAL", "STATE_SENATE", "STATE_HOUSE"]

to_numeric = ["RESIDENTIAL_ZIP_PLUS"]

to_str = ["VOTER_ID", "LAST_NAME", "FIRST_NAME", "MIDDLE_NAME",
          "HOUSE_NUM", "HOUSE_SUFFIX", "PRE_DIR", "STREET_NAME",
          "STREET_TYPE", "POST_DIR", "UNIT_TYPE", "UNIT_NUM",
          "RESIDENTIAL_ADDRESS", "RESIDENTIAL_CITY", "VOTER_NAME"]


def load_data(raw_root):
    path = raw_root + "registration/"
    file_base = "Registered_Voters_List_Part"

    folder = "2018_Nov/"
    raw_2018 = read_reg_voter_lists(path+folder+file_base, 8)

    folder = "2016_Dec/"
    raw_2016 = read_reg_voter_lists(path+folder+file_base, 8)

    return (raw_2016, raw_2018)


def clean_data(raw_registration_data):
    raw_2016, raw_2018 = raw_registration_data

    reg_voters_2018 = convert_types(raw_2018,
                                    to_drop,
                                    to_datetime,
                                    to_category,
                                    to_numeric,
                                    to_str)
    reg_voters_2018 = format_df(reg_voters_2018)

    to_drop_2016 = to_drop.copy() + ["ADDRESS_NON_STD"]
    to_drop_2016.remove("PREFERENCE")
    reg_voters_2016 = convert_types(raw_2016,
                                    to_drop_2016,
                                    to_datetime,
                                    to_category,
                                    to_numeric,
                                    to_str)
    reg_voters_2016 = format_df(reg_voters_2016)

    reg_voters = [reg_voters_2016.head(1000), reg_voters_2018.head(1000)]

    co_voters_table, voters_per_year = build_voter_table(reg_voters)
    return co_voters_table, voters_per_year


def write_data(cur, co_voters_table, voters_per_year):

    reg_names = ['voter_index_' + str(year) for year in [2016, 2018]]
    for name, df in zip(reg_names, voters_per_year):
        cur.execute("""DROP TABLE IF EXISTS {}""".format(name))
        cur.execute("""CREATE TABLE {} (
                "index" INT NOT NULL,
                "voter_index" INT NOT NULL
                )
            """.format(name))
        sql = """INSERT INTO {}
                 (index, voter_index)
                 VALUES (%s, %s)""".format(name)
        for _, row in df.iterrows():
            vals = [row.name, row.voter_index]
            cur.execute(sql, vals)

    cur.execute("""DROP TABLE IF EXISTS registered_voters""")
    cur.execute("""CREATE TABLE registered_voters (
            "voter_index" INT PRIMARY KEY,
            "voter_id" INT NOT NULL,
            "county_code" INT NOT NULL,
            "last_name" TEXT,
            "first_name" TEXT,
            "middle_name" TEXT,
            "name_suffix" TEXT,
            "status_code" TEXT NOT NULL,
            "house_num" TEXT,
            "house_suffix" TEXT,
            "pre_dir" TEXT,
            "unit_type" TEXT,
            "unit_num" TEXT,
            "residential_city" TEXT NOT NULL,
            "residential_state" TEXT NOT NULL,
            "residential_zip_code" INT NOT NULL,
            "residential_zip_plus" TEXT,
            "effective_date" DATE NOT NULL,
            "registration_date" DATE NOT NULL,
            "birth_year" TEXT NOT NULL,
            "gender" TEXT NOT NULL,
            "precinct" TEXT NOT NULL,
            "party" TEXT NOT NULL,
            "congressional" INT NOT NULL,
            "state_senate" INT NOT NULL,
            "state_house" INT NOT NULL)"""
                )
    vars = ["voter_index", "voter_id", "county_code", "last_name",
            "first_name", "middle_name", "name_suffix", "status_code",
            "house_num", "house_suffix", "pre_dir", "unit_type",
            "unit_num", "residential_city", "residential_state",
            "residential_zip_code", "residential_zip_plus",
            "effective_date", "registration_date", "birth_year",
            "gender", "precinct", "party", "congressional",
            "state_senate", "state_house"]
    perc_string = "(" + ", ".join(['%s'] * len(vars)) + ")"
    var_string = "(" + ", ".join(vars) + ")"
    sql = ("""INSERT INTO registered_voters {} VALUES {}"""
           .format(var_string, perc_string))

    for i, row in co_voters_table.iterrows():
        vals = [row.name] + list(row[vars[1:]].values)
        cur.execute(sql, vals)


def read_reg_voter_lists(name, num_files):
    data = []
    for i in range(1, num_files+1):
        data.append(pd.read_csv(name+str(i)+".txt", engine="python"))

    raw = pd.concat(data)
    raw = raw.reset_index()
    return raw


def convert_types(df, to_drop, to_datetime, to_category, to_numeric, to_str):
    reg_voters = df.drop(columns=to_drop)
    for col in to_datetime:
        reg_voters[col] = pd.to_datetime(reg_voters[col],
                                         infer_datetime_format=True)
    for col in to_str+to_category:
        reg_voters[col] = reg_voters[col].astype('str')
    for col in to_numeric:
        reg_voters[col] = pd.to_numeric(reg_voters[col])
    for col in to_category:
        reg_voters[col] = reg_voters[col].astype('category')
    return reg_voters


def format_df(df):
    def split_take_last(series):
        return series.apply(lambda x: x.split(' ')[-1])

    df = (df.rename(columns=str.lower)
            .drop(columns=['county', 'voter_name', 'precinct_name',
                           'residential_address', 'status', 'status_reason',
                           'voter_status_id', 'permanent_mail_in_voter'])
            .assign(congressional=lambda x: split_take_last(x.congressional),
                    state_senate=lambda x: split_take_last(x.state_senate),
                    state_house=lambda x: split_take_last(x.state_house))
         )
    return df


matching_vars = np.array(
        ['voter_id', 'county_code', 'last_name', 'first_name', 'middle_name',
         'precinct', 'house_num', 'house_suffix', 'pre_dir',
         'street_name', 'street_type', 'post_dir', 'unit_type', 'unit_num',
         'residential_city', 'residential_state', 'residential_zip_code']
    )


def build_voter_table(registrations):
    longest = max([len(reg) for reg in registrations])
    num_cols = len(registrations[0].columns.values)

    voters = np.zeros((longest, num_cols), dtype=object)
    voter_indices = [np.zeros((len(reg), 1)) for reg in registrations]
    voter_map = defaultdict(list)

    current_row = 0
    for i, reg in enumerate(registrations):
        matching_nums = np.array([np.where(reg.columns.values == var)[0][0]
                                  for var in matching_vars])

        reg = reg.to_numpy()
        for j, person in enumerate(reg):
            idx = get_person_index(person, voters, voter_map,
                                   reg, matching_nums)
            if idx is None:
                try:
                    voters[current_row, :] = person
                except IndexError:
                    to_cat = np.zeros((int(longest/5), num_cols), dtype=object)
                    voters = np.concatenate((voters, to_cat))
                    voters[current_row, :] = person
                voter_map[person[1]].append(current_row)
                voter_indices[i][j] = current_row
                current_row += 1
            else:
                voter_indices[i][j] = idx
    voters = pd.DataFrame(voters[:current_row, :],
                          columns=registrations[0].columns.values)
    voter_indices = [pd.DataFrame(vi, columns=['voter_index'])
                     for vi in voter_indices]
    return voters, voter_indices


def get_person_index(person, voters, voter_map, reg, matching_nums):
    to_search = voter_map[person[1]]
    for idx in to_search:
        a = voters[idx, matching_nums]
        b = person[matching_nums]
        conditional = (a == b).all()
        if conditional:
            return idx
    return None
