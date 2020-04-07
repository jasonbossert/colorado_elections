import requests

raw_root = "../../data/raw/"


def download_wiki_counties():
    response = requests.get(
        "https://en.wikipedia.org/wiki/List_of_counties_in_Colorado")
    with open(raw_root + "state/counties_response.txt", 'w') as file:
        file.write(response.text)


def download_2016_precinct_shapefiles():
    pass


def download_2018_precinct_shapefiles():
    pass


def download_state_senate_districts():
    pass


def download_state_house_districts():
    pass


def download_federal_house_districts():
    pass
