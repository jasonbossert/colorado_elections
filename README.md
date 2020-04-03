# Colorado Elections

## Introduction

Election analysis benefits from looking at results and patterns at the precinct level. Precincts are often the smallest grouping of voters that is reported, and is typically much smaller than a county. 

Colorado is a diverse state, with interesting ethnic, racial, geographical, religious, and political dynamics. Incorporating demographic information into voting analysis allows the construction of a more complete story.

This project focuses on synthesizing a diverse set of data streams to get the most granular picture of Colorado's voting patterns possible. 

## Data Sources

The primary tabular data sources for this project are the [decennial census](https://www.census.gov/programs-surveys/decennial-census/data/datasets.2010.html), the American Community Survey's [1-year](https://www.census.gov/data/developers/data-sets/acs-1year.html) and [5-year](https://www.census.gov/data/developers/data-sets/acs-5year.html) results and Colorado's [voting results](https://www.census.gov/data/developers/data-sets/acs-1year.html) and [registered voter lists](https://coloradovoters.info/download.html) (from a slightly interesting site).

Shapefiles come from the the Census's [TIGER/Line files](https://www.census.gov/cgi-bin/geo/shapefiles/index.php) blocks and block groups, and precinct shapefiles are from the [mggg-states repository](https://github.com/mggg-states/CO-shapefiles) (2018) and the [Harvard Election Data Archive](https://dataverse.harvard.edu/dataset.xhtml?persistentId=hdl:1902.1/16713) (2016), and are verified by comparing to the registered voters list.

## Change of Support

A fundamental challenge of attaching demographic information to each precinct is that the geographic shapes that the census collects data over (blocks and block-groups) do not line up with the administrative voting boundaries of Colorado (precincts). Converting census-geometry defined variables to precinct-defined variables is a classic change of support problem.

