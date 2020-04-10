import pandas as pd
import geopandas as gpd
import shapely
from sqlalchemy import Table, Column, Integer, MetaData
from geoalchemy2 import Geometry


def polygon_3d_to_2d(polygon_3d):
    coords2 = [(x, y) for x, y, _ in list(polygon_3d.exterior.coords)]
    return shapely.geometry.Polygon(coords2)


def gdf_3d_to_2d(gdf, geo_column_name='geometry'):
    new_geoms = []
    for geom in gdf[geo_column_name].values:
        if geom.geom_type == 'MultiPolygon':
            polys = geom.geoms
        elif geom.geom_type == 'Polygon':
            polys = [geom]
        else:
            raise TypeError("Unrecognized geometry")

        polys_2d = []
        for poly in polys:
            if is_geometry_3d(poly):
                polys_2d.append(polygon_3d_to_2d(poly))
            else:
                polys_2d.append(poly)
        new_geoms.append(shapely.geometry.MultiPolygon(polys_2d))
    new_gdf = gdf.copy()
    new_gdf[geo_column_name] = new_geoms
    return new_gdf


def is_geometry_3d(geometry):
    dims = len(geometry.exterior.coords[0])
    if dims == 3:
        return True
    elif dims == 2:
        return False
    else:
        raise ValueError("Weird dimensions of the input geometry.")


def write_gdf_to_sql(gdf, table_name, engine, geo_column_name='geometry',
                     data_dtypes=None):

    # Split off the data from the geometry
    df_columns = list(gdf.columns.values)
    df_columns.remove(geo_column_name)
    if df_columns:
        df = pd.DataFrame(data=gdf[df_columns],
                          columns=df_columns)

        # Write the df
        if data_dtypes is None:
            with engine.connect() as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=True,
                          index_label=None)
        else:
            with engine.connect() as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=True,
                          index_label=None, dtype=data_dtypes)

    # Make the new gdf
    geos = gpd.GeoDataFrame(data=gdf[[geo_column_name]],
                            columns=[geo_column_name])
    geos = gdf_3d_to_2d(geos)

    # Define the geometry table
    geo_table_name = table_name + '_geom'
    metadata = MetaData()
    geom_table = Table(
        geo_table_name, metadata,
        Column('id', Integer, primary_key=True),
        Column('geometry', Geometry('MultiPolygon'))
        )
    geos_map = []
    for _, row in geos.iterrows():
        geos_map.append({'id': row.name,
                         'geometry': row[geo_column_name].wkt})

    # Write the geometry table
    geom_table.drop(engine, checkfirst=True)
    geom_table.create(engine)
    with engine.connect() as conn:
        conn.execute(geom_table.insert(), geos_map)
