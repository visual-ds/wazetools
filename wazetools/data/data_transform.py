from shapely.geometry import LineString, Point, MultiPoint, box, Polygon, MultiPolygon, GeometryCollection
from multiprocessing.pool import ThreadPool
from dask.diagnostics import ProgressBar
from shapely.ops import transform
from functools import partial
import geopandas as gpd
import pandas as pd
import logging
import pickle
import dask
import pyproj
import imp
import ast
import os
import yaml
from pathlib import Path
import re
import wazetools.utils as utils
import ast

# Edicao

# LOGGING CONFIG
log_path = Path(__file__).parent.parent / 'logging.yaml'
logging.config.dictConfig(yaml.load(open(log_path)))

##############################################
#  OSM
##############################################


def get_osm(city, G=False, load_cache=True, fishnet=False, threshold=0.01):
    """Process the graph generated by Open Stree Maps (OSM) to nodes, edges DataFrames. 
    
    It also maps the edges and nodes of the graph to smaller quadrants.
    # TODO improve id system

    Parameters
    ----------
    city : string
        Name of the city that you are working on. It works as a unique id.
    G : nx.DiGraph, optional
        A road network graph generated by OSMNX 
        (the default is False, if there is cached data accessed by the name of the city)
    load_cache : bool, optional
        Whether to load cached data (the default is True, which loads cache)
    fishnet: bool, optional
        Whether to divide the city in regions as quadrants using the threshold
    threshold : float, optional
        Size of the smaller quadrants (the default is 0.01)
    
    Returns
    -------
    dictionary

        A dictionary with:
        
        - 'G': OSMNX nx.DiGraph
        - 'edges': geopandas.GeoDataFrame with the features of all edges
        - 'nodes': geopandas.GeoDataFrame with the features of all nodes
        - 'boxes': geopandas.GeoDataFrame mapping edges to quadrants (optional)
    """


    path = Path(__file__).resolve().parents[2] / 'data' / 'processed' / ('osm_' + city + '.p')

    if os.path.exists(path) and load_cache:

        with utils.Logger('Fetching data from cache'):
            osm = pickle.load(open(path, 'rb'))

    else:

        if not G:
            raise('There is no cached data for {}. Please, insert a graph'.format(path))

        with utils.Logger('Treating data'):
            osm = {
                'nodes': gpd.GeoDataFrame(
                    nodes_to_pd(G)),
                'edges': edges_to_pd(G),
                'G': G}
            osm['edges'] = gpd.GeoDataFrame(treat_osm_data(osm['edges'], ))
            osm['nodes']['point'] = osm['nodes'].apply(
                lambda node: Point([node['x'], node['y']]), axis=1)
            osm['edges']['geometry_id'] = osm['edges']['geometry'].apply(str)
            
        with utils.Logger('Calculating fishnet'):
            if fishnet:
                osm['boxes'] = create_boxes(osm, threshold=threshold)

        with utils.Logger('Saving OSM data to {}'.format(path)):
            pickle.dump(osm, open(path, 'wb'))

    return osm


def nodes_to_pd(G):
    """Osmnx graph to nodes pd.DataFrame
    
    Parameters
    ----------
    G : nx.DiGraph
        Osmnx road graph
    
    Returns
    -------
    pd.DataFrame
        Nodes dataframe
    """

    final = []
    for n, d in G.nodes(data=True):
        d.update({'n': n})
        final.append(d)
    return pd.DataFrame(final)


def edges_to_pd(G):
    """Osmnx graph to edges pd.DataFrame
    
    Parameters
    ----------
    G : nx.DiGraph
        Osmnx road graph
    
    Returns
    -------
    pd.DataFrame
        Edges dataframe with all features
    """
    final = []
    for u, v, k, d in G.edges(keys=True, data=True):
        d.update({'u': u,
                  'v': v,
                  'k': k})
        if 'geometry' not in d.keys():
            x1 = G.nodes[u]['x']
            y1 = G.nodes[u]['y']
            x2 = G.nodes[v]['x']
            y2 = G.nodes[v]['y']
            d['geometry'] = LineString([(x1, y1), (x2, y2)])

        final.append(d)
    return pd.DataFrame(final)


def split_data_frame_list(df, target_column, output_type):
    """Process list values and turn them into new rows.

    Parameters
    ----------
    df : pd.DataFrame
    target_column : string
        Column name that will be processed
    output_type : type
        Type that the list values will be converted to.
    
    Returns
    -------
    pd.DataFrame
        Same DataFrame but without lists on the targeted column
    """

    row_accumulator = []

    def split_list_to_rows(row, target_column, output_type):
        try:
            split_row = ast.literal_eval(row[target_column])
        except:
            split_row = row[target_column]
        if isinstance(split_row, list):
            for s in split_row:
                new_row = row.to_dict()
                new_row[target_column] = output_type(s)
                row_accumulator.append(new_row)
        else:
            new_row = row.to_dict()
            try:
                new_row[target_column] = float(split_row)
            except BaseException:
                new_row[target_column] = output_type(split_row)
            row_accumulator.append(new_row)

    df.apply(lambda row: split_list_to_rows(row, target_column, output_type), axis=1)
    new_df = pd.DataFrame(row_accumulator)

    return new_df


def clean_speed_number(x):

    try:
        return float(re.findall(r'\d+', x)[0])

    except BaseException:
        return float(x)

def get_df_columns_types(df, typ):
    """Given a pd.DataFrame, returns a list with columns that contains a
    certain type
    
    Parameters
    ----------
    df : pd.DataFrame
    typ : type
        [description]
    
    Returns
    -------
    list of string
        List with columns names
    """

    df_types = df.applymap(type)
    final = []
    for col in df_types:
        for t in df_types[col].unique():
            if str(typ) in str(t): 
                final.append(col)
    return final    

def clean_oneway(x):

    try:
        return int(x)
    except ValueError:
        if x:
            return 1
        else:
            return 0

def treat_osm_data(df):

    with utils.Logger('Fixing Types'):
        
        columns = ['highway', 'lanes', 'maxspeed', 'name', 'osmid']
        
        for column in list(columns):
            utils.Logger('Treating column: {}'.format(column)).print_message()
            df = split_data_frame_list(df, column, output_type=str)

        #print(df['maxspeed'])
        #df['maxspeed'] = df['maxspeed'].apply(clean_speed_number)

        df['oneway'] = df['oneway'].apply(clean_oneway)

    return df


def box_around_city(osm):
    """Box a city. Given a graph, it returns the minimum box that contains all 
    nodes and edges.
    
    Parameters
    ----------
    osm : dictionary
        Processed OSMNX dictionary
    
    Returns
    -------
    shapely.box
    """

    return box(*MultiPoint(osm['nodes']['point']).bounds)


def fishnet(geometry, osm, threshold):

    utils.Logger(
        'Calculating Fishnet with threshold of {}'.format(threshold),
        'INFO').print_message()

    bounds = geometry.bounds
    xmin = int(bounds[0] // threshold)
    xmax = int(bounds[2] // threshold)
    ymin = int(bounds[1] // threshold)
    ymax = int(bounds[3] // threshold)
    ncols = int(xmax - xmin + 1)
    nrows = int(ymax - ymin + 1)

    result = []
    for i in range(xmin, xmax + 1):
        for j in range(ymin, ymax + 1):

            b = box(i * threshold,
                    j * threshold,
                    (i + 1) * threshold,
                    (j + 1) * threshold)

            g = geometry.intersection(b)

            if g.is_empty:
                continue

            result.append({'box': g, 'i': i, 'j': j, 'centroid': g.centroid, 'osmids': list(
                osm['edges'][osm['edges']['geometry'].apply(lambda x: x.intersects(g))]['geometry_id'])})

    return pd.DataFrame(result)


def get_osmid_neighbour_box(current, df, distance):
    """Given the boxes dataframe and a current position, returns
    all neighboring boxes on a given distance
    
    Parameters
    ----------
    current : pd.Series
        Selected row
    df : pd.DataFrame
        Dataframe from OSMNX dictionary 'boxes'
    distance : int
        Neighbouring distance. 1 to the 9 closest and so on.
    
    Returns
    -------
    pd.DataFrame
        A subset of the inputed DataFrame
    """


    return df[(df['i'] >= current['i'] - distance) &
              (df['i'] <= current['i'] + distance) &
              (df['j'] >= current['j'] - distance) &
              (df['j'] <= current['j'] + distance)]


def flatten(l): return [item for sublist in l for item in sublist]


def enhance_osm_box_neighbours(current, df, distance):

    df = get_osmid_neighbour_box(current, df, distance)

    osmids = flatten(list(df['osmids']))

    return osmids


def create_boxes(osm, threshold, max_distance=2):

    with utils.Logger('Creating boxes', 'INFO'):

        initial_geometry = box_around_city(osm)

        df = fishnet(initial_geometry, osm, threshold=threshold)

        for distance in range(1, max_distance + 1):
            df['osmids_neigh_{}'.format(distance)] = df.apply(
                enhance_osm_box_neighbours, args=(df, distance), axis=1)

    return df


##############################################
#  WAZE
##############################################

def to_segment_id(a): 
    return '_'.join(map(str, flatten(sorted(list(a.coords)))))

def split_into_segments(line):

    line_points = list(line['geometry'].coords)
    return [LineString(segment) for segment in list(
                                zip(line_points[:-1], line_points[1:]))]

def segments_to_frame(segments):

    segments_id = list(map(to_segment_id, segments))

    return pd.DataFrame(list(zip(segments, segments_id)), columns=[
                            'geometry_segment', 'id_segment'])

def get_segments(line):

    segments = split_into_segments(line)

    segments = segments_to_frame(segments)

    segments['_id'] = line['_id']

    return segments


def save_gpd(file, filename, schema=None):

    try:
        os.remove(filename)
    except OSError:
        pass

    gpd.GeoDataFrame(file).to_file(filename, schema=schema)


def treat_waze_jams(city, waze, load_cache=True, columns={}):
    """Get Waze Jam Data and splits the segments to their smallest units, 'segment atom'.
    It is important because a 'segment atom' are indepent and can be in different jams
    depending on the day, time, weather and other conditions.

    This connects directly to the database and queries it.
    
    Parameters
    ----------
    city : string   
        Name of the city that you are working on. It works as a unique id.
    table : string, optional
        Table name (the default is False, which searches cached data)
    schema : bool, optional
        Schema name (the default is False, which searches cached data)
    con : bool, optional
        [description] (the default is False, which searches cached data)
    load_cache : bool, optional
        Whether to load data from cache (the default is True, which [default_description])
    columns : dict, optional
        Point to these columns:
            - id
            - pub_utc_date
            - line
        as dict ==> {'uuid': 'id', 'time': 'pub_utc_date'}
    
    Returns
    -------
    gpd.GeoPandasDataFrame
        'Atomized' table
    """


    path = Path(__file__).resolve().parents[2] / 'data' / 'processed' / ('waze_' + city + '.p')

    if os.path.exists(path) and load_cache:

        with utils.Logger('Fetching data from cache'):
            waze = pickle.load(open(path, 'rb'))

    else:

        with utils.Logger('Converting line to shape'):
            
            waze = waze.rename(columns=columns)
            
            try:
                waze['geometry'] = waze['line'].apply(
                    lambda x: LineString(ast.literal_eval(x)))
            except:
                if isinstance(waze['line'][0], str):
                    waze['line'] = waze['line'].apply(ast.literal_eval)
                
                waze['geometry'] = waze['line'].apply(
                    lambda x: LineString([(i['x'], i['y']) for i in x]))

        with utils.Logger('Converting to datetime'):
            try:
                waze['startTime'] = pd.to_datetime(waze['startTime'])
                waze['endTime'] = pd.to_datetime(waze['endTime'])
            except:
                waze['put_utc_date'] = pd.to_datetime(waze['pub_utc_date'])

        with utils.Logger('Get waze segments', 'INFO'):

            if '_id' not in waze.columns:
                waze['_id'] = waze['id']

            dask.set_options(pool=ThreadPool(40))

            segments = []
            for i, row in waze.iterrows():
                segments.append(dask.delayed(get_segments)(row))

            segments = dask.delayed(pd.concat)(segments)

            segments_waze = dask.delayed(segments).merge(waze, on='_id')

            with ProgressBar():
                waze = dask.compute(segments_waze)[0]

        with utils.Logger('Saving csv to {}'.format(path)):
            
            pickle.dump(gpd.GeoDataFrame(waze), open(path, 'wb'))

    return gpd.GeoDataFrame(waze)

##############################################
#  AADT
##############################################


def get_aadt(city_name):
    """
    info_path: path to AADT info
    shape_path: path to shape file
    return: GeoPandas DataFrame
    """

    year = 2016

    # TODO by city

    info_path_major = '../raw_data/AADT/AADF-data-major-roads.csv'
    shape_path_major = "../raw_data/AADT/shape/major-roads-link-network2016.shp"

    with utils.Logger('Loading data'):
        aadt_shape = gpd.read_file(shape_path_major)
        aadt = pd.read_csv(info_path_major)

    logging.debug('Filtering by city and year: {} {}'.format(city_name, year))

    aadt = aadt[(aadt['ONS LA Name'].str.contains(city_name, case=False)) &
                (aadt['AADFYear'] == year)]

    with utils.Logger('Merging: {}, {}'.format(city_name, year)):
        aadt = aadt_shape.merge(aadt, right_on='CP', left_on='CP_Number')

    project = partial(
        pyproj.transform,
        pyproj.Proj(init='epsg:27700'),  # source coordinate system
        pyproj.Proj(init='epsg:4326'))   # destination coordinate system

    with utils.Logger('Transform coord system'):
        aadt['geometry'] = aadt['geometry'].apply(
            lambda x: transform(project, x))
        aadt['begin'] = aadt['geometry'].apply(
            lambda line: list(map(Point, line.coords)))
        aadt[['begin', 'end']] = pd.DataFrame(
            aadt['begin'].values.tolist(), index=aadt.index)

    return gpd.GeoDataFrame(aadt)

def get_shapely_columns(df):

    return [column for column in df.columns 
            if 'shapely' in str(type(df[column][0]))]

def treat_numpy_to_geojson(df, column):
    
    try:
        df[column] = df[column].apply(float)
    except Exception as e:
        df[column] = df[column].apply(str)
        return df

    df = df.drop(columns=[column])
    
    return df

def to_geojson(df, filename, path=False):
    
    with utils.Logger('Preprocessing Data'):
        for col in get_df_columns_types(df, 'numpy'):

            df = treat_numpy_to_geojson(df, col)

        for col in get_df_columns_types(df, list):

            df = split_data_frame_list(df, col, str)

        for col in get_df_columns_types(df, 'timestamp'):

            df = split_data_frame_list(df, col, str)

    geo_columns = get_shapely_columns(df)
    for column in geo_columns:

        with utils.Logger('Writing GeoJson with {} as geometry column'.format(column)):

            path = Path(__file__).resolve().parents[2] / 'data' / 'processed' / 'geojson' / (filename + '_' + column + '.geojson')

            if not os.path.exists(path.parent):
                os.makedirs(path.parent)
        
            if isinstance(df, pd.DataFrame):
                df_geo = df.dropna(subset=[column])
                df_geo = gpd.GeoDataFrame(df_geo, geometry=column)
            else:
                df_geo.set_geometry(column)

            # Drop other geometry columns
            df_geo = df_geo.drop(columns=[x for x in geo_columns if x != column])

            try:
                os.remove(path)
            except OSError:
                pass
            df_geo.to_file(path, driver='GeoJSON')

            print('Saved at ', path)