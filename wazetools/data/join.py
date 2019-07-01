from shapely.geometry import box, LineString
import shapely
import pandas as pd
import pickle
import tqdm
import networkx as nx
import os
import numpy as np
import imp
import logging.config
import yaml
import dask
import numpy as np
import ast
from copy import deepcopy
from operator import itemgetter
from dask.diagnostics import ProgressBar
from multiprocessing.pool import ThreadPool
from pathlib import Path

import wazetools.data.data_transform as dt
import wazetools.utils as utils

log_path = Path(__file__).parent.parent / 'logging.yaml'
logging.config.dictConfig(yaml.load(open(log_path)))

def get_highway_priority():

    return pd.read_json('../scripts/highway_priority.json')


def bbox_from_point(point, radius):

    if not isinstance(point, shapely.geometry.point.Point):
        point = shapely.geometry.Point(point)

    return box(point.x - radius,
               point.y - radius,
               point.x + radius,
               point.y + radius,
               )


def circle_from_segment(segment, size=0.0001):

    return list(segment.centroid.buffer(size).exterior.coords)


def get_priority_G(osm, city, force=False):

    path = '../processed_data/join/priority_G_{}.p'.format(city)

    if os.path.exists(path) and not force:
        logging.debug('Loading cached priority G')
        priority_G = pickle.load(open(path, 'rb'))

    else:

        highway_priority = get_highway_priority()

        priorities = {'high': [1],
                      'medium': [1, 2],
                      'low': [1, 2, 3],
                      'all': [1, 2, 3, 4, 5]}

        priority_G = {}

        for priority, values in priorities.items():
            with utils.Logger('Subgraphing {}'.format(priority), 'INFO') as t:
                if priority == 'all':
                    priority_G[priority] = osm['G']
                else:
                    priority_G[priority] = nx.MultiDiGraph(((u, v, d)
                                                            for u, v, k, d in osm['G'].edges(keys=True, data=True)
                                                            if d['highway'] in list(
                        highway_priority[highway_priority['priority'].isin(values)]['highway'])))

        with utils.Logger('Saving to '.format(path), 'INFO') as t:
            pickle.dump(priority_G, open(path, 'wb'))

    return priority_G


def get_edges_from_node(node, osm):

    return osm['edges'][(osm['edges']['u'] == node) | 
                        (osm['edges']['v'] == node)].drop_duplicates(
                            subset=['u', 'v'])

# TODO: Use osm boxes to search
def get_closest_osm_node(node, osm, line=1, radius=0.0059, priority=True):

    utils.Logger(
        'Searching closest node {}'.format(line),
        'INFO').print_message()

    if not isinstance(node, shapely.geometry.point.Point):
        node = shapely.geometry.Point(node)

    # TODO: pass this as args 
    highway_priority = get_highway_priority()

    while True:
        radius = radius + 0.001

        utils.Logger(
            'Incrementing radius to  {}, {}'.format(
                radius, line)).print_message()

        # Get distance from node
        node_box = bbox_from_point(node, radius)
        osm_intersection = osm['nodes'][osm['nodes'][
            'point'].apply(lambda x: x.intersects(node_box))]
        utils.Logger('Intersection Lenght {}, {}'.format(
                                        len(osm_intersection),
                                        line)).print_message()
        if len(osm_intersection) == 0:
            utils.Logger('Passing'.format(line)).print_message()
            continue
        distance = [(row['n'], node.distance(row['point']))
                    for i, row in osm_intersection.iterrows()]

        # Merge with node info
        distance = osm['nodes'].merge(
            pd.DataFrame(distance, columns=['n', 'distance']),
            right_on='n',
            left_on='n').sort_values(by='distance')

        # Get node that connect high priority highway edges
        for i, row in distance.iterrows():

            # Get edges info
            a = get_edges_from_node(row['n'], osm)

            # If there is a priority edge, return node
            if priority:
                if radius > 0.0009 + 0.001 * 10:
                    if a['highway'].apply(
                            lambda x: highway_priority[highway_priority['highway'] == x].values[0][1]).min() < 3:
                        utils.Logger(
                            'Returning low priority  <3, {}'.format(line)).print_message()
                        return row['n']
                else:
                    if a['highway'].apply(
                            lambda x: highway_priority[highway_priority['highway'] == x].values[0][1]).min() < 2:
                        utils.Logger('Returning high priority  <2, {}'.format(line)).print_message()
                        return row['n']
            else:
                if a['highway'].apply(
                            lambda x: highway_priority[highway_priority['highway'] == x].values[0][1]).min():
                        utils.Logger( 'Returning no priority, {}'.format(line)).print_message()
                        return row['n']


def calculate_shortest_path(begin_node, end_node, priority_G, priority=True, i=1):

    utils.Logger(
        'Calculating shortest path {}'.format(i),
        'INFO').print_message()

    for prio in ['high', 'medium', 'low', 'all']:
        if priority and prio != 'low':
            continue

        try:
            return nx.shortest_path(
                priority_G[prio],
                source=begin_node,
                target=end_node)
        except Exception as e:  # (nx.NetworkXNoPath, nx.NodeNotFound):
            True
        try:
            return nx.shortest_path(
                priority_G[prio],
                source=end_node,
                target=begin_node)
        except Exception as e:
            continue
    else:
        return None


def perform_join(osm, path, i=1):

    utils.Logger('Joining aadt+osm {}'.format(i), 'INFO').print_message()
    idx = []
    for first, last in zip(path[:-1], path[1:]):
        idx.append(osm['edges'][((osm['edges']['u'] == first) & (osm['edges']['v'] == last)) | (
            (osm['edges']['v'] == first) & (osm['edges']['u'] == last))].index[0])

    return idx


def join_osm_aadt(
        osm,
        aadt,
        city,
        n_threads=10,
        load_cache=False,
        force_computation=False):

    dask.set_options(pool=ThreadPool(n_threads))

    save_path = '../processed_data/join/{}_'.format(city)
    if os.path.exists(save_path) and not load_cache:

        with utils.Logger('Fetching data from cache', 'INFO') as t:

            utils.Logger('Loading AADT').print_message()
            aadt = pickle.load(open(save_path + 'aadt.csv', 'rb'))
            utils.Logger('Loading AADT').print_message()
            osm = pickle.load(open(save_path + 'osm.csv', 'rb'))
    else:

        with utils.Logger('Calculate priority subgraphs', 'INFO') as t:
            priority_G = get_priority_G(osm, city, force=force_computation)

        with utils.Logger('Check OSM and AADT', 'INFO') as t:

            if 'CP_Number' in osm['edges']:
                osm = dt.get_osm(None, city)
                aadt = dt.get_aadt(city)

        with utils.Logger('Join aadt and osm', 'INFO') as t:

            final = []
            for i, row in aadt.iterrows():
                begin_aadt = dask.delayed(
                    get_closest_osm_node)(row['begin'], osm, i)
                end_aadt = dask.delayed(
                    get_closest_osm_node)(row['end'], osm, i)
                path = dask.delayed(calculate_shortest_path)(
                    begin_aadt, end_aadt, priority_G, i)
                osm_idx = dask.delayed(perform_join)(osm, path, i)
                final.append({'index': i,
                              'begin_aadt': begin_aadt,
                              'end_aadt': end_aadt,
                              'path': path,
                              'osm_idx': osm_idx})

            final = dask.compute(final)[0]

            for f in final:
                osm['edges'].loc[f['osm_idx'],
                                 'CP_Number'] = aadt.loc[f['index'], 'CP_Number']

            utils.Logger('OSM length {}'.format(
                len(osm['edges']))).print_message()
            aadt = aadt.merge(
                pd.DataFrame(final),
                right_on='index',
                left_index=True,
                how='left')
            utils.Logger('OSM length {}'.format(
                len(osm['edges']))).print_message()
            osm['edges'] = osm['edges'].merge(aadt, on='CP_Number', how='left')
            utils.Logger('OSM length {}'.format(
                len(osm['edges']))).print_message()

        with utils.Logger('Saving osm and aadt processed data to {}'.format(path), 'INFO') as t:
            pickle.dump(osm, open(save_path + 'osm.csv', 'wb'))
            pickle.dump(aadt, open(save_path + 'aadt.csv', 'wb'))

    return osm, aadt


# JOIN WAZE

def force_linestring(a):
    a = pd.Series(a)
    b = (a.replace(regex=True, to_replace=r'[A-Z]', value=r'')
          .replace(regex=True, to_replace='\(', value='[')
          .replace(regex=True, to_replace=', ', value='],[')
          .replace(regex=True, to_replace='\)', value=']')
          .replace(regex=True, to_replace=' ', value=',')
          .apply(lambda x: '[' + x[1:-1] + ']]'))
    return LineString(ast.literal_eval(b[0]))


def get_centroids_tuple(osm):

    return list(map(tuple, osm['boxes'][['i', 'j', 'centroid']].values))


def get_closest_centroid(segment, centroids):

    distances = list(map(lambda x: (x[0],
                                    x[1],
                                    segment.distance(x[2])), centroids))
    return sorted(distances, key=itemgetter(2))[0]


def get_boxes_edges_geometries(box_coord, osm, neighbour=1):
    
    ids = list(osm['boxes'][(osm['boxes']['i'] == box_coord[0]) &
                            (osm['boxes']['j'] == box_coord[1])]['osmids_neigh_{}'.format(neighbour)])[0]
    
    return list(map(tuple, 
                    list(osm['edges'][osm['edges']['geometry_id'].isin(ids)][['geometry_id', 'geometry']].values)))


def get_edges_in_box(segment, osm, centroids):

    closest_centroid = get_closest_centroid(segment, centroids)
    
    edges = get_boxes_edges_geometries(closest_centroid, osm)

    return edges


def calculate_azimuth(line1, line2):
    
    m1, m2 = calculate_m(line1), calculate_m(line2)
    
    return np.arctan((m1 - m2) / (1 + m1 * m2))


def calculate_m(line):
    
    x1 = line.xy[0][0]
    x2 = line.xy[0][-1]
    y1 = line.xy[1][0]
    y2 = line.xy[1][-1]

    try:
        return (y2 - y1) / (x1 - x2)
    except ZeroDivisionError:
        return (y2 - y1) / ((x1 - x2) + 0.0000000000000001)

def select_by_intersection(edges, waze_segment, base_buffer, buffer_limit, buffer_step=0.00005):
    
    buffer = deepcopy(base_buffer)
    while True:    

        selected_edges = edges[edges['geometry'].apply(lambda x: x.intersects(waze_segment.buffer(buffer)))]
        if len(selected_edges):
            return selected_edges, buffer + base_buffer
        elif buffer < buffer_limit:
            buffer = buffer + buffer_step
            #print('buffer', buffer)
        else:

            return False, buffer
    

def select_by_angle(selected_edges, waze_segment, angle_selection=0.1, distance_selection=0.0001,
                    stop_angle=0.5):
    
    # To segments
    selected_edges_segments = pd.DataFrame()
    for i, row in selected_edges.iterrows():
        selected_edges_segments = pd.concat([selected_edges_segments, dt.get_segments(row)])
    
    selected_edges_segments['distance'] = selected_edges_segments['geometry_segment'].apply(
                                    lambda x: x.distance(waze_segment))
    angle = deepcopy(angle_selection)
    
    #print('angle', angle)
    selected_edges_segments['angle'] = selected_edges_segments['geometry_segment'].apply(
                                        lambda x: abs(calculate_azimuth(x, waze_segment)))

    #print(selected_edges_segments[['distance', 'angle']].sort_values(by=['angle']))
    
    while True:
        
        selected_edges_angle = selected_edges_segments[
                                    (selected_edges_segments['angle'] < angle) &
                                    (selected_edges_segments['distance'] < distance_selection)]
        
        if len(selected_edges_angle):
                return selected_edges[selected_edges['_id'].isin(selected_edges_angle['_id'])]

        elif angle < stop_angle:
            angle = angle + angle_selection
            #print(angle)

        else:
            return pd.DataFrame()


def get_similar_edges(waze_segment, osm, centroids, waze_id, buffer=0.00001, buffer_limit=0.0003):
    
    edges_raw = pd.DataFrame(get_edges_in_box(waze_segment, osm, centroids),
                         columns=['_id', 'geometry'])
    
    while True:
        
        edges = deepcopy(edges_raw)
        
        # Intersection
        if buffer < buffer_limit:
            edges, buffer = select_by_intersection(edges, waze_segment, buffer,
                                               buffer_limit=buffer_limit)
        else:
            edges = False
        
        if isinstance(edges, bool):
            final = pd.DataFrame()
            break
        
        # Angle Selection
        edges = select_by_angle(edges, waze_segment)
        
        if len(edges):

            final = edges
            break
    
    final['id_waze'] = waze_id

    return final


def join_osm_waze(
        osm,
        features,
        city,
        n_threads=10,
        load_cache=True):

    path = Path(__file__).resolve().parents[2] / 'data' / 'processed' / ('waze+osm_' + city + '.csv')

    if os.path.exists(path) and load_cache:

        with utils.Logger('Fetching data from cache'):
            wo = pd.read_csv(path)

    else: 
        with utils.Logger('Boxing Waze by OSM', 'INFO'):
            osm_box = dt.box_around_city(osm)
            features = features[features.apply(lambda x: 
                                        x['geometry_segment'].intersects(osm_box),
                                        axis=1)]

        with utils.Logger('Joining osm and waze with {} threads'.format(n_threads),
                        'INFO'):

            centroids = get_centroids_tuple(osm)

            final = []
            i = 0
            for idx, row in features.iterrows():
                i = i + 1

                osm_segments = dask.delayed(get_similar_edges)(
                                row['geometry_segment'], osm, centroids,
                                row['id_segment'])

                final.append(osm_segments)

            with ProgressBar():
                final = dask.compute(final)[0]

            # To dataframe
            merge = pd.concat(final)
            merge.columns = ['osm_id', 'geometry', 'waze_id']

        with utils.Logger('Saving csv to {}'.format(path)):

            merge.to_csv(path, index=False)

    return merge

def merge_osm_fwaze(
        join_table,
        waze_features,
        osm,
        city,
        features_name,
        load_cache=True):

    path = Path(__file__).resolve().parents[2] / 'data' / 'processed' / ('waze+osm_' + city + '_' + features_name + '.csv')

    if os.path.exists(path) and load_cache:

        with utils.Logger('Fetching data from cache'):
            wo = pd.read_csv(path)

    else: 
        
        with utils.Logger('Merging Waze with OSM'):
            
            ww = join_table.merge(waze_features, right_index=True, left_on='waze_id')
            wo = ww.merge(osm['edges'], right_on='geometry_id', left_on='osm_id', how='right')

            wo = wo.drop(columns=['geometry_x', 'osm_id', 'id_segment'])

            wo = wo.rename(columns={'geometry_segment': 'waze_geometry',
                        'geometry_y': 'osm_geometry',
                        'geometry_id': 'osm_id'})
            

        with utils.Logger('Saving csv to {}'.format(path)):

            wo.to_csv(path, index=False)

    return wo

if __name__ == '__main__':
    pass