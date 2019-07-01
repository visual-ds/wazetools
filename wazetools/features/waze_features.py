import pandas as pd
from numpy import dtype
import wazetools.data.join as join
import wazetools.utils as utils



###########
# Features
###########

def aggregations_generator(columns, 
                           statistics=['mean','max','min',
                                       'median','std','count',
                                       'quant_05','quant_25',
                                       'quant_75','quant_95',
                                       'kurt','skew'],
                           new_statistics={}):
    
    if not isinstance(columns, list):
        raise TypeError ('columns should be a list')
    
    if not isinstance(statistics, list):
        raise TypeError ('statistics should be a list')
    
    basic_statistics = {
                       'mean':'mean',
                       'max': 'max',
                       'min': 'min',
                       'median': 'median',
                       'std': 'std',
                       'count': 'count',
                       'quant_05': lambda x: x.quantile(0.05),
                       'quant_25': lambda x: x.quantile(0.25),
                       'quant_75': lambda x: x.quantile(0.75),
                       'quant_95': lambda x: x.quantile(0.95),
                       'kurt': lambda x: x.kurt(),
                       'skew': 'skew',}
    
    statistics = {key: value for key, value in basic_statistics.items() if key in statistics}
    
    statistics.update(new_statistics)
    
    return {
        column: {
            column + '_' + statistic: value 
            for statistic, value in statistics.items()} 
        for column in columns}

def simple_statistics(waze, features):

    with utils.Logger('Calculating Simple Statistics', 'INFO'):

        if 'speedKMH' not in waze.columns:
            waze['speedKMH'] = waze['speed_kmh']

        aggregations = aggregations_generator(['speed_kmh', 'delay'])
        features = waze.groupby('id_segment').agg(aggregations)
        features.columns = features.columns.get_level_values(1)

    return features

def basic_info(waze, features):

    with utils.Logger('Extracting unique values', 'INFO'):

        unique_columns = ['id_segment', 'start_node', 'end_node', 'road_type',
                  'street', 'city', 'country', 'line', 'line_geo', 'type',
                 'geometry']

        return  waze.drop_duplicates(subset=['id_segment'])[unique_columns].set_index('id_segment')

def segment_length(waze, features):

    with utils.Logger('Calculating segment length', 'INFO'):  
        features = null_feature(waze, columns=['geometry_segment'])
        features['segment_length'] = features['geometry_segment'].apply(
                                                    lambda x: x.length)

        return features

def level_to_free_flow(x):

    level_to_free_flow_speed_relation = {
            1: (0.80, 0.61),
            2: (0.60, 0.41),
            3: (0.40, 0.21),
            4: (0.20, 0.01),
            5: (0, 0) }

    if x['level'] == 5:
        return None, None
    
    interval = level_to_free_flow_speed_relation[x['level']]
    speed_min, speed_max = x['speed_kmh']/interval[0],  x['speed_kmh']/interval[1]

    return speed_min, speed_max

def improve_free_flow_estimate(x):
    
    speed_min = x['free_flow_speed_min'].max()
    speed_max = x['free_flow_speed_max'].min()
    
    return speed_min, speed_max

def free_flow_speed_estimation(waze, features):
    """Estimates the maximum and minimum free flow speed
    values based on the fact that the value level is
    a proportion of the speed.  
    
    Returns
    -------
    'free_flow_speed_min', 'free_flow_speed_max'
        The bounds of the estimation
    """


    with utils.Logger('Estimating Free Flow Speed', 'INFO'):  

        waze[['free_flow_speed_min', 'free_flow_speed_max']] = (
            waze.apply(level_to_free_flow, axis=1).apply(pd.Series))

        features = null_feature(waze)

        features[['free_flow_speed_min', 'free_flow_speed_max']] = (
            waze[['id_segment', 'free_flow_speed_min', 'free_flow_speed_max']]
            .groupby('id_segment').apply(improve_free_flow_estimate).apply(pd.Series))

        return features
        
def fake_feature(waze, features):
    """A feature prototype.

    How to use:
    - copy this
    - change the name
    - change the log content
    - write some feature code.
    - add the function name to the desired_features parameter on
    build_features

    Rules:
    1. If this feature depends of other features, solve this 
    inside this function. Do not excepct that someone will know
    which feature to ask for.
    2. Name your features properly. We like to understand them and
    make sure that no other feature already have this name.
    
    Parameters
    ----------
    waze : pd.DataFrame
        Raw waze data
    features : pd.DataFrame
        Unique segment data
    
    Returns
    -------
    pd.DataFrame
        Features -- Unique segment with specific data. The index has to be
        the segment_id, the columns your features.
    """

    with utils.Logger('This is doing something...', 'INFO'):  

        # Null feature - it just construct a DataFrame with the index as 
        # id_segment and a set of columns of your choice.
        features = null_feature(waze, columns=[])
    
        # Some code ...

        return features

###############
# Features Tools
###############

def merge_features(f1, f2):

    return pd.concat([f1, f2], axis=1, join='outer')

def null_feature(waze, columns=[]):

    return waze.drop_duplicates(subset=['id_segment']).set_index('id_segment')[columns]

def build_jams_features(waze, features=None,
                  desired_features=['simple_statistics',
                                    'basic_info',
                                    'segment_length',
                                    'free_flow_speed_estimation']):

    if features is None:
        features = pd.DataFrame()
    
    for function in desired_features:

        features = merge_features(features, globals()[function](waze, features))

    return features

if __name__ == '__main__':
    pass
