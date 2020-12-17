import geopandas as gpd
import pandas as pd
import difflib
from shapely.geometry.base import BaseGeometry

df_all = pd.read_json('https://geo.datav.aliyun.com/areas_v2/bound/all.json')


def get_region_gdf(region_name) -> gpd.GeoDataFrame:
    match_list = difflib.get_close_matches(region_name, df_all['name'], n=1)
    if len(match_list) == 0:
        raise Exception('无法根据名称寻找到匹配的区域')
    region_name = match_list[0]
    region_adcode = df_all[df_all['name'] == region_name]['adcode'].values[0]
    return gpd.read_file(f'https://geo.datav.aliyun.com/areas_v2/bound/{region_adcode}.json')


def get_reigon_geometry(region_name) -> BaseGeometry:
    gdf = get_region_gdf(region_name)
    return gdf.geometry.values[0]


if __name__ == '__main__':
    get_reigon_geometry('武汉')
