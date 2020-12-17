from shapely.geometry.base import BaseGeometry
from shapely.geometry import box
from slab.api_service.baidumapapi import initSessionAsync, baiduApiAsync, closeSessionAsync
from slab.logger.base_logger import stream_file_logger
from slab.geocode.trans_util import bd09_to_wgs84
import asyncio
from region_search import get_reigon_geometry
import pandas as pd

logger = stream_file_logger()


def split_bound(geometry: BaseGeometry, split_num=2, region_restrict: BaseGeometry = None):
    '''
    根据几何形状的外接矩形进行切割，得到更小的矩形
    :param geometry: 进行边界分割的几何形状
    :param split_num: 沿轴的切割数量，得到的矩形为切割数的平方
    :param region_restrict: 落在限制区域外的矩形不会被返回
    :return: 分割后的矩形数组
    '''

    min_x, min_y, max_x, max_y = geometry.bounds
    x_interval = (max_x - min_x) / split_num
    y_interval = (max_y - min_y) / split_num

    box_list = []
    for x_index in range(split_num):
        for y_index in range(split_num):
            box_split = box(min_x + x_index * x_interval,
                            min_y + y_index * y_interval,
                            min_x + (x_index + 1) * x_interval,
                            min_y + (y_index + 1) * y_interval
                            )
            if region_restrict is None:
                box_list.append(box_split)
            else:
                if box_split.intersects(region_restrict):
                    box_list.append(box_split)
    return box_list


def bound_to_baidu_str(geometry: BaseGeometry):
    min_x, min_y, max_x, max_y = geometry.bounds
    return f'{min_y},{min_x},{max_y},{max_x}'


async def scrape_bound(geometry: BaseGeometry, keyword, page_num=0, coord_type=2,
                       region_restrict: BaseGeometry = None, total_threshold=60):
    '''

    :param geometry:
    :param keyword:
    :param page_num:
    :param coord_type: 默认使用高德的行政区作为边界，因此输入坐标选择国测局的火星坐标系
    :param region_restrict:
    :param total_threshold: 搜索结果返回数量阈值，超过这个阈值则细分搜索区域，使每次搜索结果不高于这个阈值
    :return:
    '''

    res = await baiduApiAsync(
        f'http://api.map.baidu.com/place/v2/search?'
        f'output=json&scope=2&page_size=20'
        f'&query={keyword}'
        f'&page_num={page_num}'
        f'&coord_type={coord_type}'
        f'&bounds={bound_to_baidu_str(geometry)}'
    )

    total_num = res['total']

    if page_num == 0:
        print('找到要素', total_num, '个')

    # 如果此区域不存在点
    if total_num == 0:
        return []
    # 数量超过阈值，需要划分小格子。把当前区域划分成4个小格子
    elif total_num >= total_threshold:
        # 判断格子是否过小而仍无法分割
        min_x, min_y, max_x, max_y = geometry.bounds
        if f'{min_x:.4f}' != f'{max_x:.4f}':
            task_list = []
            for bound_split in split_bound(geometry, region_restrict=region_restrict):
                # 创建异步任务并添加至事件循环
                task = asyncio.create_task(scrape_bound(bound_split, keyword, region_restrict=region_restrict))
                task_list.append(task)
            task_result_list = await asyncio.gather(*task_list)
            result_list = [result for task_result in task_result_list for result in task_result]
            return result_list
    # 如果翻页后，此页没有
    elif len(res['results']) == 0:
        return []

    # 遍历每条结果，对具体POI点进行处理
    result_list = []

    count = len(res['results'])
    for r in res['results']:
        # 访问字段异常
        try:
            result_object = {
                'name': r['name'],
                'uid': r['uid'],
                'latitude_bd09': float(r["location"]["lat"]),
                'longitude_bd09': float(r["location"]["lng"]),
                'address': r["address"],
                'area': r["area"]
            }

            if r["detail"] == 1:
                # 有时候没有type字段
                if 'type' in r["detail_info"]:
                    result_object['type'] = r["detail_info"]["type"]
                # 有时候没有tag字段
                if 'tag' in r["detail_info"]:
                    result_object['tag'] = r["detail_info"]["tag"]

            if 'province' in r:
                result_object['province'] = r['province']
            if 'city' in r:
                result_object['city'] = r['city']

            result_object['longitude_wgs84'], result_object['latitude_wgs84'] = bd09_to_wgs84(
                result_object['longitude_bd09'], result_object['latitude_bd09'])
            result_list.append(result_object)
        except Exception as e:
            logger.exception('访问单个结果字段时出现异常')

    print('完成要素：%d / %d' % (20 * page_num + count, total_num))
    # 如果等于二十个，需要翻页。否则不用翻页
    if count == 20:
        result_list.extend(await scrape_bound(geometry, keyword, page_num + 1, region_restrict=region_restrict))
    return result_list


async def scrape_region(region_name, keyword, init_split=15):
    region_geometry = get_reigon_geometry(region_name)
    bound_list = split_bound(region_geometry, split_num=init_split, region_restrict=region_geometry)
    task_list = []
    for bound in bound_list:
        task = scrape_bound(bound, keyword, region_restrict=region_geometry)
        task_list.append(task)
    await initSessionAsync()
    task_result_list = await asyncio.gather(*task_list)
    await closeSessionAsync()
    result_list = [result for task_result in task_result_list for result in task_result]
    return result_list


def scrape_batch(city_name_list, search_name_list):
    for city_name in city_name_list:
        for search_name in search_name_list:
            result_list = asyncio.run(
                scrape_region(city_name, search_name)
            )
            df = pd.DataFrame(result_list).drop_duplicates()
            df.to_csv(f'{city_name}_{search_name}.csv', index=False, encoding='UTF-8')
            print(f'城市：{city_name}，关键词：{search_name}，共获取{len(df)}条结果')


def main():
    scrape_batch(
        ['武汉', '深圳'],
        ['地铁', '公司', '园区']
    )


if __name__ == '__main__':
    main()
