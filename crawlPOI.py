# -*- coding: UTF-8 -*-
import requests
import json
import time
import sys
from openpyxl import Workbook
from pathlib import Path

import os
# 更改当前目录为文件锁在目录
current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)

# 读取配置文件
CONFIG_FILE = './setting.json'
f = open(CONFIG_FILE, "r", encoding='utf-8')
setting = json.load(f)
print(setting)

setting['akIndex'] = 0

# 关注区域的左下角和右上角百度地图坐标(经纬度）
Boundary = setting['boundary']
# 定义细分窗口的数量，横向X * 纵向Y
WindowSize = setting['windowSize']


# def getAK():
#     if len(setting['apiKey']) == setting['akIndex']:
#         raise Exception("AK耗尽")
#     key = setting['apiKey'][setting['akIndex']]
#     setting['akIndex'] = setting['akIndex'] + 1

#     print('更换AK，当前AK是第 %d 个 \n%s' % (setting['akIndex'], key))
#     return key

akIndex = -1 
def getAK(invalidate=False):
    global akIndex
    if akIndex == -1:
        akIndex = 0
        return setting['apiKey'][0]
    if invalidate:
        print(
            f'AK:{setting["apiKey"][akIndex]}失效。剩余{len(setting["apiKey"])-1}个AK'
        )
        setting['apiKey'].remove(setting['apiKey'][akIndex])
        akIndex = -1
    if len(setting['apiKey']) == 0:
        raise Exception("AK耗尽")

    akIndex += 1
    akIndex %= len(setting['apiKey'])
    key = setting['apiKey'][akIndex]

    # print('更换AK，当前AK是第 %d 个 \n%s' % (akIndex, key))
    return key

# 获取初始百度Key
# API_KEY = getAK()


def getRect(boundary,
            windowSize={
                'xNum': 1.0,
                'yNum': 1.0
            },
            windowIndex=0,
            stringfy=True):
    """
    获取小矩形的左上角和右下角坐标字符串（百度坐标系） 
    :param bigRect: 关注区域坐标信息
    :param windowSize:  细分窗口数量信息
    :param windowIndex:  Z型扫描的小矩形索引号
    :return: lat,lng,lat,lng
    """
    offset_x = (boundary['right']['x'] -
                boundary['left']['x']) / windowSize['xNum']
    offset_y = (boundary['right']['y'] -
                boundary['left']['y']) / windowSize['yNum']
    left_x = boundary['left']['x'] + offset_x * (windowIndex %
                                                 windowSize['xNum'])
    left_y = boundary['left']['y'] + offset_y * (windowIndex //
                                                 windowSize['yNum'])
    right_x = (left_x + offset_x)
    right_y = (left_y + offset_y)
    if stringfy:
        return str(left_y) + ',' + str(left_x) + ',' + str(
            right_y) + ',' + str(right_x)
    else:
        return {
            "left": {
                "x": left_x,
                "y": left_y
            },
            "right": {
                "x": right_x,
                "y": right_y
            }
        }


def fetchPOI(keyword, boundary, ws):
    # time.sleep(0.003)
    # global API_KEY
    pageNum = 0
    first_it = True
    while True:
        URL = "http://api.map.baidu.com/place/v2/search?query="+keyword+ \
            "&bounds=" + getRect(boundary) + \
            "&output=json" +  \
            "&ak=" + getAK() + \
            "&scope=2" + \
            "&page_size=20" + \
            "&page_num=" + str(pageNum)
            # 如果未转换，使用工具返回的所有坐标，均为BD09
            # "&coord_type=wgs84ll"+ \ 
        try:
            resp = requests.get(URL)
        except Exception as e:
            print('请求API接口时发生异常')
            print(e)
            continue
        # if resp.status_code != 200:
        #     print('请求状态异常')
            
            
        
        # 这里为什么会有异常？
        # "address":"浙江省嘉兴市桐乡市崇福镇茅桥埭村320国道旁",
        # 返回的结果有未知字符，导致解析失败。
        # 跳过即可
        try:
            res = json.loads(resp.text)
        except Exception as e:
            print(resp.text)
            print(URL)
            print('json文本解析出现异常')
            print(e)
            pageNum += 1
            
        if res['status'] != 0:
            print(res)
            # 如果配额超限，更换AK，重新来过
            if res['status'] == 4 or res['status'] == 302 \
            or ('配额' in res['message'] and '并发' not in res['message']):
                print(res)
                getAK(True)
                continue
            else:
                # 如果返回结果出现错误
                print('返回结果出现错误')
                continue

        total = res['total']

        if first_it:
            print('找到要素', total, '个')
            first_it = False

        # 如果此区域不存在点
        if total == 0:
            break
        # 如果翻页后，此页没有
        elif len(res['results']) == 0:
            break
        # 超过了400个，需要划分小格子。把当前区域划分成4个小格子
        elif total == 400:
            for i in range(4):
                fetchPOI(
                    keyword,
                    getRect(boundary, {
                        'xNum': 2.0,
                        'yNum': 2.0
                    }, i, False), ws)
            # 递归完成之后，跳出循环
            break
        else:
            count = len(res['results'])
            for r in res['results']:
                # 访问字段异常
                try:
                    values = [
                        r['name'],
                        float(r["location"]["lat"]),
                        float(r["location"]["lng"]), r["address"], r["area"]
                    ]
                    if r["detail"] == 1:
                        # 有时候没有type字段
                        if 'type' in r["detail_info"]:
                            values.append(r["detail_info"]["type"])
                        # 有时候没有tag字段
                        if 'tag' in r["detail_info"]:
                            values.append(r["detail_info"]["tag"])
                    ws.append(values)
                except Exception as e:
                    print('访问字段异常')
                    print(r)
                    print(e)
                
            print('完成要素：%d / %d' % (20 * pageNum + count, total))
            # 如果等于二十个，需要翻页。否则不用翻页
            if count == 20:
                pageNum += 1
            else:
                break


def requestBaiduApi(keyword, ws, boundary):
    # 声明全局变量，从而可以对其进行赋值（Python没有声明关键字的特点
    # global API_KEY
    # 添加标题
    ws.append(['名称', '纬度', '经度', '地址', '区县', '一类', '二类'])
    # 循环视口
    windowNum = int(WindowSize['xNum'] * WindowSize['yNum'])
    for i in range(windowNum):
        rect = getRect(boundary, WindowSize, i, stringfy=False)

        print('当前搜索窗口：%d / %d (%s)' % (i + 1, windowNum, keyword))
        fetchPOI(keyword, rect, ws)


def main():
    # 爬取数据的时候，应该使用csv格式存储，而不是excel。

    # 在关键词多的时候，一个工作簿存在了大量的数据，写入很慢。
    # 应该分开存储
    for i in range(len(setting['city'])):
        # 创建一个工作簿
        for keyword in setting['keyWord']:
            wb = Workbook(write_only=True)
            # 只写工作簿没有默认工作表，需要需要创建工作表
            ws = wb.create_sheet()
            requestBaiduApi(keyword, ws, setting['boundary'][i])
            # 保存工作簿
            Path(f'./{setting["city"][i]}').mkdir(parents=True,exist_ok=True)
            wb.save(f'./{setting["city"][i]}/{keyword}.xlsx')
            time.sleep(1)

if __name__ == '__main__':
    main()