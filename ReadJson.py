import json
import re


def getGrid(filename):
    grid_name = ['A1', 'A2', 'A3', 'A4',
                 'B1', 'B2', 'B3', 'B4',
                 'C1', 'C2', 'C3', 'C4',
                 'D1', 'D2', 'D3', 'D4']

    with open(filename, "r", encoding='utf-8') as f:
        data = json.load(f)

    grid = {}
    # points放边界四个角的集合
    points = set()
    for feature in data["features"]:
        latitude =

    lats = []
    longs = []






    print(s)
    return grid


def readData(filename):
    with open(filename, "r", encoding='utf-8') as f:
        data = json.load(f)

    lang_dict = {}
    for row in data["rows"]:

        lang = row["doc"]["lang"]
        if lang in lang_dict.keys():
            lang_dict[lang] += 1
        else:
            lang_dict[lang] = 1

    # 按value从大到小排序
    list = sorted(lang_dict.items(), key=lambda item:item[-1], reverse=True)
    result = {k:v for k,v in list}
    return result

def main():
    dict = getGrid("data/sydGrid.json")
    print(dict)

    result = readData("data/tinyTwitter.json")
    print(result)

main()





