import json
import mmap

from mpi4py import MPI

from enum import Enum
from collections import Counter


# one grid class contains the border of one synGrid
class BorderStatus(Enum):
    center = 0
    westBorder = 1
    southBorder = 2
    southWestBoder = 3
    undefined = 5


class Grid(object):

    def __init__(self, name, NW, SE):
        self.west = NW[0]
        self.east = SE[0]
        self.north = NW[1]
        self.south = SE[1]
        self.name = name
        self.borderStatus = BorderStatus.undefined

    def setBorderStatus(self, westBorder, southBorder):
        if self.west == westBorder and self.south == southBorder:
            self.borderStatus = BorderStatus.southWestBoder
        elif self.west == westBorder:
            self.borderStatus = BorderStatus.westBorder
        elif self.south == southBorder:
            self.borderStatus = BorderStatus.southBorder
        else:
            self.borderStatus = BorderStatus.center

    def insideGrid(self, loc):
        if self.borderStatus == BorderStatus.center:
            return loc[0] > self.west and loc[0] <= self.east and loc[1] > self.south and loc[1] <= self.north
        elif self.borderStatus == BorderStatus.westBorder:
            return loc[0] >= self.west and loc[0] <= self.east and loc[1] > self.south and loc[1] <= self.north
        elif self.borderStatus == BorderStatus.southBorder:
            return loc[0] > self.west and loc[0] <= self.east and loc[1] >= self.south and loc[1] <= self.north
        elif self.borderStatus == BorderStatus.center:
            return loc[0] > -self.west and loc[0] <= self.east and loc[1] >= self.south and loc[1] <= self.north

    def __str__(self):
        return "[id:%s,W:%s,E:%s,N:%s,S:%s,BorderStatus:%s]" % (
        self.name, self.west, self.east, self.north, self.south, self.borderStatus)


# Twitter class, contains the language label and location
class Twitter(object):
    def __init__(self, lang, loc):
        self.lang = lang
        self.loc = loc

    def __str__(self):
        return "[lang:%s,loc:%s,%s]" % (self.lang, self.loc[0], self.loc[1])


# GridLand class contains a grid and a language dict for counting the usage of languages
class GridLang(object):
    def __init__(self, grid):
        self.grid = grid
        self.langDict = {}
        self.totalTweets = 0

    def insertTwitter(self, twitter):
        if self.grid.insideGrid(twitter.loc):
            if not self.langDict.__contains__(twitter.lang):
                self.langDict[twitter.lang] = 1
            else:
                self.langDict[twitter.lang] += 1
            self.totalTweets += 1
            return True
        else:
            return False

    def __str__(self):
        return "grid:%s,dict:%s" % (self.grid.__str__(), self.langDict)

    def sortGridLangResult(self):
        sorted(self.langDict, key=lambda x: x[1], reverse=True)


# GridLangMap class, contains a list of grids and totalNum of Twitters
class GridLangMap(object):
    def __init__(self):
        self.gridLangList = []
        self.totalGrids = 0
        self.totalTwitters = 0
        self.westBorder = 999999
        self.southBorder = 999999
        self.eastBorder = -999999
        self.northBorder = -999999

    def addGrid(self, gridLang):
        self.gridLangList.append(gridLang)
        self.totalGrids += 1

    def postGridProcess(self):
        if self.totalGrids != 0:
            self.sortGrid()
            # assume that the grid provided is a square
            self.northBorder = self.gridLangList[0].grid.north
            self.westBorder = self.gridLangList[0].grid.west
            self.southBorder = self.gridLangList[self.totalGrids - 1].grid.south
            self.eastBorder = self.gridLangList[self.totalGrids - 1].grid.east
            for gridLang in self.gridLangList:
                gridLang.grid.setBorderStatus(self.westBorder, self.southBorder)

    def sortGrid(self):
        self.gridLangList.sort(key=lambda x: (-x.grid.north, x.grid.west))

    def insertTwitter(self, twitter):
        if twitter == None:
            return
        for gridLang in self.gridLangList:
            if gridLang.insertTwitter(twitter):
                self.totalTwitters += 1
                return

    def sortGridLangResult(self):
        for gridLang in gridLangMap.gridLangList:
            gridLang.sortGridLangResult()
            # print(gridLang.langDict)


# read synGrid file and pack the contents into a GridLangMap class
def gridProcessor(gridFilePath):
    # magical numbers for NorthWest and SouthEast node in coordinates
    NW_IDX = 0
    SE_IDX = 2
    try:
        f = open(gridFilePath)
        sydGrid = json.load(f)
        # print(sydGrid['features'][1]['properties']['id'])
        # print(sydGrid['features'][3]['geometry']['coordinates'])
        gridLangMap = GridLangMap()
        for i in sydGrid['features']:
            NW = i['geometry']['coordinates'][0][NW_IDX]
            SE = i['geometry']['coordinates'][0][SE_IDX]
            name = i['properties']['id']  # use the id provided to label the grid
            gridLangMap.addGrid(GridLang(Grid(name, NW, SE)))

        gridLangMap.postGridProcess()
        return gridLangMap
    except IOError:
        print('failed to open:', gridFilePath)
    finally:
        try:
            if f != None:
                f.close()
        except IOError:
            print('failed to close:', gridFilePath)


# pack one json object into a Twitter class
def twitterJsonObjectProcessor(twitterJsonObject):
    lang = twitterJsonObject['doc']['lang']
    posi = twitterJsonObject['doc']['coordinates']
    loc = None
    if posi != None:
        loc = posi['coordinates']
    elif twitterJsonObject['doc']['geo'] != None:
        posi = twitterJsonObject['doc']['geo']
        loc = [posi['coordinates'][1], posi['coordinates'][0]]

    if lang != None and loc != None:  # a filter to exclude the twitter without lang or coordinate label
        return Twitter(lang, loc)
    return None


# the main function, read Twitter file with mmap technique
# TODO
def mmapTwitterProcessor(twitterFilePath, rankNum, girdLangMap):
    try:
        with open(twitterFilePath, 'rb') as f:
            mmp = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            header = json.loads(mmp.readline().decode('utf-8') + ']}');
            totalRows = header['total_rows']
            count = 1  # skip the first row
            for line in iter(mmp.readline, b''):
                count += 1
                isEnd = totalRows == count
                jsonObject = jsonLoadProcessor(line, isEnd, count)
                # print(jsonObject['doc']['lang'])
                # print(jsonObject['doc']['coordinates'])
                twitter = twitterJsonObjectProcessor(jsonObject)
                # if twitter != None:
                #     print(twitter)
                gridLangMap.insertTwitter(twitter)
    except IOError:
        print('failed to open:', twitterFilePath)
    finally:
        try:
            if f != None:
                f.close()
        except IOError:
            print('failed to close:', twitterFilePath)


# convert the byteArray got by mmap into jsonObject
def jsonLoadProcessor(byteArray, isEnd, count):
    str = byteArray.decode('utf-8')
    # print(isEnd)

    for index in range(len(str) - 1, 0, -1):  # find the last '}' and convert the string into valid json format
        if str[index] == '}':
            if not isEnd:
                break
            else:
                isEnd = False  # according to the structure of the twitter file, the second to last '}' is valid for the last line
    # print(count)
    # print(str[0:index+1])
    return json.loads(str[0:index + 1])  # index+1 so that we include the last '}' in the slice


# print the final result in given format
def printFinalResult(gridLangMap):
    gridNameList = ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C1', 'C2', 'C3', 'C4', 'D1', 'D2', 'D3', 'D4']
    printFormat = '| {0:^5} | {1:^15} | {2:^25} | {3:^100} |'
    print((printFormat).format('Cell', '#Total_Tweets', '#Number_of_Languages_Used', '#Top 10 Languages & #Tweets'))
    for i in range(gridLangMap.totalGrids):
        gridLang = gridLangMap.gridLangList[i]
        gridName = gridNameList[i]
        totalTweets = gridLang.totalTweets
        numLang = 1
        # TODO:get the top 10 languages
        top10Language = '(English-9,000, Chinese-555, French-444, …Greek-66)'
        print((printFormat).format(gridName, totalTweets, numLang,
                                   top10Language))

    # print(("%-4s %-20s %20s %-100s\n"%('A1',11111,11,'(English-9,000, Chinese-555, French-444, …Greek-66)')))
    # for i in range(gridLangMap.totalGrids):


'''for parallelize'''


# 进程 0专用：读取第一行，返回 total_rows
def getRows(filename):
    total_rows = 0
    with open(filename, "r") as f:
        first_row = f.readline()
        header = json.loads(first_row + "]}")
        total_rows = header["total_rows"]
        # print("total_rows =", total_rows)   # for debug
    f.close()

    return total_rows


# 并行读取文件
import linecache
from itertools import islice


def parallelRead(filename, gridLangMap, total_rows, start_index, chunk_size):
    # TODO: 需要效率更高的读取方法
    # try:
    #     with open(filename, "r") as f:
    #         print("start index =", start_index)
    #         print("chunk size = ", chunk_size)
    #         lines = list(islice(f, start_index, start_index + chunk_size))
    #         print("读取到的行数 = ", len(lines))

    #         for i in range(0, len(lines)):
    #             # 貌似jsonLoadProcessor 不需要count这个参数？
    #             if i + start_index == total_rows - 1:
    #                 jsonObject = jsonLoadProcessor(lines[i], True, 0)
    #             else:
    #                 jsonObject = jsonLoadProcessor(lines[i], False, 0)

    #             print("正在处理第 ", str(i + start_index), "行")

    #             twitter = twitterJsonObjectProcessor(jsonObject)
    #             if twitter != None:
    #                 gridLangMap.insertTwitter(twitter)

    # except:
    #     print("Error !")

    # linecache.getline 按行号读
    start_index += 1
    print("start index =", start_index)
    print("chunk size = ", chunk_size)
    for i in range(start_index, start_index + chunk_size):
        line = linecache.getline(filename, i)
        line = line.replace("\n", "").replace("\r", "")

        if i == total_rows:  # 最后一行单独处理
            line = line[:-2]
        else:
            line = line[:-1]

        jsonObject = json.loads(line)
        twitter = twitterJsonObjectProcessor(jsonObject)
        if twitter != None:
            gridLangMap.insertTwitter(twitter)


# TODO：0号进程把所有收到的数据合并成最终答案
def mergeData(combine_data):
    print("\n结果个数：", len(combine_data))
    for d in combine_data:
        for l in d.gridLangList:
            print(l)
        print()
    print("\n\n")

    results = combine_data[0]
    for i in range(1, len(combine_data)):
        for j in range(0, 16):
            results.gridLangList[j].totalTweets += combine_data[i].gridLangList[j].totalTweets

            for k in combine_data[i].gridLangList[j].langDict.keys():
                if k in results.gridLangList[j].langDict.keys():
                    results.gridLangList[j].langDict[k] += combine_data[i].gridLangList[j].langDict[k]
                else:
                    results.gridLangList[j].langDict[k] = combine_data[i].gridLangList[j].langDict[k]

    return results


# main function
if __name__ == '__main__':

    filename = "smallTwitter.json"

    # 并行
    comm = MPI.COMM_WORLD
    comm_rank = comm.Get_rank()
    comm_size = comm.Get_size()

    data = None
    gridLangMap = None

    # 1-core-1-node 单进程
    if comm_size == 1:
        gridLangMap = gridProcessor('sydGrid.json')
        mmapTwitterProcessor(filename, 1, gridLangMap)
        for gridLang in gridLangMap.gridLangList:
            print(gridLang)

        print("***************************************************")
        print(gridLangMap.westBorder, gridLangMap.northBorder)
        gridLangMap.sortGridLangResult()
        printFinalResult(gridLangMap)


    # 多进程
    else:
        if comm_rank == 0:
            # 0号进程读取grid文件，发给其他进程
            gridLangMap = gridProcessor('sydGrid.json')
            # 获取文件总行数， 发给其他进程
            total_rows = getRows(filename)

            data = {"gridMap": gridLangMap, "total_rows": total_rows}

        # 所有进程都执行，但是只有0会发送
        # data = comm.bcast(data if comm_rank == 0 else None, root=0)
        data = comm.bcast(data, root=0)

        # rank != 0 的其他进程
        gridLangMap = data["gridMap"]

        chunk_size = data["total_rows"] // comm_size
        start_index = chunk_size * comm_rank + 1  # 每个进程开始读的起始行数，需要跳过第一行
        # 最后一个进程要处理的数量不同
        if comm_rank == comm_size - 1:
            chunk_size = data["total_rows"] - chunk_size * comm_rank - 1

        parallelRead(filename, gridLangMap, data["total_rows"], start_index, chunk_size)

        # 给结果排序
        gridLangMap.sortGridLangResult()
        # data = gridLangMap
        print("进程", comm_rank, " 的统计结果为：")
        printFinalResult(gridLangMap)
        print("\n")

        # 进程 0 合并数据
        combine_data = comm.gather(gridLangMap, root=0)

        if comm_rank == 0:
            final_result = mergeData(combine_data)
            print("最终结果:")
            printFinalResult(final_result)







