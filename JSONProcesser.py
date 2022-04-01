import json
import mmap

# one grid class contains the border of one synGrid
class Grid(object):
    def __init__(self,name,NW,SE):
        self.west=NW[0]
        self.east=SE[0]
        self.north=NW[1]
        self.south=SE[1]
        self.name=name

    #TODO: 增加边界条件判断
    def insideGrid(self,loc):
        return loc[0]>self.west and loc[0]<=self.east and loc[1]>self.south and loc[1]<=self.north

    def __str__(self):
        return "[id:%s,W:%s,E:%s,N:%s,S:%s]"%(self.name,self.west,self.east,self.north,self.south)

# Twitter class, contains the language label and location
class Twitter(object):
    def __init__(self,lang,loc):
        self.lang = lang
        self.loc = loc

    def __str__(self):
        return "[lang:%s,loc:%s,%s]"%(self.lang,self.loc[0],self.loc[1])

# GridLand class contains a grid and a language dict for counting the usage of languages
# TODO:加入字典的排序和输出功能
class GridLang(object):
    def __init__(self,grid):
        self.grid = grid
        self.langDict = {}
        self.totalTweets = 0

    def insertTwitter(self,twitter):
        if self.grid.insideGrid(twitter.loc):
            if not self.langDict.__contains__(twitter.lang):
                self.langDict[twitter.lang]=1
            else:
                self.langDict[twitter.lang]+=1
            self.totalTweets+=1
            return True
        else:
            return False
    def __str__(self):
        return "grid:%s,dict:%s"%(self.grid.__str__(),self.langDict)

# GridLangMap class, contains a list of grids and totalNum of Twitters
class GridLangMap(object):
    def __init__(self):
        self.gridLangList = []
        self.totalGrids = 0
        self.totalTwitters = 0

    def addGrid(self,gridLang):
        self.gridLangList.append(gridLang)
        self.totalGrids+=1

    def insertTwitter(self,twitter):
        if twitter == None:
            return
        for gridLang in self.gridLangList:
            if gridLang.insertTwitter(twitter):
                self.totalTwitters+=1
                return

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
            NW=i['geometry']['coordinates'][0][NW_IDX]
            SE=i['geometry']['coordinates'][0][SE_IDX]
            name = i['properties']['id'] # use the id provided to label the grid
            gridLangMap.addGrid(GridLang(Grid(name,NW,SE)))
        return gridLangMap
    except IOError:
        print('failed to open:', gridFilePath)
    finally:
        try:
            if f != None:
                f.close()
        except IOError:
            print('failed to close:', gridFilePath)

# # (deprecated) use json.load to read twitter file
# def twitterProcessor(twitterFilePath,girdLangMap):
#     f = open(twitterFilePath,encoding='utf-8')
#     twiterArray = json.load(f)
#     for twitterJsonObject in twiterArray['rows']:
#         lang = twitterJsonObject['doc']['lang']
#         loc = twitterJsonObject['doc']['coordinates']
#         if lang != None and loc != None:
#             twitter = Twitter(lang,loc['coordinates'])
#             print(twitter)
#             gridLangMap.insertTwitter(twitter)


# pack one json object into a Twitter class
def twitterJsonObjectProcessor(twitterJsonObject):
    lang = twitterJsonObject['doc']['lang']
    loc = twitterJsonObject['doc']['coordinates']
    if lang != None and loc != None: # a filter to exclude the twitter without lang or coordinate label
        return Twitter(lang,loc['coordinates'])
    return None

# the main function, read Twitter file with mmap technique
# TODO: 加入并行处理模块
def mmapTwitterProcessor(twitterFilePath,rankNum,girdLangMap):
    try:
        with open(twitterFilePath,'rb') as f:
            mmp = mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ)
            header = json.loads(mmp.readline().decode('utf-8')+']}');
            totalRows = header['total_rows']
            count = 1 #skip the first row
            for line in iter(mmp.readline,b''):
                count +=1
                isEnd = totalRows==count
                jsonObject = jsonLoadProcessor(line,isEnd,count)
                # print(jsonObject['doc']['lang'])
                # print(jsonObject['doc']['coordinates'])
                twitter = twitterJsonObjectProcessor(jsonObject)
                if twitter != None:
                    print(twitter)
                gridLangMap.insertTwitter(twitter)
    except IOError:
        print('failed to open:',twitterFilePath)
    finally:
        try:
            if f != None:
                f.close()
        except IOError:
            print('failed to close:', twitterFilePath)

# convert the byteArray got by mmap into jsonObject
def jsonLoadProcessor(byteArray,isEnd,count):
    str = byteArray.decode('utf-8')
    # print(isEnd)

    for index in range(len(str) - 1, 0, -1):# find the last '}' and convert the string into valid json format
        if str[index] == '}':
            if not isEnd:
                break
            else:
                isEnd = False # according to the structure of the twitter file, the second to last '}' is valid for the last line
    # print(count)
    # print(str[0:index+1])
    return json.loads(str[0:index+1]) # index+1 so that we include the last '}' in the slice




# main function
# TODO:加入数据集展示功能
if __name__ == '__main__':
    gridLangMap= gridProcessor('data/sydGrid.json')
    mmapTwitterProcessor('data/smallTwitter.json',1,gridLangMap)
    for gridLang in gridLangMap.gridLangList:
        print(gridLang)

