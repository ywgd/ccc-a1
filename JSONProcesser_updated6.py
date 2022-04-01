import json
import mmap
import os
from enum import Enum
from collections import Counter
# one grid class contains the border of one synGrid
# from main.Util import *


class BorderStatus(Enum):
    center=0
    westBorder=1
    southBorder=2
    southWestBoder=3
    undefined=5

class Grid(object):

    def __init__(self,name,NW,SE):
        self.west=NW[0]
        self.east=SE[0]
        self.north=NW[1]
        self.south=SE[1]
        self.name=name
        self.borderStatus = BorderStatus.undefined

    def setBorderStatus(self,westBorder,southBorder):
        if self.west==westBorder and self.south==southBorder:
            self.borderStatus=BorderStatus.southWestBoder
        elif self.west==westBorder:
            self.borderStatus=BorderStatus.westBorder
        elif self.south==southBorder:
            self.borderStatus=BorderStatus.southBorder
        else:
            self.borderStatus=BorderStatus.center

    def insideGrid(self,loc):
        if self.borderStatus==BorderStatus.center:
            return loc[0]>self.west and loc[0]<=self.east and loc[1]>self.south and loc[1]<=self.north
        elif self.borderStatus==BorderStatus.westBorder:
            return loc[0] >= self.west and loc[0] <= self.east and loc[1] > self.south and loc[1] <= self.north
        elif self.borderStatus==BorderStatus.southBorder:
            return loc[0] > self.west and loc[0] <= self.east and loc[1] >= self.south and loc[1] <= self.north
        elif self.borderStatus==BorderStatus.center:
            return loc[0]>-self.west and loc[0]<=self.east and loc[1]>=self.south and loc[1]<=self.north

    def __str__(self):
        return "[id:%s,W:%s,E:%s,N:%s,S:%s,BorderStatus:%s]"%(self.name,self.west,self.east,self.north,self.south,self.borderStatus)



# Twitter class, contains the language label and location
class Twitter(object):
    def __init__(self,lang,loc):
        self.lang = lang
        self.loc = loc

    def __str__(self):
        return "[lang:%s,loc:%s,%s]"%(self.lang,self.loc[0],self.loc[1])

# GridLand class contains a grid and a language dict for counting the usage of languages
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

    def sortGridLangResult(self):
        return sorted(self.langDict.items(),key= lambda x:x[1],reverse=True)




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

    def addGrid(self,gridLang):
        self.gridLangList.append(gridLang)
        self.totalGrids+=1

    def postGridProcess(self):
        if self.totalGrids!=0:
            self.sortGrid()
            # assume that the grid provided is a square
            self.northBorder = self.gridLangList[0].grid.north
            self.westBorder = self.gridLangList[0].grid.west
            self.southBorder = self.gridLangList[self.totalGrids-1].grid.south
            self.eastBorder = self.gridLangList[self.totalGrids-1].grid.east
            for gridLang in self.gridLangList:
                gridLang.grid.setBorderStatus(self.westBorder,self.southBorder)

    def sortGrid(self):
        self.gridLangList.sort(key=lambda x: (-x.grid.north, x.grid.west))

    def insertTwitter(self,twitter):
        if twitter == None:
            return
        for gridLang in self.gridLangList:
            if gridLang.insertTwitter(twitter):
                self.totalTwitters+=1
                return

    # def sortGridLangResult(self):
    #     for gridLang in gridLangMap.gridLangList:
    #         gridLang.sortGridLangResult()
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
            NW=i['geometry']['coordinates'][0][NW_IDX]
            SE=i['geometry']['coordinates'][0][SE_IDX]
            name = i['properties']['id'] # use the id provided to label the grid
            gridLangMap.addGrid(GridLang(Grid(name,NW,SE)))

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
    if twitterJsonObject == None:
        return None
    lang = twitterJsonObject['doc']['lang']
    posi = twitterJsonObject['doc']['coordinates']
    loc = None
    if posi != None:
        loc = posi['coordinates']
    elif twitterJsonObject['doc']['geo']!=None:
        posi = twitterJsonObject['doc']['geo']
        loc = [posi['coordinates'][1],posi['coordinates'][0]]

    if lang != None and loc != None: # a filter to exclude the twitter without lang or coordinate label
        return Twitter(lang,loc)
    return None

# the main function, read Twitter file with mmap technique
# (deprecated) not suitable for multicore processing
# def mmapTwitterProcessor(twitterFilePath,rankNum,girdLangMap):
#     try:
#         with open(twitterFilePath,'rb') as f:
#             mmp = mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ)
#             header = json.loads(mmp.readline().decode('utf-8')+']}')
#             totalRows = header['total_rows']
#             if header['offset']!=None:
#                 totalRows-=header['offset']
#             count = 1 #skip the first row
#             for line in iter(mmp.readline,b''):
#                 count +=1
#                 isEnd = totalRows==count
#                 if isEnd ==True and twitterFilePath=='bigTwitter.json':
#                     break
#                 jsonObject = jsonLoadProcessor(line,isEnd,count)
#                 # print(jsonObject['doc']['lang'])
#                 # print(jsonObject['doc']['coordinates'])
#                 twitter = twitterJsonObjectProcessor(jsonObject)
#                 if twitter != None:
#                     # print(twitter)
#                     gridLangMap.insertTwitter(twitter)
#     except IOError:
#         print('failed to open:',twitterFilePath)
#     finally:
#         try:
#             if f != None:
#                 f.close()
#         except IOError:
#             print('failed to close:', twitterFilePath)

def mmapTwitterProcessor(twitterFilePath,rank,totalCores,gridLangMap):
    try:
        with open(twitterFilePath,'rb') as f:
            fileSize = os.path.getsize(twitterFilePath)
            blockSize = int(fileSize / totalCores)
            startIdx = rank * blockSize
            endIdx = (rank + 1) * blockSize
            mmp = mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ)
            mmp.seek(startIdx)
            line = mmp.readline()
            # print('discarded', line)
            while mmp.tell() < endIdx:
                line = mmp.readline()
                # print(line)
                jsonObject = jsonLoadProcessor(line)
                twitter = twitterJsonObjectProcessor(jsonObject)
                if twitter != None:
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
def jsonLoadProcessor(byteArray):
    try:
        str = byteArray.decode('utf-8')
        # print(isEnd)
        index = 0
        for index in range(len(str) - 1, 0, -1):# find the last '}' and convert the string into valid json format
            if str[index] == '}' and index-1>0 and str[index-1]!=']':
                break
         # print(count)
        # print(str[0:index+1])
        if index > 0:
            return json.loads(str[0:index+1]) # index+1 so that we include the last '}' in the slice
        else:
            return None
    except json.decoder.JSONDecodeError:
        print('error when decoding: No: ',byteArray)



# print the final result in given format
def printFinalResult(gridLangMap):
    langDict = languageListProcessor()
    gridNameList = ['A1','A2','A3','A4','B1','B2','B3','B4','C1','C2','C3','C4','D1','D2','D3','D4']
    printFormat = '| {0:^5} | {1:^15} | {2:^25} | {3:^150} |'
    print((printFormat).format('Cell','#Total_Tweets','#Number_of_Languages_Used','#Top 10 Languages & #Tweets'))
    for i in range(0,gridLangMap.totalGrids):
        gridLang = gridLangMap.gridLangList[i]
        gridName = gridNameList[i]
        totalTweets = gridLang.totalTweets
        numLang = len(gridLang.langDict)
        #TODO:get the top 10 languages
        top10Language =getTop10Language(gridLang,langDict)
        if top10Language==None:
            top10Language = 'not found in language dict'
        print((printFormat).format(gridName, totalTweets, numLang,
                                                    top10Language))

    # print(("%-4s %-20s %20s %-100s\n"%('A1',11111,11,'(English-9,000, Chinese-555, French-444, …Greek-66)')))
    # for i in range(gridLangMap.totalGrids):

def languageListProcessor(languageFilePath='languageInfo.txt'):
    f = open(languageFilePath,encoding='utf-8')
    # line = f.readline()
    line ='default'
    langDict = {}
    while line != '':
        line = f.readline()
        # print(line)
        str = line.strip().split()
        if len(str)==2:
            langDict[str[1]]=str[0]
        elif len(str)>0:
            langDict[str[len(str)-1]]=''.join(str)
    return langDict

def getTop10Language(gridLang,langDict):
    count = 0
    top10List = gridLang.sortGridLangResult()
    if len(top10List)==0:
        return '()'
    resultStr = '('
    for twins in top10List:
        count+=1
        if count <= 10:
            if count != 1:
                resultStr += ','
            if langDict.__contains__(twins[0]):
                resultStr+=langDict[twins[0]]+'-'+str(twins[1])
            else:
                resultStr += 'unknown_lang:'+ twins[0]  + '-' + str(twins[1])
    resultStr+=')'
    return resultStr


def multiLanguageTest(gridLangMap):
    gridLangMap.gridLangList[5].langDict['en'] = 8
    gridLangMap.gridLangList[5].langDict['ar'] = 10
    gridLangMap.gridLangList[5].langDict['bn'] = 7
    gridLangMap.gridLangList[5].langDict['fi'] = 6
    gridLangMap.gridLangList[5].langDict['fr'] = 8
    gridLangMap.gridLangList[5].langDict['he'] = 10
    gridLangMap.gridLangList[5].langDict['hi'] = 11
    gridLangMap.gridLangList[5].langDict['id'] = 12
    gridLangMap.gridLangList[5].langDict['it'] = 11111
    gridLangMap.gridLangList[5].langDict['ja'] = 28
    gridLangMap.gridLangList[5].langDict['ko'] = 14
    gridLangMap.gridLangList[5].langDict['msa'] = 11
    gridLangMap.gridLangList[5].langDict['no'] = 0
    gridLangMap.gridLangList[5].langDict['pl'] = 0
    gridLangMap.gridLangList[5].langDict['pt'] = 84
    gridLangMap.gridLangList[5].langDict['und'] = 100
    # langDict = languageListProcessor()
    # print(getTop10Language(gridLangMap.gridLangList[5], langDict))



def mergeData(combine_data):
    # print("结果个数：", len(combine_data))

    results = combine_data[0]
    for i in range(1, len(combine_data)):
        for j in range(0, 16):
            results.totalTwitters += combine_data[i].totalTwitters
        results.gridLangList[j].langDict.update(combine_data[i].gridLangList[j].langDict)

    # self.totalGrids 是什么？

    return results
# main function
if __name__ == '__main__':
    gridLangMap0= gridProcessor('sydGrid.json')
    gridLangMap1 = gridProcessor('sydGrid.json')
    gridLangMap2 = gridProcessor('sydGrid.json')
    filePath = 'smallTwitter.json'

    mmapTwitterProcessor(filePath, 0, 3, gridLangMap0)
    mmapTwitterProcessor(filePath, 2, 3, gridLangMap2 )
    mmapTwitterProcessor(filePath, 1, 3, gridLangMap1)

    gridLangMap = mergeData([gridLangMap0,gridLangMap1,gridLangMap2])

    print('#############seperated maps###############')
    printFinalResult(gridLangMap0)
    printFinalResult(gridLangMap1)
    printFinalResult(gridLangMap2)
    print('#############merged map###############')
    printFinalResult(gridLangMap)





