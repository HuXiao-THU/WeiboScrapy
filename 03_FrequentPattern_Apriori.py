# This is used to find the frequent patterns in the Weibo data.
# The input is a directory containing all the Weibo data.
# The output is pickle files for later process

import csv
import os
import re
import jieba
import pickle
import time


input_dir = './Weibo_data/processed_data/pandamic'
keyword = '疫情'    # the keyword of the Weibo
output_dir = './Weibo_data/FrequentPattern'
minSupportThreshold = 0.02

# 读取数据集，后面在apriori()中会处理成集合格式
def loadDataSet(file_name):
    dataSet = []
    with open(file_name, encoding='utf-8-sig') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)    # 获取表头
        for row in csvreader:
            # 获取每一行数据并处理
            line = row[5]
            line = line.strip()  #去前后的空格
            line = re.sub(r"[\s+\.\!\/_,$%^*()?;；:-【】+\"\']+|[+——！，;:。？、~@#￥%……&*（）]+",\
                "", line) #去标点符号
            # 分词
            segments = jieba.lcut(line)
            # 预处理中无法清理干净的词
            for i in range(len(segments)-1, -1, -1):
                if segments[i] in ['原图','组图','的','了','张','地图','显示','也','就','是','还','着'] or segments[i] == keyword:
                    del segments[i]
            dataSet.append(segments)  
    return dataSet      
 
# 将所有元素转换为frozenset型字典，存放到列表中
def createC1(dataSet):
    checkDict = {}  # 用字典加速查询
    C1 = []
    for line in dataSet:
        for word in line:
            if word not in checkDict:
                checkDict[word] = 1
                C1.append([word])
    C1.sort()
    # 使用frozenset是为了后面可以将这些值作为字典的键
    return list(map(frozenset, C1))  # frozenset一种不可变的集合，set可变集合
 
# 过滤掉不符合支持度的集合
# 返回 频繁项集列表retList和所有元素的支持度字典
# D为数据集，Ck为k个元素的Candidate
def scanD(D, Ck, minSupport):
    ssCnt = {}
    for tid in D:
        for can in Ck:
            if can.issubset(tid):   # 判断can是否是tid的《子集》 （这里使用子集的方式来判断两者的关系）
                if can not in ssCnt:    # 统计该值在整个记录中满足子集的次数（以字典的形式记录，frozenset为键）
                    ssCnt[can] = 1
                else:
                    ssCnt[can] += 1
    numItems = float(len(D))
    retList = []        # 重新记录满足条件的数据值（即支持度大于阈值的数据）
    supportData = {}    # 每个数据值的支持度
    for key in ssCnt:
        support = ssCnt[key] / numItems
        if support >= minSupport:
            retList.append(key)
            supportData[key] = support
    return retList, supportData # 排除不符合支持度元素后的元素 每个元素支持度
 
# 生成所有可以组合的元素个数为k的集合
# 频繁项集列表Lk 项集元素个数实际上是k-1
def aprioriGen(Lk, k):
    retList = []
    lenLk = len(Lk)
    for i in range(lenLk): # 两层循环比较Lk中的每个元素与其它元素
        for j in range(i+1, lenLk):
            set1 = Lk[i]
            set2 = Lk[j]
            union = set1 | set2 # 求并集
            if len(list(union)) == k:
                retList.append(Lk[i] | Lk[j])

    retList = list(set(retList))    # 去重  
    return retList  # 返回频繁项集列表Ck
 
# 主算法
# 返回 所有满足大于阈值的组合 集合支持度列表
def apriori(dataSet, minSupport = 0.5):
    D = list(map(set, dataSet)) # 转换列表记录为集合  [{1, 3, 4}, {2, 3, 5}, {1, 2, 3, 5}, {2, 5}]
    C1 = createC1(dataSet)      # 将每个元素转会为frozenset集合
    L1, supportData = scanD(D, C1, minSupport)  # 过滤数据
    print("L 1 finished!")
    L = [L1]
    k = 2
    while (len(L[k-2]) > 0):    # 若仍有满足支持度的集合则继续
        Ck = aprioriGen(L[k-2], k)  # Ck为k个元素的候选频繁项集
        Lk, supK = scanD(D, Ck, minSupport) # Lk频繁项集
        if Lk == []:
            break
        supportData.update(supK)    # 更新字典（把新出现的集合:支持度加入到supportData中）
        L.append(Lk)
        print("L", k, "finished!")
        k += 1  # 每次新组合的元素都只增加了一个，所以k也+1（k表示元素个数）
    return L, supportData

def testDataSet():
    return [[1,3,4],[2,3,5],[1,2,3,5],[2,5]]

def main():
    for root, dirs, files in os.walk(input_dir):
        for _file in files:
            file_name = os.path.join(root, _file)
            # 这里开始对目录下对每个文件进行操作
            dataSet = loadDataSet(file_name)
            print(_file, "loaded!")
            # dataSet = testDataSet()     # for debug
            start = time.time()
            L, supportData = apriori(dataSet, minSupportThreshold)
            end = time.time()
            patterns = [pattern for Lk in L for pattern in Lk] # a list of set

            pickle_name = _file[:-4] + '.pickle'
            pickle_file = os.path.join(output_dir, pickle_name)
            with open(pickle_file, 'wb') as f:
                pickle.dump(patterns, f, pickle.HIGHEST_PROTOCOL)
                pickle.dump(supportData, f, pickle.HIGHEST_PROTOCOL)
            print(pickle_name, "saved!\t", end-start, "sec used")

            del dataSet
            del L
            del supportData
            del patterns

if __name__ == '__main__':
    main()