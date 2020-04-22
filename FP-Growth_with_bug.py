import os
import csv
import re
import jieba
import pickle
import time

min_support_threshold = 0.02

min_confidence = 0.5

input_dir = './Weibo_data/processed_data/test'
# keyword = '自行车'
# input_dir = './Weibo_data/processed_data/pandamic2'
# keyword = '疫情'
# input_dir = './Weibo_data/processed_data/school'
# keyword = '开学'
output_dir = './Weibo_data/FrequentPattern'

class FPNode:
    def __init__(self, item, count, parent):
        self.item = item
        self.count = count              # support
        self.parent = parent
        self.next = None               # the same elements
        self.children = {}

    def display(self, ind=1):
        print(''*ind, self.item, '', self.count)
        for child in self.children.values():
            child.display(ind+1)

class FPgrowth:
    def __init__(self, min_support=3, min_confidence=0.6):
        self.min_support = min_support
        self.min_confidence = min_confidence

    '''
    Function:  transfer2FrozenDataSet
    Description: transfer data to frozenset type
    Input:  data              dataType: ndarray     description: train_data
    Output: frozen_data       dataType: frozenset   description: train_data in frozenset type
    '''
    def transfer2FrozenDataSet(self, data):
        frozen_data = {}
        for elem in data:
            frozen_data[frozenset(elem)] = 1
        return frozen_data

    '''
      Function:  updataTree
      Description: updata FP tree
      Input:  data              dataType: ndarray     description: ordered frequent items
              FP_tree           dataType: FPNode      description: FP tree
              header            dataType: dict        description: header pointer table
              count             dataType: count       description: the number of a record 
    '''
    def updataTree(self, data, FP_tree, header, count):
        frequent_item = data[0]
        if frequent_item in FP_tree.children:
            FP_tree.children[frequent_item].count += count
        else:
            FP_tree.children[frequent_item] = FPNode(frequent_item, count, FP_tree)
            if header[frequent_item][1] is None:
                header[frequent_item][1] = FP_tree.children[frequent_item]
            else:
                self.updateHeader(header[frequent_item][1], FP_tree.children[frequent_item]) # share the same path

        if len(data) > 1:
            self.updataTree(data[1::], FP_tree.children[frequent_item], header, count)  # recurrently update FP tree

    '''
      Function: updateHeader
      Description: update header, add tail_node to the current last node of frequent_item
      Input:  head_node           dataType: FPNode     description: first node in header
              tail_node           dataType: FPNode     description: node need to be added
    '''
    def updateHeader(self, head_node, tail_node):
        while head_node.next is not None:
            head_node = head_node.next
        head_node.next = tail_node

    '''
      Function:  createFPTree
      Description: create FP tree
      Input:  train_data        dataType: ndarray     description: features
      Output: FP_tree           dataType: FPNode      description: FP tree
              header            dataType: dict        description: header pointer table
    '''
    def createFPTree(self, train_data):
        initial_header = {}
        # 1. the first scan, get singleton set
        for record in train_data:
            for item in record:
                initial_header[item] = initial_header.get(item, 0) + train_data[record]

        # get singleton set whose support is large than min_support. If there is no set meeting the condition,  return none
        header = {}
        for k in initial_header.keys():
            if initial_header[k] >= self.min_support:
                header[k] = initial_header[k]
        frequent_set = set(header.keys())
        if len(frequent_set) == 0:
            return None, None

        # enlarge the value, add a pointer
        for k in header:
            header[k] = [header[k], None]

        # 2. the second scan, create FP tree
        FP_tree = FPNode('root', 1, None)        # root node
        for record, count in train_data.items():
            frequent_item = {}
            for item in record:                # if item is a frequent set， add it
                if item in frequent_set:       # 2.1 filter infrequent_item
                    frequent_item[item] = header[item][0]

            if len(frequent_item) > 0:
                ordered_frequent_item = [val[0] for val in sorted(frequent_item.items(), key=lambda val:val[1], reverse=True)]  # 2.1 sort all the elements in descending order according to count
                self.updataTree(ordered_frequent_item, FP_tree, header, count) # 2.2 insert frequent_item in FP-Tree， share the path with the same prefix

        return FP_tree, header

    '''
      Function: ascendTree
      Description: ascend tree from leaf node to root node according to path
      Input:  node           dataType: FPNode     description: leaf node
      Output: prefix_path    dataType: list       description: prefix path
              
    '''
    def ascendTree(self, node):
        prefix_path = []
        while node.parent != None and node.parent.item != 'root':
            node = node.parent
            prefix_path.append(node.item)
        return prefix_path

    '''
    Function: getPrefixPath
    Description: get prefix path
    Input:  base          dataType: FPNode     description: pattern base
            header        dataType: dict       description: header
    Output: prefix_path   dataType: dict       description: prefix_path
    '''
    def getPrefixPath(self, base, header):
        prefix_path = {}
        start_node = header[base][1]
        prefixs = self.ascendTree(start_node)
        if len(prefixs) != 0:
            prefix_path[frozenset(prefixs)] = start_node.count

        while start_node.next is not None:
            start_node = start_node.next
            prefixs = self.ascendTree(start_node)
            if len(prefixs) != 0:
                prefix_path[frozenset(prefixs)] = start_node.count
        return prefix_path

    '''
    Function: findFrequentItem
    Description: find frequent item
    Input:  header               dataType: dict       description: header [name : (count, pointer)]
            prefix               dataType: dict       description: prefix path
            frequent_set         dataType: set        description: frequent set
    '''
    def findFrequentItem(self, header, prefix, frequent_set):
        # for each item in header, then iterate until there is only one element in conditional fptree
        header_items = [val[0] for val in sorted(header.items(), key=lambda val: val[1][0])]
        if len(header_items) == 0:
            return

        for base in header_items:
            new_prefix = prefix.copy()
            new_prefix.add(base)
            support = header[base][0]
            frequent_set[frozenset(new_prefix)] = support

            prefix_path = self.getPrefixPath(base, header)
            if len(prefix_path) != 0:
                conditonal_tree, conditional_header = self.createFPTree(prefix_path)
                if conditional_header is not None:
                    self.findFrequentItem(conditional_header, new_prefix, frequent_set)

    '''
     Function:  generateRules
     Description: generate association rules
     Input:  frequent_set       dataType: set         description:  current frequent item
             rule               dataType: dict        description:  an item in current frequent item
     '''
    def generateRules(self, frequent_set, rules):
        for frequent_item in frequent_set:
            if len(frequent_item) > 1:
                self.getRules(frequent_item, frequent_item, frequent_set, rules)

    '''
     Function:  removeItem
     Description: remove item
     Input:  current_item       dataType: set         description:  one record of frequent_set
             item               dataType: dict        description:  support_degree 
     '''
    def removeItem(self, current_item, item):
        tempSet = []
        for elem in current_item:
            if elem != item:
                tempSet.append(elem)
        tempFrozenSet = frozenset(tempSet)
        return tempFrozenSet

    '''
     Function:  getRules
     Description: get association rules
     Input:  frequent_set       dataType: set         description:  one record of frequent_set
             rule               dataType: dict        description:  support_degree 
     '''
    def getRules(self, frequent_item, current_item, frequent_set, rules):
        for item in current_item:
            try:
                subset = self.removeItem(current_item, item)
                confidence = frequent_set[frequent_item]/frequent_set[subset]
            except Exception as e:
                print("error:", e)
                print("fre_item:")
                print(frequent_item in frequent_set)
                print("subset:")
                print(subset in frequent_set)
                input("###########")
            if confidence >= self.min_confidence:
                flag = False
                for rule in rules:
                    if (rule[0] == subset) and (rule[1] == frequent_item - subset):
                        flag = True

                if flag == False:
                    rules.append((subset, frequent_item - subset, confidence))

                if (len(subset) >= 2):
                    self.getRules(frequent_item, subset, frequent_set, rules)

    '''
      Function:  train
      Description: train the model
      Input:  train_data       dataType: ndarray   description: items
              display          dataType: bool      description: print the rules
      Output: rules            dataType: list      description: the learned rules
              frequent_items   dataType: list      description: frequent items set
    '''
    def train(self, data, display=True):
        data = self.transfer2FrozenDataSet(data)
        FP_tree, header = self.createFPTree(data)
        print("FP-tree built!")
        #FP_tree.display()
        frequent_set = {}
        prefix_path = set([])
        self.findFrequentItem(header, prefix_path, frequent_set)
        print("FrequentItems found!")
        # for item in frequent_set:
        #     print(item)
        # input("#####################")
        rules = []
        self.generateRules(frequent_set, rules)
        print("Rules found!")

        if display:
            print("Frequent Items:")
            print(frequent_set)
            # for item in frequent_set:
            #     print(item)
            print("_______________________________________")
            print("Association Rules:")
            print(rules)
            # for rule in rules:
            #     print(rule)
        return frequent_set, rules

# load dateset as a list of list
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
                if segments[i] in ['原图','组图','的','了','张','地图','显示','也','就','是','还','着','被','都','在']:
                    del segments[i]
            segments = list(set(segments))
            dataSet.append(segments)  
    return dataSet

def main():
    for root, dirs, files in os.walk(input_dir):
        for _file in files:
            file_name = os.path.join(root, _file)

            # 这里开始对目录下对每个文件进行操作
            dataSet = loadDataSet(file_name)
            print(_file, "loaded!")
            # dataSet = testDataSet()     # for debug

            # 初始化
            num_items = len(dataSet)
            min_support = int(num_items * min_support_threshold)
            FP = FPgrowth(min_support, min_confidence)

            # 入口
            start = time.time()
            frequent_set, rules = FP.train(dataSet)
            end = time.time()
            
            print("time used:", end-start, "sec")
            

if __name__ == '__main__':
    main()
    
