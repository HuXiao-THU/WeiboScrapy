import pickle
import os
import csv

input_dir = './Weibo_data/FrequentPattern'
fp_dir = './Weibo_data/Fp'
output_dir = './Weibo_data/Rules'
min_confidence = 0.25


def generateRules(frequent_set, rules):
    for frequent_item in frequent_set:
        if len(frequent_item) > 1:
            getRules(frequent_item, frequent_set, rules)

def removeItem(current_item, item):
    tempSet = []
    for elem in current_item:
        if elem != item:
            tempSet.append(elem)
    tempFrozenSet = frozenset(tempSet)
    return tempFrozenSet

def getRules(frequent_item, frequent_set, rules):
    for item1 in frequent_item:
        for item2 in frequent_item:
            if item1 != item2:
                if frozenset((item1,item2)) in frequent_set.keys():
                    # input("keyi ")
                    confidence = frequent_set[frozenset((item1,item2))]/frequent_set[frozenset([item1])]
                else:
                    continue
        
                if confidence >= min_confidence:
                    flag = False
                    for rule in rules:
                        if (rule[0] == item1) and (rule[1] == item2):
                            flag = True

                    if flag == False:
                        rules.append([item1, item2, confidence])
                        print(item1, item2, confidence)

def main():
    for root, dirs, files in os.walk(input_dir):
        for _file in files:
            file_name = os.path.join(root, _file)
            with open(file_name, 'rb') as f:
                patterns = pickle.load(f)
                supportData = pickle.load(f)
            print(_file, "load succeed!")

            # write out the frequent pattern data
            frequent_data = [["support","num_words","pattern"]]
            for fre_item, sup in supportData.items():
                row = [sup, len(fre_item), tuple(fre_item)]
                frequent_data.append(row)

            csv_name = 'FP' + _file[5:-7] + '.csv'
            csv_file = os.path.join(fp_dir, csv_name)
            with open(csv_file, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(frequent_data)
            print(csv_name, "saved!")

            # write out the rules data
            frequent_set = supportData
            rules = []
            generateRules(frequent_set, rules)

            csv_name = 'Rule' + _file[5:-7] + '.csv'
            csv_file = os.path.join(output_dir, csv_name)
            with open(csv_file, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows([["word1","word2","confidence"]])
                writer.writerows(rules)
            print(csv_name, "saved!")

            del frequent_data
            del patterns
            del supportData
            del frequent_set
            del rules


if __name__ == '__main__':
    main()
