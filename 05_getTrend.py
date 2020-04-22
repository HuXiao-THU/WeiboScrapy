import pickle
import csv
import os

pickle_dir_school = './Weibo_data/FrequentPattern/school'
pickle_dir_pandemic = './Weibo_data/FrequentPattern/pandemic'

output = './temp.csv'

# set the keyword here
keyword = frozenset([ "我爸" ])

def frequencyTrend(key, mydir):
    """
    This function use pickle_dir
    """
    kiss = '|'.join(key)
    trend = [ kiss ]
    frequency = 0

    for root, dirs, files in os.walk(mydir):
        for _file in files:
            file_name = os.path.join(root, _file)
            with open(file_name, 'rb') as f:
                patterns = pickle.load(f)
                supportData = pickle.load(f)
            frequency = 0
            if key in supportData.keys():
                frequency = supportData[key]
            
            # 如果是0，代表不存在或小于min support threshold
            # 这里打印是为了确认顺序正确！！！务必注意确认！！！
            print(_file, "frequence is ", frequency)
            trend.append(frequency)

    with open(output, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(trend)
    print(output, "saved!")

if __name__ == '__main__':
    frequencyTrend(keyword, pickle_dir_school)
