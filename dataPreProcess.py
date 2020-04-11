import os
import csv

file_dir = './WeiboScrapy/topic/'
output_file = './WeiboScrapy/SampleData/sample_开学.csv'

def write_csv(write_list):
    """将筛选过的信息写入csv文件"""
    try:
        result_headers = [
            '微博ID',
            '用户id',
            '用户昵称',
            '微博链接',
            # 原创
            '内容',
            # 转发理由
            # 原始用户
            '发布位置',
            '发布时间',
            '信息采集时间',
            '关键词',
            '发布工具',
            '转发数',
            '评论数',
            '点赞数',
        ]
        if True:
            result_headers.insert(4, '是否为原创微博')
            result_headers.insert(6, '转发理由')
            result_headers.insert(7, '原始用户')
        result_data = write_list

        fileExist = os.path.exists(output_file)

        with open(output_file, 'a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            if not fileExist:
                writer.writerows([result_headers])
            writer.writerows(result_data)
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()

def main():
    count = 0
    all_count = 0
    count_temp = 0
    all_count_temp = 0

    for root, dirs, files in os.walk(file_dir):
        for _file in files:
            file_name = root + _file
            print(_file)
            buffer_list = []
            with open(file_name, encoding='utf-8-sig') as csvfile:
                csvreader = csv.reader(csvfile)
                header = next(csvreader)    # 获取表头
                count_temp = 0  # 重置数据
                all_count_temp = 0
                for row in csvreader:
                    all_count_temp += 1
                    # 删去非原创内容
                    if row[4] == 'False':
                        continue
                    # 删去内容带【】的官方新闻
                    content = row[5]
                    titlel = content.find('【')
                    titler = content.find('】')
                    if titlel != -1 or titler != -1:
                        continue
                    buffer_list.append(row)
                    count_temp += 1
            write_csv(buffer_list)
            print('effective rate = ', count_temp, '/', all_count_temp)
            count += count_temp
            all_count += all_count_temp

    print("total effective rate: ", count, "/", all_count)

if __name__ == '__main__':
    main()