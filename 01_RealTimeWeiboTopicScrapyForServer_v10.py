import requests

from lxml import etree
from collections import OrderedDict

from urllib.parse import quote

import csv

import traceback

import random

import re

from time import sleep
from time import strftime
from time import localtime

import queue

import os
import getopt

from datetime import datetime, timedelta
import sys
from threading import Thread

# header information
Cookie = '' # paste your cookie here
User_Agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0'

class WeiboTopicScrapy(Thread):

    def __init__(self,keyword,filter,start_time,end_time,sleep_time):
        Thread.__init__(self)
        self.headers={
            'Cookie':Cookie,
            'User_Agent':User_Agent
        }
        self.keyword = keyword
        self.filter = filter # 1: 原创微博； 0：所有微博
        self.start_time = start_time
        self.end_time = end_time
        self.sleep_time = sleep_time
        self.got_num = 0  # 爬取到的微博数
        self.weibo = []  # 存储爬取到的所有微博信息
        self.date_check = {}    # 记录爬取日期
        self.check_queue = queue.Queue()
        self.repeat_check = {}  # 避免爬取重复内容
        if not os.path.exists('topic'):
            os.mkdir('topic')
        self.start()

    def deal_html(self,url):
        """处理html"""
        try:
            html = requests.get(url, headers=self.headers).content
            selector = etree.HTML(html)
            return selector
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def deal_garbled(self,info):
        """处理乱码"""
        try:
            info = (info.xpath('string(.)').replace(u'\u200b', '').encode(
                sys.stdout.encoding, 'ignore').decode(sys.stdout.encoding))
            return info
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_long_weibo(self,weibo_link):
        """获取长原创微博"""
        try:
            selector = self.deal_html(weibo_link)
            info = selector.xpath("//div[@class='c']")[1]
            wb_content = self.deal_garbled(info)
            wb_time = info.xpath("//span[@class='ct']/text()")[0]
            weibo_content = wb_content[wb_content.find(':') +
                                       1:wb_content.rfind(wb_time)]
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_original_weibo(self,info, weibo_id):
        """获取原创微博"""
        try:
            weibo_content = self.deal_garbled(info)
            weibo_content = weibo_content[:weibo_content.rfind(u'赞')]
            a_text = info.xpath('div//a/text()')
            if u'全文' in a_text:
                weibo_link = 'https://weibo.cn/comment/' + weibo_id
                wb_content = self.get_long_weibo(weibo_link)
                if wb_content:
                    weibo_content = wb_content
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_long_retweet(self,weibo_link):
        """获取长转发微博"""
        try:
            wb_content = self.get_long_weibo(weibo_link)
            weibo_content = wb_content[:wb_content.rfind('原文转发')]
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_retweet(self,info, weibo_id):
        """获取转发微博"""
        try:
            original_user = info.xpath("div/span[@class='cmt']/a/text()")
            if not original_user:
                wb_content = '转发微博已被删除'
                return wb_content
            else:
                original_user = original_user[0]
            wb_content = self.deal_garbled(info)
            wb_content = wb_content[wb_content.find(':') +
                                    1:wb_content.rfind('赞')]
            wb_content = wb_content[:wb_content.rfind('赞')]
            a_text = info.xpath('div//a/text()')
            if '全文' in a_text:
                weibo_link = 'https://weibo.cn/comment/' + weibo_id
                weibo_content = self.get_long_retweet(weibo_link)
                if weibo_content:
                    wb_content = weibo_content
            retweet_reason = self.deal_garbled(info.xpath('div')[-1])
            retweet_reason = retweet_reason[5:retweet_reason.rindex('赞')]
            wb_content = [retweet_reason, original_user, wb_content]
            return wb_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_content(self,info, is_original):
        """获取微博内容"""
        try:
            weibo_id = info.xpath('@id')[0][2:]
            if is_original:
                content = self.get_original_weibo(info, weibo_id)
                # print(content)
                weibo_content = ['/','/',content]
            else:
                weibo_content = self.get_retweet(info, weibo_id)
                # print('转发理由： ' + weibo_content[0] + '\n' + '原始用户: ' + weibo_content[1] +
                        #   '\n' + '转发内容: ' + weibo_content[2])
            return weibo_content
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_place(self,info):
        """获取微博发布位置"""
        try:
            div_first = info.xpath('div')[0]
            a_list = div_first.xpath('a')
            publish_place = '无'
            for a in a_list:
                if ('place.weibo.com' in a.xpath('@href')[0]
                        and a.xpath('text()')[0] == '显示地图'):
                    weibo_a = div_first.xpath("span[@class='ctt']/a")
                    if len(weibo_a) >= 1:
                        publish_place = weibo_a[-1]
                        if ('视频' == div_first.xpath(
                                "span[@class='ctt']/a/text()")[-1][-2:]):
                            if len(weibo_a) >= 2:
                                publish_place = weibo_a[-2]
                            else:
                                publish_place = '无'
                        publish_place = self.deal_garbled(publish_place)
                        break
            # print('微博发布位置: ' + publish_place)
            return publish_place
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_time(self,info):
        """获取微博发布时间"""
        try:
            str_time = info.xpath("div/span[@class='ct']")
            str_time = self.deal_garbled(str_time[0])
            publish_time = str_time.split('来自')[0]
            if '刚刚' in publish_time:
                publish_time = datetime.now().strftime('%Y-%m-%d %H:%M')
            elif '分钟' in publish_time:
                minute = publish_time[:publish_time.find('分钟')]
                minute = timedelta(minutes=int(minute))
                publish_time = (datetime.now() -
                                minute).strftime('%Y-%m-%d %H:%M')
            elif '今天' in publish_time:
                today = datetime.now().strftime('%Y-%m-%d')
                time = publish_time[3:]
                publish_time = today + ' ' + time
            elif '月' in publish_time:
                year = datetime.now().strftime('%Y')
                month = publish_time[0:2]
                day = publish_time[3:5]
                time = publish_time[7:12]
                publish_time = year + '-' + month + '-' + day + ' ' + time
            else:
                publish_time = publish_time[:16]
            # print('微博发布时间: ' + publish_time)
            return publish_time
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_publish_tool(self,info):
        """获取微博发布工具"""
        try:
            str_time = info.xpath("div/span[@class='ct']")
            str_time = self.deal_garbled(str_time[0])
            if len(str_time.split('来自')) > 1:
                publish_tool = str_time.split(u'来自')[1]
            else:
                publish_tool = '无'
            # print('微博发布工具: ' + publish_tool)
            return publish_tool
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_footer(self,info):
        """获取微博点赞数、转发数、评论数"""
        try:
            footer = {}
            pattern = r'\d+'
            str_footer = info.xpath('div')[-1]
            str_footer = self.deal_garbled(str_footer)
            str_footer = str_footer[str_footer.rfind('赞'):]
            weibo_footer = re.findall(pattern, str_footer, re.M)

            up_num = int(weibo_footer[0])
            # print('点赞数: ' + str(up_num))
            footer['up_num'] = up_num

            retweet_num = int(weibo_footer[1])
            # print('转发数: ' + str(retweet_num))
            footer['retweet_num'] = retweet_num

            comment_num = int(weibo_footer[2])
            # print('评论数: ' + str(comment_num))
            footer['comment_num'] = comment_num
            return footer
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def extract_picture_urls(self,info, weibo_id):
        """提取微博原始图片url"""
        try:
            a_list = info.xpath('div/a/@href')
            first_pic = 'https://weibo.cn/mblog/pic/' + weibo_id + '?rl=0'
            all_pic = 'https://weibo.cn/mblog/picAll/' + weibo_id + '?rl=1'
            if first_pic in a_list:
                if all_pic in a_list:
                    selector = self.deal_html(all_pic)
                    preview_picture_list = selector.xpath('//img/@src')
                    picture_list = [
                        p.replace('/thumb180/', '/large/')
                        for p in preview_picture_list
                    ]
                    picture_urls = ','.join(picture_list)
                else:
                    if info.xpath('.//img/@src'):
                        preview_picture = info.xpath('.//img/@src')[-1]
                        picture_urls = preview_picture.replace(
                            '/wap180/', '/large/')
                    else:
                        sys.exit(
                            "爬虫微博可能被设置成了'不显示图片'，请前往"
                            "'https://weibo.cn/account/customize/pic'，修改为'显示'"
                        )
            else:
                picture_urls = '无'
            return picture_urls
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_picture_urls(self,info, is_original):
        """获取微博原始图片url"""
        try:
            weibo_id = info.xpath('@id')[0][2:]
            picture_urls = {}
            if is_original:
                original_pictures = self.extract_picture_urls(info, weibo_id)
                picture_urls['original_pictures'] = original_pictures
                if not self.filter:
                    picture_urls['retweet_pictures'] = '无'
            else:
                retweet_url = info.xpath("div/a[@class='cc']/@href")[0]
                retweet_id = retweet_url.split('/')[-1].split('?')[0]
                retweet_pictures = self.extract_picture_urls(info, retweet_id)
                picture_urls['retweet_pictures'] = retweet_pictures
                a_list = info.xpath('div[last()]/a/@href')
                original_picture = '无'
                for a in a_list:
                    if a.endswith(('.gif', '.jpeg', '.jpg', '.png')):
                        original_picture = a
                        break
                picture_urls['original_pictures'] = original_picture
            return picture_urls
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_user_id(self, info):
        """获取发布者的id"""
        try:
            str_footer = info.xpath('div')[-1]
            comment = str_footer.xpath('a')[-2]
            comment_link = comment.xpath('@href')[0]
            p1 = comment_link.find("uid=")
            p2 = comment_link.find("&rl", p1)
            user_id = comment_link[p1 + 4:p2]
            return user_id
        
        except Exception as e:
            print('Error: ', e)

    def get_one_weibo(self,info):
        """获取一条微博的全部信息"""
        try:
            weibo = OrderedDict()
            is_original = False if len(info.xpath("div/span[@class='cmt']")) > 3 else True
            if (not self.filter) or is_original:
                weibo['id'] = info.xpath('@id')[0][2:]
                weibo['publisher_id'] = self.get_user_id(info)
                weibo['publisher'] = info.xpath('div/a/text()')[0]
                weibo['link'] = 'weibo.cn/' + str(weibo['publisher_id']) + '/' + weibo['id']    # 生成一个wap版的链接
                content = self.get_weibo_content(info, is_original)
                if not self.filter:
                    weibo['original'] = is_original  # 是否原创微博
                weibo['content'] = content[2]   # 微博正文
                if is_original:
                    weibo['content'] = content[2][len(weibo['publisher']) + 1:]  #原创微博的话删掉作者名
                if not self.filter:
                    weibo['forward_reason'] = content[0]
                    weibo['original_user'] = content[1]
                weibo['publish_place'] = self.get_publish_place(info)  # 微博发布位置
                weibo['publish_time'] = self.get_publish_time(info)  # 微博发布时间
                weibo['sample_time'] = strftime("%Y/%m/%d %H:%M:%S", localtime())  # 信息采集时间
                weibo['keyword'] = self.keyword # 关键词
                weibo['publish_tool'] = self.get_publish_tool(info)  # 微博发布工具
                footer = self.get_weibo_footer(info)
                weibo['retweet_num'] = footer['retweet_num']  # 转发数
                weibo['comment_num'] = footer['comment_num']  # 评论数
                weibo['up_num'] = footer['up_num']  # 微博点赞数
            else:
                weibo = None
            return weibo
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def write_csv(self):
        """将爬取的信息写入csv文件"""
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
            if not self.filter:
                result_headers.insert(4, '是否为原创微博')
                result_headers.insert(6, '转发理由')
                result_headers.insert(7, '原始用户')
            result_data = [w.values() for w in self.weibo]

            fileExist = os.path.exists('topic/' + self.keyword + '_' + strftime("%Y_%m_%d") + '.csv')

            with open('topic/' + self.keyword + '_' + strftime("%Y_%m_%d") + '.csv', 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                if not fileExist:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()


    def run(self):
        while True:
            sys.stdout.flush()
            sys.stderr.flush()
            if strftime("%Y_%m_%d") not in self.date_check.keys():
                self.got_num = 0  # 爬取到的微博数
                self.date_check[strftime("%Y_%m_%d")] = True
            self.weibo = []  # 清空缓存
            
            # 可在此修改为多页，实时爬取则建议取page = 1
            page = 1
            Referer = 'https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}&page={}'.format(quote(self.keyword),\
                page - 1)
            headers = {
                'Cookie': Cookie,
                'User-Agent': User_Agent,
                'Referer': Referer
            }
            params = {
                'hideSearchFrame': '',
                'keyword': self.keyword,
                'advancedfilter': '1',
                'starttime': self.start_time,
                'endtime': self.end_time,
                'sort': 'time',
                'page': page
            }

            res = requests.get(url='https://weibo.cn/search/mblog', params=params, headers=headers)
            html = etree.HTML(res.text.encode('utf-8'))

            try:
                weibos = html.xpath("//div[@class='c' and @id]")

                for i in range(0, len(weibos)):

                    aweibo = self.get_one_weibo(info=weibos[i])
                    if aweibo:
                        wid = aweibo['id']
                        cont = aweibo['content']
                        if len(cont) > 500 or len(cont) < 5:
                            continue
                        if wid not in self.repeat_check.keys():
                            self.repeat_check[wid] = True
                            self.check_queue.put(wid)
                        else:
                            break
                        self.weibo.append(aweibo)
                        self.got_num += 1
                        
                        if self.check_queue.qsize() > 200:
                            wid = self.check_queue.get()
                            self.repeat_check.pop(wid) 

                        # print('-' * 100)

                self.write_csv()

                print(str(self.got_num) +" "+ self.keyword + " entries collected today " + strftime("%Y/%m/%d %H:%M:%S", localtime()))
                sleep(self.sleep_time + random.randint(0, 10))
                # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。
            except Exception as e:
                print('Error: ', e)
                traceback.print_exc()

def main(argv):
    # original = 0 爬取所有微博，1 爬取原创微博
    original = 0
    keyword = '疫情'
    sleep_time = 20
    try:
        opts, args = getopt.getopt(argv[1:], 'ok:s:', ['original', 'keyword=', 'sleep='])
    except getopt.GetoptError:
        usage()
        sys.exit()
    
    for opt, arg in opts:
        if opt in ['-o', '--original']:
            original = 1
        elif opt in ['-k', '--keyword']:
            keyword = arg
        elif opt in ['-s', '--sleep']:
            sleep_time = int(arg)
        else:
            print("Error: invalid parameters")
            usage()
            sys.exit()

    WeiboTopicScrapy(keyword=keyword,filter=original,start_time='',end_time='',sleep_time=sleep_time)

if __name__ == '__main__':
    main(sys.argv)
