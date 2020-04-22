# WeiboScrapy
A Scrapy to get real-time Weibo data

## Module instruction
* 01_RealTimeWeiboTopicScrapyForServer_v10.py

    The main module to get real-time Weibo data
* 02_dataPreProcess.py:

    As its name, clean the data, but not very clean for this version...
* 03_FrequentPattern_Apriori.py

    Use Apriori algorithm to find the frequent pattern sets and store the FPS in pickle files. It will be very slow if the min support threshold is lower than 0.03...
* 04_PickleUnpack.py
    
    Use the pickle files to create csv files of the FPS and association rules that human can read
* 05_getTrend.py

    Get trend data of the interested keyword thoughout the data period you have collected



Modified based on:
https://github.com/Python3Spiders/WeiboSuperSpider