"""
百度百科个人信息 经历部分信息抽取
author: jtx
"""
import sys, os

sys.path.append('/home/liangzhi/xjt/')

import sys
import os
import re
from bs4 import BeautifulSoup
import logging
import pymongo
import base64
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG2, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
import json
from bs4 import BeautifulSoup

class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'automobile_kb'
        self.proj = proj
        self.host = "baike.baidu.com"  # 网站域名
        self.host2 = "baike.baidu.com"
        self.host_name = "百度百科"  # 网站中文名
        self.api_url = ""  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.mongo_client.admin.authenticate("xxx", "xxx")

        # self.save_coll_name = "res_kb_expert_patent_linshi"  # 需要保存的表名
        # self.mongo_db = self.mongo_client[config["db"]]
        # self.mongo_coll = self.mongo_db[self.save_coll_name]

        config["db1"] = 'yyf_db'
        self.read_col1_name = "res_kb_process_expert_ai_disam"
        self.mongo_read_db1 = self.mongo_client[config["db1"]]
        self.mongo_read_col1 = self.mongo_read_db1[self.read_col1_name]

        config["db2"] = 'yyf_db'
        self.read_col2_name = "res_kb_expert_baike"
        self.mongo_read_db2 = self.mongo_client[config["db2"]]
        self.mongo_read_col2 = self.mongo_read_db2[self.read_col2_name]

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 5
        configure_logging("baike_clean_img.log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
        }
        self.headers2 = {'Host': self.host,
                         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0'}
        self.count = 0
        # 链接mongodb

    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)

    def save_record(self, record, coll_name, pk):

        tmp = []
        self.count += 1
        print('count:', self.count)

        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # print( tmp
        show = "  ".join(tmp)
        r_in_db = coll_name.find_one(pk)
        if not r_in_db:
            coll_name.insert_one(record)
            self.logger.info("成功插入(%s)" % (record['kId']))
        else:
            self.logger.info("重复数据(%s)" % (record['kId']))

        # 此处更新维护表, 包括百度百科flag,是否有结果flag
        # self.update_maintenance_table(flag_aminer=1, flag_aminer_result=1)


    # 更新维护表
    def update_maintenance_table(self, _id, img_url):
        # flag = 1
        myquery = {"_id": _id}
        newvalues = {"$set": {"img_url": img_url,
                              }}
        self.mongo_read_col2.update_one(myquery, newvalues)
        self.logger.info('维护表成功更新, _id:%s' % _id)

    def run(self, start_page=1, max_page=-1, page_size='20', round=1):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================
        # dd = '2020-12-04 19:00:00'
        # dd = datetime.datetime.strptime(dd, "%Y-%m-%d %H:%M:%S")
        # for num, i in enumerate(self.mongo_read_col2.find({'crawl_time': {'$gt': dd}})):
        for num, i in enumerate(self.mongo_read_col2.find()):
            # if num<44000:
            #     continue
            _id = i['_id']
            html = i['html']
            print(str(num), _id)
            soup = BeautifulSoup(html, 'lxml')
            tag_img = soup.find('div', {'class':'summary-pic'})
            if tag_img:
                tag_img_a = tag_img.find('a')
                if tag_img_a:
                    tag_img_a_img = tag_img_a.find('img')['src']
                    img_url = tag_img_a_img
                    self.update_maintenance_table(_id, img_url)
                else:
                    img_url = ''
                    self.update_maintenance_table(_id, img_url)
                    print('没有tag_time_a, 更新img_url为空')

            else:
                img_url = ''
                self.update_maintenance_table(_id, img_url)
                print('没有找到tag_img, 更新img_url为空')





        self.logger.info("Finish Run")


if __name__ == '__main__':
    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1, page_size='10', round=2)
