"""
百度百科个人信息采集
author: jtx
"""
import sys,os
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



class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'industry_ic'
        self.proj = proj
        self.host = "baike.baidu.com"  # 网站域名
        self.host2 = "baike.baidu.com"
        self.host_name = "百度百科"  # 网站中文名
        self.api_url = ""  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.mongo_client.admin.authenticate("xxxx", "xxxx")
        self.save_coll_name = "xxx"  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]
       
        self.mongo_coll = self.mongo_db[self.save_coll_name]

        config["db1"] = 'res_kb'
        self.read_col1_name = "res_kb_expert_ckcest_baike"
        self.mongo_read_db1 = self.mongo_client[config["db1"]]
       
        self.mongo_read_col1 = self.mongo_read_db1[self.read_col1_name]

        # config["db2"] = 'res_kb'
        # self.read_col2_name = "res_kb_expert_relation"
        # self.mongo_read_db2 = self.mongo_client[config["db2"]]
        
        # self.mongo_read_col2 = self.mongo_read_db2[self.read_col2_name]

        # config["db3"] = 'res_kb'
        # self.read_col3_name = "industry_keyword_expert_name"
        # self.mongo_read_db3 = self.mongo_client[config["db3"]]
        
        # self.mongo_read_col3 = self.mongo_read_db3[self.read_col3_name]
        #
        # config["db4"] = 'res_kb'
        # self.read_col4_name = "aminer_maintenance_table"
        # self.mongo_read_db4 = self.mongo_client[config["db4"]]
       
        # self.mongo_read_col4 = self.mongo_read_db4[self.read_col4_name]

        self.start_down_time = datetime.datetime.now()
        self.down_retry = 5
        configure_logging("BAIDUBAIKE_basic.log")  # 日志文件名
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

    def save_record(self, record, coll_name, pk, search_key, research_institution):

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
            self.logger.info("成功插入(%s)" % (record['expert_name']))
        else:
            self.logger.info("重复数据(%s)" % (record['expert_name']))

        # 此处更新维护表, 包括百度百科flag,是否有结果flag
        self.update_maintenance_table(flag_aminer=1, flag_aminer_result=1, search_key=search_key, research_institution=research_institution)

    def url_list_page(self, content_page_list, research_institution_en, research_institution, url_list_page):
        soup = BeautifulSoup(content_page_list, 'lxml')
        url = ''
        flag_page = ''

        soup_content = soup.get_text().strip()
        # 判断页面类型
        if '这是一个多义词，请在下列义项上选择浏览' in soup_content:
            flag_page = '多选'
        elif 'lemma-summary' in content_page_list:
            flag_page = '唯一'
            return url_list_page
        elif '百度百科尚未收录词条' in soup_content:
            flag_page = '未找到'
            return url

        research_institution_list = []
        research_institution_list.append(research_institution)
        # 对机构进行切分，如果有多个机构的话
        if flag_page == '多选':
            if '；' in research_institution:
                research_institution_list = research_institution.split('；')
            for research_institution in research_institution_list:
                tag_ul = soup.find('ul', {'class': 'custom_dot para-list list-paddingleft-1'})
                if tag_ul:
                    tag_ul_para_list = tag_ul.find_all('div', {'class': 'para'})
                    if tag_ul_para_list:
                        for each_tag in tag_ul_para_list:
                            each_tag_text = each_tag.get_text().strip()
                            print(each_tag_text)
                            if research_institution in each_tag_text:
                                try:
                                    url = 'https://baike.baidu.com' + each_tag.find('a')['href']
                                except:
                                    url = ''
                                break
                            else:
                                url = ''
                if url:
                    break
            
        print('url:', url)
        return url

    def url_detail_page(self, content_detail_page, url_detail_page, expert_name):
        record = {}
        record['url'] = url_detail_page
        record['html'] = content_detail_page
        resume = ''

        soup = BeautifulSoup(content_detail_page, 'lxml')
        tag_resume = soup.find('div', {'class':'lemma-summary'})
        if tag_resume:
            resume = tag_resume.get_text().strip()

        expert_name_en = ''
        nation = ''
        birthplace = ''
        birthday = ''
        university = ''
        job = ''
        achievement = ''
        gender = ''
        another_name = ''
        title = ''
        native_place = ''
        sub_nation = ''
        dict_tag_result = {}
        img_url = ''

        tag_img = soup.find('div', {'class': 'summary-pic'})
        if tag_img:
            tag_img_a = tag_img.find('a')
            if tag_img_a:
                tag_img_a_img = tag_img_a.find('img')['src']
                img_url = tag_img_a_img


        tag_basic = soup.find('div', {'class':'basic-info cmn-clearfix'})
        if tag_basic:
            dt_list = tag_basic.find_all('dt')
            dd_list = tag_basic.find_all('dd')
            for num, each in enumerate(dt_list):
                each_text = each.get_text().strip()
                print(each_text)
                if each_text == '英文名':
                    expert_name_en = dd_list[num].get_text().strip()
                elif each_text == '国    籍':
                    nation = dd_list[num].get_text().strip()
                elif each_text == '出生地':
                    birthplace = dd_list[num].get_text().strip()
                elif each_text == '出生日期':
                    birthday = dd_list[num].get_text().strip()
                elif each_text == '毕业院校':
                    university = dd_list[num].get_text().strip()
                elif each_text == '职    业':
                    job = dd_list[num].get_text().strip()
                elif each_text == '主要成就':
                    achievement = dd_list[num].get_text().strip()
                elif each_text == '性    别':
                    gender = dd_list[num].get_text().strip()
                elif each_text == '别    名':
                    another_name = dd_list[num].get_text().strip()
                elif each_text == '职    称':
                    title = dd_list[num].get_text().strip()
                elif each_text == '籍    贯':
                    native_place = dd_list[num].get_text().strip()
                elif each_text == '民    族':
                    sub_nation = dd_list[num].get_text().strip()

        # 给每个小板块加入list
        num_judge_list = []
        num_judge_list_all_div = []
        tag_main = soup.find('div', {'class':'main-content'})
        if tag_main:
            tag_each_div_list = tag_main.find_all('div')
            
            for num, each_div in enumerate(tag_each_div_list):
                div_record = {}
                num_judge_list_all_div.append(each_div.get_text().strip().replace('\n编辑', ''))
                if 'para-title level-2' in str(each_div):
                    div_text = each_div.get_text().strip()
                    div_record['level2'] = div_text
                    div_record['num'] = num
                    num_judge_list.append(div_record)

                elif 'para-title level-3' in str(each_div):
                    div_text = each_div.get_text()
                    div_record['level3'] = div_text
                    div_record['num'] = num
                    num_judge_list.append(div_record)


            if num_judge_list:
                # 抽取每个板块的内容和子内容处理
                dict_tag_cleaning = {}
                list_level2 = []
                list_level3 = []
                flag = 2
                if num_judge_list:
                    for each in num_judge_list:
                        # 名字
                        each_level2 = each.get('level2', '')
                        each_level3 = each.get('level3', '')
                        num = each['num']
                        if each_level2:
                            list_level2.append(num)
                        if each_level3:
                            list_level3.append(num)

                # 加入最后一项到list_level2

                end_add = len(num_judge_list_all_div)-1
                print(end_add)
                list_level2.append(end_add)

                print(list_level2)
                print(list_level3)

                flag_label = 0
                # 把每个三级标签加入二级标签中

                dict_each_region = []
                list_need_add = []
                flag_creat_a_new_list = 0
                flag_nedd_new_list = 0
                length_list_level2 = len(list_level2)
                flag = 0
                list_level2_copy = list_level2.copy()
                for i in range(length_list_level2-1):
                    if type(list_level2[i]) == list:
                        continue
                    for num3, i3 in enumerate(list_level3):
                        if flag == 0:
                            if list_level2[i] < i3 < list_level2[i+1]:
                                list_need_add.append(i3)

                                flag = 1
                            else:
                                pass
                        else:
                            if list_level2[i] < i3 < list_level2[i+1]:
                                list_need_add.append(i3)
                                flag = 1
                                if num3 + 1 < len(list_level3):
                                    pass
                                else:
                                    if flag == 1:
                                        # list2_level_copy中插入新的add_list
                                        insert_index = list_level2_copy.index(list_level2[i])
                                        list_level2_copy.insert(insert_index + 1, list_need_add)
                                        list_level3 = list(set(list_level3).difference(set(list_need_add)))
                                        list_level3.sort()
                                        print(list_level3)
                                        list_need_add = []
                                        flag = 0
                                        break
                            else:
                                # list2_level_copy中插入新的add_list
                                insert_index = list_level2_copy.index(list_level2[i])
                                list_level2_copy.insert(insert_index+1, list_need_add)
                                list_level3 = list(set(list_level3).difference(set(list_need_add)))
                                list_level3.sort()
                                print(list_level3)
                                list_need_add = []
                                flag = 0
                                break


                # 从新赋值给list_level2
                list_level2 = list_level2_copy


                print(list_level2)
                for each_list_level2 in list_level2:
                    if type(each_list_level2) != list:
                        name = num_judge_list_all_div[each_list_level2].replace('编辑', '').replace('.','_').replace('\n', '')
                        print(name)


                for each_list_level3 in list_level3:
                    name = num_judge_list_all_div[each_list_level3]
                    print(name)

                dict_tag_result = {}
                list_result = list_level2
                for i in range(length_list_level2-1):
                    if type(list_result[i]) == list:
                        continue
                    else:
                        # print(i)
                        if length_list_level2 - (i+1) > 1:
                            if type(list_result[i+1]) != list:
                                start_num = list_result[i]+1
                                end_num = list_result[i+1]-1
                                content = ''
                                for i2 in range(start_num, end_num):
                                    content += num_judge_list_all_div[i2]
                                dict_tag_result[num_judge_list_all_div[list_result[i]].replace('编辑', '').replace('.','_').replace('\n', '')] = content
                            else:
                                dict_tag_result_little = {}
                                for i3 in range(len(list_result[i+1])):
                                    print(i3)
                                    len_linshi = len(list_result[i+1])
                                    start_num_little = list_result[i+1][i3]+1
                                    if i3 == len_linshi-1:
                                        i3_change = list_result[i+2]
                                        end_num_little = i3_change-1
                                    else:
                                        end_num_little = list_result[i+1][i3+1]-1
                                    content_little = ''
                                    for i4 in range(start_num_little, end_num_little):
                                        content_little += num_judge_list_all_div[i4]
                                    dict_tag_result_little[num_judge_list_all_div[list_result[i+1][i3]].replace('.','_')] = content_little
                                dict_tag_result[num_judge_list_all_div[list_result[i]].replace('编辑', '').replace('.','_').replace('\n', '')] = dict_tag_result_little
                        # 这是最后一项的情况
                        elif length_list_level2 - (i+1) == 1:
                            if type(list_result[i + 1]) != list:
                                start_num = list_result[i] + 1
                                end_num = list_result[i + 1] - 1
                                content = ''
                                for i2 in range(start_num, end_num):
                                    content += num_judge_list_all_div[i2]
                                dict_tag_result[num_judge_list_all_div[list_result[i]].replace('编辑', '').replace('.','_').replace('\n',
                                                                                                                 '')] = content
                            else:
                                dict_tag_result_little = {}
                                for i3 in range(len(list_result[i + 1])):
                                    print(i3)
                                    len_linshi = len(list_result[i + 1])
                                    start_num_little = list_result[i + 1][i3] + 1
                                    if i3 == len_linshi - 1:
                                        i3_change = list_result[i + 2]
                                        end_num_little = i3_change - 1
                                    else:
                                        end_num_little = list_result[i + 1][i3 + 1] - 1
                                    content_little = ''
                                    for i4 in range(start_num_little, end_num_little):
                                        content_little += num_judge_list_all_div[i4]
                                    dict_tag_result_little[
                                        num_judge_list_all_div[list_result[i + 1][i3]].replace('.','_')] = content_little
                                dict_tag_result[num_judge_list_all_div[list_result[i]].replace('编辑', '').replace('.','_').replace('\n',
                                                                                                                 '')] = dict_tag_result_little
                        elif length_list_level2 - (i+1) == 0:
                            pass

                print(dict_tag_result)



        record['resume'] = resume
        record['url'] = url_detail_page
        record['expert_name_en'] = expert_name_en
        record['expert_name'] = expert_name
        record['nation'] = nation
        record['birthplace'] = birthplace
        record['birthday'] = birthday
        record['university'] = university
        record['gender'] = gender
        record['job'] = job
        record['achievement'] = achievement
        record['another_name'] = another_name
        record['title'] = title
        record['native_place'] = native_place
        record['sub_nation'] = sub_nation
        record['crawl_time'] = datetime.datetime.now()
        record['tag'] = dict_tag_result
        record['img_url'] = img_url


        return record

    # 更新维护表
    def update_maintenance_table(self, flag_aminer, flag_aminer_result, search_key, research_institution):
        # flag = 1
        myquery = {'_id': self._id}
        newvalues = {"$set": {"flag_baidubaike": flag_aminer,
                              "flag_baidubaike_result": flag_aminer_result}}
        self.mongo_coll.update_one(myquery, newvalues)
        self.logger.info('维护表成功更新, kId:%s' % self.kId)





    def run(self, start_page=1, max_page=-1, page_size='20', round=1):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================

        # self.expert_name = '庄越挺'

        list_mongo = []
        c = 0
        for num, each in enumerate(self.mongo_coll.find()):

            # if num > 200:
            #     break
            print(num)
            dict_mongo = {}
            # print(each)
            search_key1 = each['name']
            search_key = search_key1
            research_institution = each['orgs']
            research_institution_en = ''
            kId = each['kId']
            department = ''
            url = each['url']
            category = ''
            source = ''
            _id = each['_id']
            
            # 百度百科的flag
            flag_baidubaike = each.get('flag_baidubaike', '')
            

            if (flag_baidubaike == 0 or flag_baidubaike == ''):
                dict_mongo['department'] = department
                dict_mongo['url'] = url
                dict_mongo['category'] = category
                dict_mongo['source'] = source
                dict_mongo['search_key'] = search_key
                dict_mongo['research_institution'] = research_institution
                dict_mongo['research_institution_en'] = research_institution_en
                dict_mongo['kId'] = kId
                dict_mongo['flag_baidubaike'] = flag_baidubaike
                dict_mongo['_id'] = _id
                list_mongo.append(dict_mongo)


        c_number = 0
        for each_list_mongo in list_mongo:
            self.logger.info(each_list_mongo)
            c_number += 1
            self.logger.info('第几个搜索关键词：%s' % str(c_number))

            # if c_number < 35000:
            #     continue

            search_key = each_list_mongo['search_key']
            research_institution = each_list_mongo['research_institution']
            research_institution_en = each_list_mongo['research_institution_en']
            kId = each_list_mongo['kId']
            flag = each_list_mongo['flag_baidubaike']
            self._id = each_list_mongo['_id']
            self.kId = kId


            r_in_db = self.mongo_read_col1.find_one(
                {'expert_name': search_key, 'research_institution_mongo': research_institution})
            if r_in_db:
                if kId not in r_in_db['kId']:
                    myquery = {'expert_name': search_key, 'research_institution_mongo': research_institution}
                    new_kId_list = r_in_db['kId']
                    new_kId_list.append(kId)
                    newvalues = {"$set": {"kId": new_kId_list}}
                    self.mongo_read_col1.update_one(myquery, newvalues)
                    self.logger.info('重复数据kId更新成功')
                else:
                    self.logger.info('数据库中数据重复, 跳过%s' % search_key)
                    self.update_maintenance_table(flag_aminer=1, flag_aminer_result=1, search_key=search_key, research_institution=research_institution)
                    continue
            else:
                self.logger.info('数据库中不存在，这是条新数据，需要插入')



            department = each_list_mongo['department']
            url = each_list_mongo['url']
            category = each_list_mongo['category']
            source = each_list_mongo['source']

            if flag == 0 or flag == '':
                detail_url = ''
                time.sleep(1)
                self.logger.info('flag_baike为0，进行百科搜索人名结果')
                self.expert_name = search_key
                self.research_institution = research_institution
                self.research_institution_en = research_institution_en
                self.kId = kId
                # 列表页
                list_data = []
                url_list_page = 'https://baike.baidu.com/item/'+str(self.expert_name)+'?force=1'

                # 列表页判断和专家唯一性查找

                resp_list_page = self.downloader.crawl_data(url_list_page, None, self.headers, "get")
                if resp_list_page:
                    resp_list_page.encoding = 'utf-8'
                    content_list_page = resp_list_page.text
                    detail_url = self.url_list_page(content_list_page, research_institution_en, research_institution, url_list_page)
                else:
                    self.logger.info('列表页不响应')

                if detail_url:
                    self.expert_name_zh = search_key
                    resp_detail_page = self.downloader.crawl_data(detail_url, None, self.headers, "get")
                    if resp_detail_page:
                        resp_detail_page.encoding = 'utf-8'
                        content_detail_page = resp_detail_page.text
                        # print(content_detail_page)

                        record = self.url_detail_page(content_detail_page, detail_url, search_key)
                        if record:
                            kId_list = []
                            kId_list.append(kId)
                            print(kId)
                            record['research_institution_mongo'] = research_institution
                            record['research_institution_mongo_en'] = research_institution_en
                            record['kId'] = kId_list
                            self.save_record(record, self.mongo_read_col1, {'expert_name':search_key, 'research_institution':research_institution}, search_key, research_institution)
                    #         flag_end = 1
                    #
                    # flag_end = 1
                    # if flag_end == 0:
                    #     self.logger.info('专家不匹配, pass')
                    #     self.update_maintenance_table(flag_aminer=1, flag_aminer_result=0)
                else:
                    self.logger.info('没有detail_url')
                    self.update_maintenance_table(flag_aminer=1, flag_aminer_result=0, search_key=search_key, research_institution=research_institution)


        self.logger.info("Finish Run")




if __name__ == '__main__':

    bp = ListDetailSpider(SAVE_MONGO_CONFIG2)
    bp.run(start_page=1, max_page=-1, page_size='10', round=2)
