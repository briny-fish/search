import requests
from lxml import etree
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime, timedelta
import random
import tqdm
import json
import re
import html2text
import time
# -*- coding:utf-8 -*-
import pandas as pd
import re


import jieba


def filter_tags(htmlstr):
    """
    # Python通过正则表达式去除(过滤)HTML标签
    :param htmlstr:
    :return:
    """
    # 先过滤CDATA
    re_cdata = re.compile('//<!\
    CDATA\[[ >]∗ //\
    CDATA\[[ >]∗ //\
    \] > ',re.I) #匹配CDATA
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.I)
    # Script
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.I)
    # style
    re_br = re.compile('<br\s*?/?>')
    # 处理换行
    re_h = re.compile('</?\w+[^>]*>')
    # HTML标签
    re_comment = re.compile('<!--[^>]*-->')
    # HTML注释
    s = re_cdata.sub('', htmlstr)
    # 去掉CDATA
    s = re_script.sub('', s)  # 去掉SCRIPT
    s = re_style.sub('', s)
    # 去掉style
    s = re_br.sub('\n', s)
    # 将br转换为换行
    s = re_h.sub('', s)  # 去掉HTML 标签
    s = re_comment.sub('', s)
    # 去掉HTML注释
    # 去掉多余的空行
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    s = replaceCharEntity(s)  # 替换实体
    return s



def replaceCharEntity(htmlstr):
    """
    :param htmlstr:HTML字符串
    :function:过滤HTML中的标签
    """
    CHAR_ENTITIES = {'nbsp': ' ', '160': ' ',
                     'lt': '<', '60': '<',
                     'gt': '>', '62': '>',
                     'amp': '&', '38': '&',
                     'quot': '"', '34': '"', }

    re_charEntity = re.compile(r'&#?(?P<name>\w+);')
    sz = re_charEntity.search(htmlstr)
    while sz:
        entity = sz.group()  # entity全称，如>
        key = sz.group('name')  # 去除&;后entity,如>为gt
        try:
            htmlstr = re_charEntity.sub(CHAR_ENTITIES[key], htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
        except KeyError:
            # 以空串代替
            htmlstr = re_charEntity.sub('', htmlstr, 1)
            sz = re_charEntity.search(htmlstr)
    return htmlstr


def repalce(s, re_exp, repl_string):
    return re_exp.sub(repl_string,s)



import re
def Cleaning_data(x):
    m2=str(x).replace('<p>&nbsp; &nbsp; &nbsp; &nbsp;','').replace('</p><p><br></p>','').replace('<br>','').replace('</p>','').replace('<p>','').replace('       ','').replace('[图片]','').strip()
    m3=filter_tags(m2)
    m4=replaceCharEntity(m3)
    return m4

class ElasticObj:
    def __init__(self, index_name, index_type, ip="localhost"):
        self.index_name = index_name
        self.index_type = index_type
        self.cnt = 0
        self.es = Elasticsearch([ip], http_auth=('elastic', '123456123'), port=9200, timeout=20)

    def Get_Data_Id(self, url):
        tmp = {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "url": url
                        }
                    }
                }
            }
        }
        res = self.es.search(index=self.index_name, size=1, body=tmp)['hits']['hits'][0]
        # print(res)
        return res
    def Get_Score_ID(self, query):
        tmp = ['' for i in range(4)]
        tmp[0] = {
            "query": {
                "bool": {
                    "should":
                        [{"match":
                              {"data": {"query":query.lower(),"fuzziness":"0"}}},
                         {"match":
                              {"title": {"query": query.lower(), "boost": 0.863,"fuzziness":"0"}}}]
                }
            }

        }

        res = []
        xx = 0
        res = self.es.search(index=self.index_name, size=10, body=tmp[0])['hits']['hits']
        xx+=1
        if len(res)<10:
            ttmp = {
                   "size": 10-len(res),
                   "query": {
                      "function_score": {
                         "functions": [
                            {
                               "random_score": {
                                  "seed": random.randint(0,10000)
                               }
                            }
                         ]
                      }
                   }
                }
            ttmp = self.es.search(index=self.index_name, size=10-len(res), body=ttmp)['hits']['hits']
            res+=ttmp
        return res

    def Update(self,id, data):
        update_test = {
            "script": {
                "lang": "painless",
                "source": "ctx._source.data=params.data;",
                "params": {"data": data}
            }
        }
        self.es.update(index=self.index_name, id=id, body=update_test)

def get_html_text(url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'refer': 'http://data.people.com.cn/pd/gjzcxx/s?qs={%22cId%22:%2230%22,%22cds%22:[{%22fld%22:%22dataTime.start%22,%22cdr%22:%22AND%22,%22hlt%22:%22false%22,%22vlr%22:%22AND%22,%22qtp%22:%22DEF%22,%22val%22:%222021-04-01%22},{%22fld%22:%22dataTime.end%22,%22cdr%22:%22AND%22,%22hlt%22:%22false%22,%22vlr%22:%22AND%22,%22qtp%22:%22DEF%22,%22val%22:%222021-04-15%22}],%22obs%22:[{%22fld%22:%22dataTime%22,%22drt%22:%22DESC%22}]}',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
        'cookies': 'targetEncodinghttp://127001=2; wdcid=582f7d0386e28398; validateCode=S3YKEJrhCFtfeUFcYnWJEg%3D%3D; JSESSIONID=EB65199DB7FE5D13DE83ABDBAC5E4884; pageSize=20; pageNo=4'
    }
    html = requests.get(url, headers=headers,timeout=3)
    html.raise_for_status()
    html.encoding = "utf-8"
    #print(etree.tostring(etree.HTML(html.text)))
    return etree.tostring(etree.HTML(html.text)).decode()


urllist = []

# outfile_tr = open('fail_url_tr1','a+',encoding='utf-8')
esObj_en = ElasticObj('en_url_test', '_doc')
url_en = open('fail_url_tr','r',encoding='utf-8')
url_en = [x[:-1] for x in url_en.readlines()]

# esObj_tr = ElasticObj('tr_url_test', '_doc')
# with open('en_test_urls.csv','r',encoding='utf-8') as f:
#     f.readline()
#     cnt = 0
#     for line in f.readlines():
#         print(cnt)
#         cnt+=1
#         tmp = line.split('')
#         url = tmp[0]
#         title = tmp[1][:-1]
#         try:
#             html = get_html_text(url)
#             data = html2text.html2text(html)
#             data = re.sub('\n\n','',data)
#
#             data = re.sub('[\t\r]',' ',data)
#
#             #print(data)
#             id = esObj_en.Get_Data_Id(url)['_id']
#             esObj_en.Update(id,data)
#         except:
#             outfile_en.write(url+'\n')

outfile_en = open('fail_url_en', 'a', encoding='utf-8')
def proc(lines,i):
    cnt = 0
    totF = 0
    for line in lines:
        #print(cnt)
        cnt+=1
        tmp = line.split('')
        url = tmp[0]
        try:
            html = get_html_text(url)
            data = Cleaning_data(html)
            data = re.sub('\n','',data)
            data = re.sub('  ', ' ', data)
            data = re.sub('  ', ' ', data)
            data = re.sub('  ', ' ', data)
            data = re.sub('  ', ' ', data)
            data = re.sub('  ', ' ', data)
            id = esObj_en.Get_Data_Id(url)['_id']
            esObj_en.Update(id,data)

        except:
            totF+=1
            print('process:%s tot%s,totF%s'%(i,cnt,totF))

            outfile_en.write(url+'\n')
            outfile_en.flush()
            # outfile_en.close()

from multiprocessing import Process,Lock
if  __name__=='__main__':

    totlen = 710000
    per = int(totlen/60)
    f = open('en_test_urls.csv', 'r', encoding='utf-8')
    f.readline()
    lines = f.readlines()
    pcnt = 0
    for l in range(0,totlen,per):
        p = Process(target=proc,args=(lines[l:l+per],pcnt))
        pcnt+=1
        p.start()
