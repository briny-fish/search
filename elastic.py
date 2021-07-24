from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import random

class Data:
    def __init__(self, file_q, file_qrel, file_docs):
        self.queries = self.load_queries(file_q)
        self.qrels = self.load_qrels(file_qrel)
        self.docs = self.load_docs(file_docs)

    def load_queries(self, file):
        tmpQ = []
        with open(file, 'r', encoding='utf-8') as f:
            flag = 0
            for line in f.readlines():
                if flag==0:
                    flag = 1
                    continue
                tmp = line
                tmpID = tmp[:tmp.index(',')]
                tmpQuery = tmp[tmp.index(',') + 1:-1]
                tmpQ.append([tmpID,tmpQuery])
        print(len(tmpQ))
        return tmpQ

    def load_docs(self, file):
        pass
        # tmpDoc = []
        # with open(file, 'r', encoding='utf-8') as f:
        #     for line in f.readlines():
        #         tmp = line.split(' ')
        #         if (len(tmp) < 2):
        #             break
        #         tmpDoc.append([tmp[0], tmp[2]])
        #
        # print(len(tmpDoc))
        # # print(tmpDoc[-1])
        # return tmpDoc

    def load_qrels(self, file):
        pass
        # tmpQrel = {}
        # with open(file, 'r', encoding='utf-8') as f:
        #     for line in f.readlines():
        #         tmp = line.split(' ')
        #         if (len(tmp) < 2):
        #             break
        #         tmpQrel[tmp[0]] = tmp[2]
        # print(len(tmpQrel))
        # return tmpQrel


class ElasticObj:
    def __init__(self, index_name, index_type, ip="114.212.84.4"):
        self.index_name = index_name
        self.index_type = index_type
        self.cnt = 0
        self.es = Elasticsearch([ip], http_auth=('elastic', '123456123'), port=9200, timeout=20)

    def Get_Data_Id(self, id):
        tmp = {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "ID": id
                        }
                    }
                }
            }
        }
        res = self.es.search(index=self.index_name, size=1, body=tmp)['hits']['hits'][0]['_source']
        # print(res)

    def Get_Score_ID(self, id, query):
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
        # tmp[1] = {
        #     "query": {
        #         "bool": {
        #             "should":
        #                 [{"match":
        #                       {"data": {"query":query.lower(),"fuzziness":"1"}}},
        #                  {"match":
        #                       {"title": {"query": query.lower(), "boost": 0.3,"fuzziness":"1"}}}]
        #         }
        #     }
        #
        # }
        # tmp[2] = {
        #     "query": {
        #         "bool": {
        #             "should":
        #                 [{"match":
        #                       {"data": {"query": query.lower(), "fuzziness": "auto"}}},
        #                  {"match":
        #                       {"title": {"query": query.lower(), "boost": 0.3, "fuzziness": "auto"}}}]
        #         }
        #     }
        # }
        # tmp[3] = {
        #     "query": {
        #         "bool": {
        #             "should":
        #                 [{"match":
        #                       {"data": {"query": query.lower(), "fuzziness": "2"}}},
        #                  {"match":
        #                       {"title": {"query": query.lower(), "boost": 0.3, "fuzziness": "2"}}}]
        #         }
        #     }
        # }
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

    def update_summery(self, id, text):
        update_test = {
            "script": {
                "lang": "painless",
                "source": "ctx._source.summery=params.summery;",
                "params": {"summery": text}
            }
        }
        self.es.update(index="marco", id=id, body=update_test)


import string
import nltk
import re


def proc(x):
    # p1 = re.compile(r'-\{.*?(zh-hans|zh-cn):([^;]*?)(;.*?)?\}-')
    # p2 = re.compile(r'[(][: @ . , ？！\s][)]')
    # p3 = re.compile(r'[「『]')
    # p4 = re.compile(r'[\s+\.\!\/_,$%^*(+\"\')]+|[+——()?【】“”！，。？、~@#￥%……&*（）0-9 , : ; \-\ \[\ \]\ ]')

    # line = p1.sub(r' ', x)
    # line = p2.sub(r' ', line)
    # line = p3.sub(r' ', line)
    # line = p4.sub(r' ', line)
    tokens = nltk.word_tokenize(x)
    return tokens


from tqdm import tqdm

esObj_en = ElasticObj('en_url', '_doc')
esObj_tr = ElasticObj('tr_url', '_doc')
datas = Data('to_predict.csv', '', '')

# from gensim.summarization import bm25
# cop = []
# for i in tqdm(range(322)):

#     doc = {
#         "from":i*10000,
#         "size":10000 ,
#         "track_total_hits":True,
#         "query": {
#             "match_all": {}
#         }
#     }
#     coptmp = esObj.es.search(index=esObj.index_name,body=doc)['hits']
#     for line in coptmp['hits']:
#         cop.append(proc(line['_source']['title']+line['source']['data']))
# print(len(cop))
# import pickle
# bm25Model = bm25.BM25(cop)
# bmout = open('/home/data1/zongwz/bm25out.pkl','wb')
# pickle.dump(bm25Model,bmout)
outfile = open('submission.csv','w',encoding='utf-8')
outfile.write('qid,doc\n')
with tqdm(total=len(datas.queries)) as _tqdm:  # 使用需要的参数对tqdm进行初始化
    # 设置前缀 一般为epoch的信息
    #mrr = 0.0
    #totmrr = 0.0
    for i in range(0, len(datas.queries)):
        #_tqdm.set_postfix(mrr='{:.6f}'.format(mrr))  # 设置你想要在本次循环内实时监视的变量  可以作为后缀打印出来
        _tqdm.update(1)  # 设置你每一次想让进度条更新的iteration 大小
        # q = datas.docs[i][0]
        # ans = datas.qrels[q]
        # print((datas.queries.keys()))
        query = datas.queries[i][1]
        esObj = esObj_en
        if(datas.queries[i][0][:3]=='tr_'):
            esObj = esObj_tr
        rankinglist = esObj.Get_Score_ID('', query)
        urllist = []
        for x in range(len(rankinglist)):
            urllist.append(re.sub('\n','',rankinglist[x]['_source']['url']))

        outfile.write(datas.queries[i][0]+','+''.join(urllist)+'\n')

        # print(q)
        # print(rankinglist)



