#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback
# from T98_Bybo_Cus_Treat import *
from Bybo_T02_Card_Income import *


import os
commDri ='/data/dev/py/bybo/bybo_util'
cru_dir = os.path.abspath(__file__)
sysplat = sys.platform
if 'win32' in sysplat.lower():
    commDri = cru_dir.split('\\bybo\\')[0] + '\\bybo\\bybo_util'
    # print(commDri)
if ('darwin' or 'linux') in sysplat.lower():
    commDri = cru_dir.split('/bybo/')[0] + '/bybo/bybo_util'
    # print(commDri)

sys.path.append(commDri)
from bybo_base import Bybo_base

'''
项目名称: 拜博口腔ODS数据抽取
文件名称: ByBo_test.py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''



class ByBo_RunSql(Bybo_base):

    def __init__(self):
        super().__init__()
        stF = "%Y-%m-%d %H:%M:%S"
        stF1 = "%Y-%m-%d"
        # 默认为前一天
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        self.begin_date = '2021-01-01'
        self.end_date = '2021-01-31'
        # self.begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF1)
        # self.end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF1)
        self._conn = self.get_connect()
        self._cur = self._conn.cursor()
        self._limit = 1
        self.order_name=''

    def log_info(self,order_name,order_table):
        self.order_name=order_name
        logger = self.logger
        starttime = datetime.datetime.now()
        jobSt = 1
        jobMsg = '成功'
        sql = ''
        stF = "%Y-%m-%d %H:%M:%S"
        beginTime = time.strftime(stF, time.localtime(time.time()))
        logger.info("Task begin")
        logger.info("draw "+self.order_name+" begintime=" + beginTime)
        try:
            self.Draw_data(order_table)
        except Exception as e:

            jobSt = 0
            jobMsg = str(e)
            logger.error(sql + "err Sql" + jobMsg)
            self._conn.rollback()
            raise e
        else:
            self._conn.commit()

        finally:
            endtime = datetime.datetime.now()
            sub = (endtime - starttime).seconds
            self.addMessage(self._conn, self.order_name, jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            logger.info(self.order_name + "endtime=" + endtime.strftime(stF)+",Use Total_time "+str(sub)+" second")

    def Draw_data(self, order_table):
        for query in order_table:
            print(query.format_map(vars()))
            self._cur.execute(query.format_map(vars()))

    def conn_close(self):
        self._cur.close()
        self._conn.close()
        self.logger.info("database close")

    # def main():



if __name__ == '__main__':
    stF = "%Y-%m-%d %H:%M:%S"
    # 默认为前一天
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF)
    end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF)

    frist_data = '1900-01-01 00:00:00'
    end_date2020 = '2020-12-31 23:59:59'
    frist_data_t2 = '2021-01-01 00:00:00'
    end_date2020_t2 = '2021-03-14 23:59:59'
    frist_data_t3 = '2021-03-15 00:00:00'
    end_date2020_t3 = '2021-03-15 23:59:59'
    if len(sys.argv) == 2:
        begin_date = sys.argv[1] + ' 00:00:00'

    h=ByBo_RunSql()
    # h.log_info('T98_Bybo_Cus_Treat',Cus_Treat_Create)
    # h.log_info('Cus_Treat', Cus_Treat)
    # h.log_info('T02_Card_Income_Create',T02_Card_Income_Create)
    h.log_info('T02_Card_Income',T02_Card_Income)

    h.conn_close()