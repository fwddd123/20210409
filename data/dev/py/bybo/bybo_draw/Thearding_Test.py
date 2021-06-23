#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback
import threading

import os
cru_dir = os.path.abspath(__file__)
sysplat = sys.platform
if 'win32' in sysplat.lower():
    commDri = cru_dir.split('\\bybo\\')[0] + '\\bybo'
    # print(commDri)
if 'darwin' or 'linux' in sysplat.lower():
    commDri = cru_dir.split('/bybo/')[0] + '/bybo'
    # print(commDri)

sys.path.append(commDri)

from bybo_util.bybo_base import Bybo_base

'''
项目名称: 拜博口腔ODS数据抽取
文件名称: Bybo_T01_Patient_Createtable.py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class Bybo_T02Appointment_Data2Dw(Bybo_base):
    def __init__(self,threadID, name, delay):
        super().__init__()
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.delay = delay

    def Draw_data(self,end_date,begin_date,regin):
        # 参数从命令行获取
        starttime = datetime.datetime.now()
        jobSt = 1
        jobMsg = '成功'
        logger = self.logger
        conn = self.get_connect()
        cur = conn.cursor()
        sql = ''
        stF = "%Y-%m-%d %H:%M:%S"
        # row_del = 0
        # row_insert = 0
        # print(self.get_last_suceedate(conn,'Close_Bybo_T01_Patient_Data2Log.py '))
        beginTime = time.strftime(stF, time.localtime(time.time()))
        logger.info("Bybo_T02_Appointment_Data2Dw.py begintime=" + beginTime)

        try:
            # Part1 T01_Patient_H_Tmp 清空Tmp表
            sql_truncate = '''select  *  from T02_Appointment_H_Tmp where a= %s;''' %(regin)
            print(sql_truncate)
            # cur.execute(sql_truncate)



        except Exception as e:
            jobSt = 0
            jobMsg = str(e)
            logger.error(sql + "【异常sql】" + jobMsg)
            conn.rollback()
            raise e
        else:
            conn.commit()

        finally:
            endtime = datetime.datetime.now()
            sub = (endtime - starttime).seconds
            self.addMessage(conn, 'Bybo_T02_Appointment_Data2Dw.py ', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':
    eg1 = Bybo_T02Appointment_Data2Dw(1, "Thread-1", 1)
    eg2 = Bybo_T02Appointment_Data2Dw(2, "Thread-1", 2)
    logger1 = eg1.logger
    logger1.info("任务开始")
    logger2 = eg1.logger
    logger2.info("任务开始")

    stF = "%Y-%m-%d %H:%M:%S"
    regin = ["HB","XN","CN"]

    # 默认为前一天
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF)
    end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF)

    frist_data = '2020-01-01 00:00:00'
    end_date2020 = '2020-12-31 23:59:59'
    frist_data_t2 = '2021-01-01 00:00:00'
    end_date2020_t2 = '2021-03-14 23:59:59'
    frist_data_t3 = '2021-03-15 00:00:00'
    end_date2020_t3 = '2021-03-15 23:59:59'

    if len(sys.argv) == 2:
        begin_date = sys.argv[1] + ' 00:00:00'


    try:
        zst = time.time()
        # last_suceedate = eg.get_last_suceedate()
        # print(last_suceedate)
        F_or_N = eg1.get_rowcount('T02_Appointment_H')
        print(F_or_N)
        if F_or_N > 0:
            eg1.Draw_data(end_date, begin_date,regin[0])
            eg2.Draw_data(end_date, begin_date,regin[1])
            # eg.Draw_data(end_date, begin_date)
        else:
            eg1.Draw_data(begin_date, frist_data)
            eg2.Draw_data(begin_date, frist_data)

        # eg.Draw_data(end_date,begin_date)
        logger1.info('【运行总时间】' + str(time.time() - zst)[:4])
        logger2.info('【运行总时间】' + str(time.time() - zst)[:4])

    except Exception as e:
        logger1.error("【运行异常】" + str(e))
        logger2.error("【运行异常】" + str(e))

