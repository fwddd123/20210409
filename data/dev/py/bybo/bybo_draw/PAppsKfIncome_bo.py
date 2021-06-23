#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback


sys.path.append('/Users/jiangyi/Desktop/test2/pyWork/work/doWork/bybo')

from bybo_util.bybo_base import Bybo_base


'''
项目名称: 数据整合  大数据平台
文件名称: p_apps_kf_income.py 
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


# class P_APPS_KF_Income(TkzjBase):
class PAppsKfIncome(Bybo_base):
    def __init__(self):
        super().__init__()

    '''
    @Description: 创建表kpi02_hospital_day，插入数据，该类只是示例程序。
    @参数 self
    @Return: 无
    @author zyh
    @CreateDate: 2019年11月11日15:56:40
    @update: 2019年11月12日15:44:40
    '''

    def do_analysis(self):
        starttime = datetime.datetime.now()
        jobSt = 1
        jobMsg = '成功'
        logger = self.logger
        # conn = None
        # cur = None
        conn = self.get_connect()
        cur = conn.cursor()

        sql = ''
        isFull = int(self.get_property_value('ISFULL'))
        days = int(self.get_property_value('days'))
        Target_DataBase=self.get_property_value('Target_DataBase')

        logger.info('days=' + str(days))
        logger.info('isfull=' + str(isFull))
        stF = "%Y-%m-%d %H:%M:%S"
        # beginTime = time.strftime(stF, time.localtime(time.time()))
        timeNow = datetime.datetime.now()
        timeNow -= datetime.timedelta(days=days)
        timeArray = time.strptime(timeNow.strftime(stF), stF)
        LastDate = int(time.mktime(timeArray))
        whereConditon = ''
        try:
            # conn = self.get_connect()
            # cur = conn.cursor()
            sql = '''
              CREATE TABLE IF NOT EXISTS %s.`kpi02_hospital_day` (
               `PERIOD_ID` int(11) DEFAULT NULL,
                `HOSP_ID` varchar(10) DEFAULT NULL,
                `DEPT_ID` varchar(10) DEFAULT NULL,
                `PAT_TYP` varchar(10) DEFAULT NULL,
                `KPI_CODE` varchar(6) DEFAULT NULL,
                `VALUE` decimal(40,6) DEFAULT '0.000000',
                 `CREATETIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 `UPDATETIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY `uniq_index` (`PERIOD_ID`,`HOSP_ID`,`DEPT_ID`,`PAT_TYP`,`KPI_CODE`) USING BTREE
                  ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''' % (Target_DataBase)
            cur.execute(sql)

            sql = '''
              insert into %s.kpi02_hospital_day (
               PERIOD_ID ,
               HOSP_ID,
               DEPT_ID,
               PAT_TYP,
               KPI_CODE)
                VALUES (%d,'%s','%s','%s','%s')
            ''' % (Target_DataBase,LastDate, 'a', 'b', 'c', 'd')
            cur.execute(sql)
        except Exception as e:
            jobSt = 0
            # logger.error(traceback.format_exc())
            jobMsg = traceback.format_exc()
            # logger.error(sql + "【异常sql】" + jobMsg)
            conn.rollback()
            raise e
        else:
            conn.commit()
        finally:
            endtime = datetime.datetime.now()
            sub = (endtime - starttime).seconds
            try:
                self.addMessage(conn, 'p_apps_kf_income1.py', jobMsg, jobSt, starttime.strftime("%Y-%m-%d %H:%M:%S"),
                                endtime.strftime("%Y-%m-%d %H:%M:%S"), sub)
            except Exception as e2:
                logger.error(traceback.format_exc())
            cur.close()
            conn.close()


'''
@Description: 程序运行入口
@Return: 无
@author czf
@CreateDate: 2020年04月27日15:56:40
@update: 2020年04月27日15:56:40
'''
if __name__ == '__main__':
    eg = PAppsKfIncome()
    logger = eg.logger
    stF = "%Y-%m-%d %H:%M:%S"
    beginTime = time.strftime(stF, time.localtime(time.time()))
    logger.info("p_apps_kf_income1.py begintime=" + beginTime)
    try:
        zst = time.time()
        eg.do_analysis()
        logger.info('【运行总时间】' + str(time.time() - zst)[:4])
    except Exception as e:
        logger.error("【运行异常】" + str(e))
        eg.do_analysis()
