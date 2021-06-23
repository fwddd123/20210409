#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback

import os
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
文件名称: Bybo_T01_Patient_Createtable. py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class T05_AiYa_SaleClue_H_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name='T05_AiYa_SaleClue_H'
        self._taget_table = 'T05_AiYa_SaleClue_H'

    def creat_table(self):
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
        beginTime = time.strftime(stF, time.localtime(time.time()))
        logger.info("%s.py" %(self.__name)+ beginTime)
        ods_crm = self.get_property_value('ods_crm')
        ods_ekanya = self.get_property_value('ods_ekanya')

        try:
            # T01_Patient_H DW目标表
            T01_Outter_Stock_H = '''
                CREATE TABLE IF NOT EXISTS %s
                (
                SaleClueId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'线索编号',
                UserId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'爱牙大使用户编号',
                ClueCustomerName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'意向客户姓名',
                CityId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'所在城市编号',
                CityName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'所在城市名称',
                AppointmentId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'预约编号',
                PatientId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                DataSource nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索来源代码',
                ClueStatus nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索状态代码',
                ClueResult nvarchar(10) NOT NULL    DEFAULT ''COMMENT'预约结果代码',
                ClueCustomerSex nvarchar(10) NOT NULL    DEFAULT ''COMMENT'意向客户性别代码',
                ClueCustomerPhone nvarchar(20) NOT NULL    DEFAULT ''COMMENT'意向客户电话号码',
                BusinessType nvarchar(10) NOT NULL    DEFAULT ''COMMENT'业务类型代码',
                ClueTreatment nvarchar(10) NOT NULL    DEFAULT ''COMMENT'诊疗项目代码',
                ExceptTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'期望就诊时间',
                CreateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'更新时间',

    
              
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                End_date datetime  NOT NULL DEFAULT '2099-01-01 00:00:00' COMMENT '结束时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8;
                       '''%(self.__name)
            cur.execute(T01_Outter_Stock_H)



            # T01_Patient_H T01_Patient_H 临时表
            T01_Outter_Stock_H_Tmp = '''
                CREATE TABLE IF NOT EXISTS %s_Tmp
                (        SaleClueId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'线索编号',
                UserId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'爱牙大使用户编号',
                ClueCustomerName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'意向客户姓名',
                CityId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'所在城市编号',
                CityName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'所在城市名称',
                AppointmentId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'预约编号',
                PatientId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                DataSource nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索来源代码',
                ClueStatus nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索状态代码',
                ClueResult nvarchar(10) NOT NULL    DEFAULT ''COMMENT'预约结果代码',
                ClueCustomerSex nvarchar(10) NOT NULL    DEFAULT ''COMMENT'意向客户性别代码',
                ClueCustomerPhone nvarchar(20) NOT NULL    DEFAULT ''COMMENT'意向客户电话号码',
                BusinessType nvarchar(10) NOT NULL    DEFAULT ''COMMENT'业务类型代码',
                ClueTreatment nvarchar(10) NOT NULL    DEFAULT ''COMMENT'诊疗项目代码',
                ExceptTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'期望就诊时间',
                CreateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'更新时间',

    
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8;
            '''%(self.__name)
            cur.execute(T01_Outter_Stock_H_Tmp)

            # T01_Patient_H T01_Patient_H log表
            T01_Outter_Stock_H_Log = '''
                CREATE TABLE IF NOT EXISTS %s_Log
              
                (        SaleClueId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'线索编号',
                UserId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'爱牙大使用户编号',
                ClueCustomerName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'意向客户姓名',
                CityId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'所在城市编号',
                CityName nvarchar(100) NOT NULL    DEFAULT ''COMMENT'所在城市名称',
                AppointmentId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'预约编号',
                PatientId nvarchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                DataSource nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索来源代码',
                ClueStatus nvarchar(10) NOT NULL    DEFAULT ''COMMENT'线索状态代码',
                ClueResult nvarchar(10) NOT NULL    DEFAULT ''COMMENT'预约结果代码',
                ClueCustomerSex nvarchar(10) NOT NULL    DEFAULT ''COMMENT'意向客户性别代码',
                ClueCustomerPhone nvarchar(20) NOT NULL    DEFAULT ''COMMENT'意向客户电话号码',
                BusinessType nvarchar(10) NOT NULL    DEFAULT ''COMMENT'业务类型代码',
                ClueTreatment nvarchar(10) NOT NULL    DEFAULT ''COMMENT'诊疗项目代码',
                ExceptTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'期望就诊时间',
                CreateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'更新时间',
                
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8;
                            '''%(self.__name)
            cur.execute(T01_Outter_Stock_H_Log)


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
            self.addMessage(conn, 'T05_AiYa_SaleClue_H.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T05_AiYa_SaleClue_H_Createtable()
    logger = eg.logger
    logger.info("任务开始")
    stF = "%Y-%m-%d %H:%M:%S"
    # 默认为前一天
    # yesterday = datetime.date.today() - datetime.timedelta(days=1)
    # begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF)
    # end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF)

    try:
        zst = time.time()
        eg.creat_table()
        logger.info('【运行总时间】' + str(time.time() - zst)[:4])
    except Exception as e:
        logger.error("【运行异常】" + str(e))
