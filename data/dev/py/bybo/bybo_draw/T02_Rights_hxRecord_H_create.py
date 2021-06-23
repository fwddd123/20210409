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


class T02_Rights_hxRecord_H_create(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name = 'T02_Rights_hxRecord_H'
        self._taget_table = 'T02_Rights_hxRecord_H'

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
        logger.info("Bybo_T02_Rights_hxRecord_H.py begintime=" + beginTime)
        ods_crm = self.get_property_value('ods_crm')
        ods_ekanya = self.get_property_value('ods_ekanya')

        try:
            # T01_Patient_H DW目标表
            sql_T02_Appointment_H = '''
                         CREATE TABLE IF NOT EXISTS T02_Rights_hxRecord_H
    (
            RightsHXRecordId varchar(50) NOT NULL    DEFAULT ''COMMENT'权益核销流水表编号',
            AccountId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户CRM编号',
            ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'账单编号',
            CardId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户卡券编号',
            RightsTypeId varchar(10) NOT NULL    DEFAULT ''COMMENT'权益类型代码',
            CreatedOnDateTime date NOT NULL   DEFAULT '1900-00-00'COMMENT'权益核销创建日期',
            CreatedOnDate datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'权益核销创建时间',
          
            State varchar(10) NOT NULL    DEFAULT ''COMMENT'状态代码',
            Region varchar(50) NOT NULL    DEFAULT '' COMMENT'源系统区域代码',

        
            RecordCreatedUser  varchar(50) NOT NULL DEFAULT '' COMMENT '记录创建人编号',
            RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
            RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
            Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
            End_date datetime  NOT NULL DEFAULT '2099-01-01 00:00:00' COMMENT '结束时间',
            InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
            Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
            Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
            Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
            SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
    ) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
                       '''
            cur.execute(sql_T02_Appointment_H)



            # T01_Patient_H T01_Patient_H 临时表
            sql_T02_Appointment_H_Tmp = '''
            CREATE TABLE IF NOT EXISTS T02_Rights_hxRecord_H_Tmp
(
            RightsHXRecordId varchar(50) NOT NULL    DEFAULT ''COMMENT'权益核销流水表编号',
            AccountId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户CRM编号',
            ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'账单编号',
            CardId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户卡券编号',
            RightsTypeId varchar(10) NOT NULL    DEFAULT ''COMMENT'权益类型代码',
            CreatedOnDateTime date NOT NULL   DEFAULT '1900-00-00'COMMENT'权益核销创建日期',
            CreatedOnDate datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'权益核销创建时间',
           
            State varchar(10) NOT NULL    DEFAULT ''COMMENT'状态代码',
            Region varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',

            RecordCreatedUser  varchar(50) NOT NULL DEFAULT '' COMMENT '记录创建人编号',
            RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
            RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
            Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
            InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
            Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
            Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
            SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
    ) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
            '''
            cur.execute(sql_T02_Appointment_H_Tmp)

            # T01_Patient_H T01_Patient_H log表
            sql_T02_Appointment_H_Log = '''
                        CREATE TABLE IF NOT EXISTS T02_Rights_hxRecord_H_Log
            (
            RightsHXRecordId varchar(50) NOT NULL    DEFAULT ''COMMENT'权益核销流水表编号',
            AccountId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户CRM编号',
            ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'账单编号',
            CardId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户卡券编号',
            RightsTypeId varchar(10) NOT NULL    DEFAULT ''COMMENT'权益类型代码',
            CreatedOnDateTime date NOT NULL   DEFAULT '1900-00-00'COMMENT'权益核销创建日期',
            CreatedOnDate datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'权益核销创建时间',
           
            State varchar(10) NOT NULL    DEFAULT ''COMMENT'状态代码',
            Region varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',

            RecordCreatedUser  varchar(50) NOT NULL DEFAULT '' COMMENT '记录创建人编号',
            RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
            RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
            Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
            InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
            Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
            Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
            Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
            SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
              DEFAULT CHARSET = utf8;
                        '''
            cur.execute(sql_T02_Appointment_H_Log)


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
            self.addMessage(conn, 'T02_Appointment_H.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T02_Rights_hxRecord_H_create()
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
