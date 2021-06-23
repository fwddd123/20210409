#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback

import os
cru_dir = os.path.abspath(__file__)
sysplat = sys.platform
if 'win32' in sysplat.lower():
    commDri = cru_dir.split('\\bybo\\')[0] + '\\bybo'
    # print(commDri)
if ('darwin' or 'linux') in sysplat.lower():
    commDri = cru_dir.split('/bybo/')[0] + '/bybo'
    print(commDri)
sys.path.append(commDri)
from bybo_util.bybo_base import Bybo_base

'''
项目名称: 拜博口腔ODS数据抽取
文件名称: Bybo_T01_Patient_Createtable. py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class T01_Promoter_H_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name='T01_Promoter_H'
        self._taget_table = 'T01_Promoter_H'

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
            PromoterId varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编号',
            PromoterCode varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编码',
            BusinessUnitId varchar(50) NOT NULL    DEFAULT ''COMMENT'所属部门编号',
            ChannelId varchar(50) NOT NULL    DEFAULT ''COMMENT'经销商编号',
            ProviderId varchar(50) NOT NULL    DEFAULT ''COMMENT'E看牙员工编号',
            StoresId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编码',
            DepartmentTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'部门类型代码',
            PromoterName varchar(50) NOT NULL    DEFAULT ''COMMENT'人员姓名',
            CreatedOnDate date NOT NULL   DEFAULT '1900-01-01'COMMENT'创建日期',
            CreatedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
            ModifiedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'修改时间',
            OfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编号',
            Region varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',

          

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
                       '''%(self.__name)
            cur.execute(T01_Outter_Stock_H)



            # T01_Patient_H T01_Patient_H 临时表
            T01_Outter_Stock_H_Tmp = '''
                CREATE TABLE IF NOT EXISTS %s_Tmp
                ( PromoterId varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编号',
                PromoterCode varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编码',
                BusinessUnitId varchar(50) NOT NULL    DEFAULT ''COMMENT'所属部门编号',
                ChannelId varchar(50) NOT NULL    DEFAULT ''COMMENT'经销商编号',
                ProviderId varchar(50) NOT NULL    DEFAULT ''COMMENT'E看牙员工编号',
                StoresId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编码',
                DepartmentTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'部门类型代码',
                PromoterName varchar(50) NOT NULL    DEFAULT ''COMMENT'人员姓名',
                CreatedOnDate date NOT NULL   DEFAULT '1900-01-01'COMMENT'创建日期',
                CreatedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
                ModifiedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'修改时间',
                OfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编号',
                Region varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',
             

                RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
                RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
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
                (PromoterId varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编号',
                PromoterCode varchar(50) NOT NULL    DEFAULT ''COMMENT'人员档案编码',
                BusinessUnitId varchar(50) NOT NULL    DEFAULT ''COMMENT'所属部门编号',
                ChannelId varchar(50) NOT NULL    DEFAULT ''COMMENT'经销商编号',
                ProviderId varchar(50) NOT NULL    DEFAULT ''COMMENT'E看牙员工编号',
                StoresId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编码',
                DepartmentTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'部门类型代码',
                PromoterName varchar(50) NOT NULL    DEFAULT ''COMMENT'人员姓名',
                CreatedOnDate date NOT NULL   DEFAULT '1900-01-01'COMMENT'创建日期',
                CreatedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'创建时间',
                ModifiedOnTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'修改时间',
                OfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'门店编号',
                Region varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',
            
                
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
            self.addMessage(conn, 'T01_Promoter_H.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg =T01_Promoter_H_Createtable()
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
