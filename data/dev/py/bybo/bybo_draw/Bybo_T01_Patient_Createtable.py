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
if 'darwin' or 'linux' in sysplat.lower():
    commDri = cru_dir.split('/bybo/')[0] + '/bybo'
    # print(commDri)

sys.path.append(commDri)

'''
项目名称: 拜博口腔ODS数据抽取
文件名称: Bybo_T01_Patient_Createtable.py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class Bybo_T01Patient_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()

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
        logger.info("Bybo_T01_Patient_Createtable.py.py begintime=" + beginTime)

        try:
            # T01_Patient_H DW目标表
            sql_T01_Patient_H = '''
                         CREATE TABLE IF NOT EXISTS T01_Patient_H
(
    PatientID         varchar(50)    NOT NULL    DEFAULT '' COMMENT '患者编号',
    Name              varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者姓名',
    Sex               varchar(10)     NOT NULL   DEFAULT '' COMMENT '性别代码',
    Birth             date    NOT NULL    DEFAULT '1900-00-00' COMMENT '出生日期',
    FirstVisit        date      NOT NULL   DEFAULT '1900-00-00' COMMENT '初诊日期',
    MembershipId      varchar(50)  NOT NULL   DEFAULT '' COMMENT '会员编号',
    PatientType       varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者类型',
    SourceTypeLevel1  varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者一级来源',
    SourceTypeLevel2        varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者二级来源',
    SourceTypeLevel3       varchar(200)     NOT NULL   DEFAULT '' COMMENT '患者三级来源',
    Id                varchar(50)      NOT NULL   DEFAULT '' COMMENT '源系统患者编号',
    RecordCreatedUser varchar(50)     NOT NULL    DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Region       varchar(10)    NOT NULL    DEFAULT '' COMMENT '源系统区域代码',
    IsInactive   varchar(10)     NOT NULL    DEFAULT '-1' COMMENT '有效标识',
    CRMAccountId varchar(50)     NOT NULL    DEFAULT '' COMMENT 'CRM客户编号',
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
            cur.execute(sql_T01_Patient_H)

            # T01_Patient_H T01_Patient_H 临时表
            sql_T01_Patient_H_Tmp = '''
            CREATE TABLE IF NOT EXISTS T01_Patient_H_Tmp
(
    PatientID         varchar(50)    NOT NULL    DEFAULT '' COMMENT '患者编号',
    Name              varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者姓名',
    Sex               varchar(10)     NOT NULL   DEFAULT '' COMMENT '性别代码',
    Birth             date    NOT NULL    DEFAULT '1900-00-00' COMMENT '出生日期',
    FirstVisit        date      NOT NULL   DEFAULT '1900-00-00' COMMENT '初诊日期',
    MembershipId      varchar(50)  NOT NULL   DEFAULT '' COMMENT '会员编号',
    PatientType       varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者类型',
    SourceTypeLevel1  varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者一级来源',
    SourceTypeLevel2        varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者二级来源',
    SourceTypeLevel3       varchar(200)     NOT NULL   DEFAULT '' COMMENT '患者三级来源',
    Id                varchar(50)      NOT NULL   DEFAULT '' COMMENT '源系统患者编号',
    RecordCreatedUser varchar(50)     NOT NULL    DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Region       varchar(10)    NOT NULL    DEFAULT '' COMMENT '源系统区域代码',
    IsInactive   varchar(10)     NOT NULL    DEFAULT '-1' COMMENT '有效标识',
    CRMAccountId varchar(50)     NOT NULL    DEFAULT '' COMMENT 'CRM客户编号',
    Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
    Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
    Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
    SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
    ) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
            '''
            cur.execute(sql_T01_Patient_H_Tmp)

            # T01_Patient_H T01_Patient_H log表
            sql_T01_Patient_H_Log = '''
                        CREATE TABLE IF NOT EXISTS T01_Patient_H_Log
            (
    PatientID         varchar(50)    NOT NULL    DEFAULT '' COMMENT '患者编号',
    Name              varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者姓名',
    Sex               varchar(10)     NOT NULL   DEFAULT '' COMMENT '性别代码',
    Birth             date    NOT NULL    DEFAULT '1900-00-00' COMMENT '出生日期',
    FirstVisit        date      NOT NULL   DEFAULT '1900-00-00' COMMENT '初诊日期',
    MembershipId      varchar(50)  NOT NULL   DEFAULT '' COMMENT '会员编号',
    PatientType       varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者类型',
    SourceTypeLevel1  varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者一级来源',
    SourceTypeLevel2        varchar(200)    NOT NULL    DEFAULT '' COMMENT '患者二级来源',
    SourceTypeLevel3       varchar(200)     NOT NULL   DEFAULT '' COMMENT '患者三级来源',
    Id                varchar(50)      NOT NULL   DEFAULT '' COMMENT '源系统患者编号',
    RecordCreatedUser varchar(50)     NOT NULL    DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Region       varchar(10)    NOT NULL    DEFAULT '' COMMENT '源系统区域代码',
    IsInactive   varchar(10)     NOT NULL    DEFAULT '-1' COMMENT '有效标识',
    CRMAccountId varchar(50)     NOT NULL    DEFAULT '' COMMENT 'CRM客户编号',
    Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
    Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
    Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
    Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
    SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
              DEFAULT CHARSET = utf8;
                        '''
            cur.execute(sql_T01_Patient_H_Log)


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
            self.addMessage(conn, 'Bybo_T01_Patient_Createtable.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = Bybo_T01Patient_Createtable()
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
