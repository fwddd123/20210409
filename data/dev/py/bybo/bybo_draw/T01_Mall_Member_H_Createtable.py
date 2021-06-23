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
文件名称: Bybo_T01_Patient_Createtable.py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class T01_Mall_Member_H_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name = 'T01_Mall_Member_H'
        self._taget_table = 'T01_Mall_Member_H'

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
        logger.info("T01_Mall_Member_HCreatetable.py begintime=" + beginTime)


        Target_Dm = self.get_property_value('Target_Dm')

        try:
            # T01_Patient_H DW目标表
            sql = '''
            CREATE TABLE IF NOT EXISTS %s
(
                MemberId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编号',
                MemberName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员姓名',
                MemberNickname varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员昵称',
                MemberCode varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编码',
                MemberRealName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员真实姓名',
                PatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                Relationship varchar(50) NOT NULL    DEFAULT ''COMMENT'亲属关系',
                MemberGradeValue int NOT NULL    DEFAULT '0'COMMENT'会员经验值',
                Mobile varchar(50) NOT NULL    DEFAULT ''COMMENT'手机号',
                Phone varchar(50) NOT NULL    DEFAULT ''COMMENT'电话',
                MemberLevel varchar(10) NOT NULL    DEFAULT ''COMMENT'会员等级代码',
                Sex varchar(10) NOT NULL    DEFAULT ''COMMENT'性别代码',
                Source varchar(10) NOT NULL    DEFAULT ''COMMENT'会员来源代码',
                MemberType varchar(10) NOT NULL    DEFAULT ''COMMENT'会员其他类型代码',
                Status varchar(10) NOT NULL    DEFAULT ''COMMENT'用户状态代码',
                RegisterDate date NOT NULL    DEFAULT '1900-01-01'COMMENT'注册日期',
                RegisterTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'注册时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                CustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城客户编号',
                IntroducerCustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'推荐人商城客户编号',
                TshUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰生活用户编号',
                TkzxUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰康在线用户编号',
                
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                End_date datetime  NOT NULL DEFAULT '2099-01-01 00:00:00' COMMENT '结束时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8
                       '''%(self.__name)
            cur.execute(sql)

            # T01_Patient_H T01_Patient_H 临时表
            sql = '''
            CREATE TABLE IF NOT EXISTS %s_Tmp
                (MemberId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编号',
                MemberName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员姓名',
                MemberNickname varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员昵称',
                MemberCode varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编码',
                MemberRealName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员真实姓名',
                PatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                Relationship varchar(50) NOT NULL    DEFAULT ''COMMENT'亲属关系',
                MemberGradeValue int NOT NULL    DEFAULT '0'COMMENT'会员经验值',
                Mobile varchar(50) NOT NULL    DEFAULT ''COMMENT'手机号',
                Phone varchar(50) NOT NULL    DEFAULT ''COMMENT'电话',
                MemberLevel varchar(10) NOT NULL    DEFAULT ''COMMENT'会员等级代码',
                Sex varchar(10) NOT NULL    DEFAULT ''COMMENT'性别代码',
                Source varchar(10) NOT NULL    DEFAULT ''COMMENT'会员来源代码',
                MemberType varchar(10) NOT NULL    DEFAULT ''COMMENT'会员其他类型代码',
                Status varchar(10) NOT NULL    DEFAULT ''COMMENT'用户状态代码',
                RegisterDate date NOT NULL    DEFAULT '1900-01-01'COMMENT'注册日期',
                RegisterTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'注册时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                CustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城客户编号',
                IntroducerCustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'推荐人商城客户编号',
                TshUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰生活用户编号',
                TkzxUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰康在线用户编号',
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8
            '''%(self.__name)
            cur.execute(sql)

            # T01_Patient_H T01_Patient_H log表
            sql = '''
            CREATE TABLE IF NOT EXISTS %s_Log
(
     MemberId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编号',
                MemberName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员姓名',
                MemberNickname varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员昵称',
                MemberCode varchar(50) NOT NULL    DEFAULT ''COMMENT'商城会员编码',
                MemberRealName varchar(100) NOT NULL    DEFAULT ''COMMENT'商城会员真实姓名',
                PatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'客户编号',
                Relationship varchar(50) NOT NULL    DEFAULT ''COMMENT'亲属关系',
                MemberGradeValue int NOT NULL    DEFAULT '0'COMMENT'会员经验值',
                Mobile varchar(50) NOT NULL    DEFAULT ''COMMENT'手机号',
                Phone varchar(50) NOT NULL    DEFAULT ''COMMENT'电话',
                MemberLevel varchar(10) NOT NULL    DEFAULT ''COMMENT'会员等级代码',
                Sex varchar(10) NOT NULL    DEFAULT ''COMMENT'性别代码',
                Source varchar(10) NOT NULL    DEFAULT ''COMMENT'会员来源代码',
                MemberType varchar(10) NOT NULL    DEFAULT ''COMMENT'会员其他类型代码',
                Status varchar(10) NOT NULL    DEFAULT ''COMMENT'用户状态代码',
                RegisterDate date NOT NULL    DEFAULT '1900-01-01'COMMENT'注册日期',
                RegisterTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'注册时间',
                UpdateTime datetime NOT NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                CustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'商城客户编号',
                IntroducerCustomerId varchar(50) NOT NULL    DEFAULT ''COMMENT'推荐人商城客户编号',
                TshUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰生活用户编号',
                TkzxUserId varchar(50) NOT NULL    DEFAULT ''COMMENT'泰康在线用户编号',
                Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
                InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
                Indicate varchar(10) NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
                Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
                Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
                SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
                ) ENGINE = InnoDB
                DEFAULT CHARSET = utf8
                '''%(self.__name)
            cur.execute(sql)


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
            self.addMessage(conn, 'Bybo_T01_Office_Createtable.py ', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T01_Mall_Member_H_Createtable()
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
