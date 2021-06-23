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


class Bybo_T02ChargeOrderDetail_Createtable(Bybo_base):
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
        logger.info("Bybo_T02_ChargeOrderDetail_Createtable.py begintime=" + beginTime)

        try:
            # T01_Patient_H DW目标表
            sql_T02_ChargeOrderDetail_H = '''
                         CREATE TABLE IF NOT EXISTS T02_ChargeOrderDetail_H
(
    ChargeOrderDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单明细编号',
    ChargeOrderId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单编号',
    ChargeItemId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单项目编号',
    OfficeId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费门店编号',
    DoctorId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效医生编号',
    NurseId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效护士编号',
    ConsultantId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效咨询师编号',
    ItemSuperType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目大类名称',
    ItemSubType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目小类名称',
    ItemName varchar(200) NOT NULL    DEFAULT '' COMMENT'项目名称',
    ProcedureCode varchar(50) NOT NULL    DEFAULT '' COMMENT'项目编号',
    Count int NOT NULL    DEFAULT 0 COMMENT'项目数量',
    Unit  varchar(10) NOT NULL    DEFAULT '' COMMENT'项目单位',
    Price  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目单价',
    TotalPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'单项折扣后总应收金额',
    ActualPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目实收金额',
    PlanPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'应用单项和整单折扣后的总应收金额',
    Overdue  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目欠费金额',
    DiscountPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣掉的总金额',
    Discount  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣率',
    Id varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统收费单明细编号',
    Region varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统区域代码',
    IsInactive varchar(10) NOT NULL    DEFAULT '' COMMENT'有效标识',
    SourceDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'原始单据删除标志',

    RecordCreatedUser   varchar(50)    NOT NULL DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime   datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime   datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Start_date          datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    End_date            datetime       NOT NULL DEFAULT '2099-01-01 00:00:00' COMMENT '结束时间',
    InDw_timestamp      datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
    Indicate            varchar(10)    NOT NULL DEFAULT '' COMMENT '标识符，该数据为del、upd、ins',
    Partitions_ID       int            NOT NULL DEFAULT 0 COMMENT '分区键',
    Contrast_Id         varchar(32)    NOT NULL DEFAULT '' COMMENT '比对键',
    SourceName          varchar(50)    NOT NULL DEFAULT '' COMMENT '数据源'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
                       '''
            cur.execute(sql_T02_ChargeOrderDetail_H)

            # T01_Patient_H T01_Patient_H 临时表
            sql_T02_ChargeOrderDetail_H_Tmp = '''
            CREATE TABLE IF NOT EXISTS T02_ChargeOrderDetail_H_Tmp
(
    ChargeOrderDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单明细编号',
    ChargeOrderId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单编号',
    ChargeItemId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单项目编号',
    OfficeId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费门店编号',
    DoctorId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效医生编号',
    NurseId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效护士编号',
    ConsultantId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效咨询师编号',
    ItemSuperType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目大类名称',
    ItemSubType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目小类名称',
    ItemName varchar(200) NOT NULL    DEFAULT '' COMMENT'项目名称',
    ProcedureCode varchar(50) NOT NULL    DEFAULT '' COMMENT'项目编号',
    Count int NOT NULL    DEFAULT 0 COMMENT'项目数量',
    Unit  varchar(10) NOT NULL    DEFAULT '' COMMENT'项目单位',
    Price  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目单价',
    TotalPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'单项折扣后总应收金额',
    ActualPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目实收金额',
    PlanPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'应用单项和整单折扣后的总应收金额',
    Overdue  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目欠费金额',
    DiscountPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣掉的总金额',
    Discount  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣率',
    Id varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统收费单明细编号',
    Region varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统区域代码',
    IsInactive varchar(10) NOT NULL    DEFAULT '' COMMENT'有效标识',
    SourceDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'原始单据删除标志',

    RecordCreatedUser   varchar(50)    NOT NULL DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime   datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime   datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Start_date          datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    InDw_timestamp      datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
    Partitions_ID       int            NOT NULL DEFAULT 0 COMMENT '分区键',
    Contrast_Id         varchar(32)    NOT NULL DEFAULT '' COMMENT '比对键',
    SourceName          varchar(50)    NOT NULL DEFAULT '' COMMENT '数据源'
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
            '''
            cur.execute(sql_T02_ChargeOrderDetail_H_Tmp)

            # T01_Patient_H T01_Patient_H log表
            sql_T02_ChargeOrderDetail_H_Log = '''
                        CREATE TABLE IF NOT EXISTS T02_ChargeOrderDetail_H_Log
            (
    ChargeOrderDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单明细编号',
    ChargeOrderId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单编号',
    ChargeItemId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费单项目编号',
    OfficeId varchar(50) NOT NULL    DEFAULT '' COMMENT'收费门店编号',
    DoctorId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效医生编号',
    NurseId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效护士编号',
    ConsultantId varchar(50) NOT NULL    DEFAULT '' COMMENT'绩效咨询师编号',
    ItemSuperType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目大类名称',
    ItemSubType varchar(200) NOT NULL    DEFAULT '' COMMENT'项目小类名称',
    ItemName varchar(200) NOT NULL    DEFAULT '' COMMENT'项目名称',
    ProcedureCode varchar(50) NOT NULL    DEFAULT '' COMMENT'项目编号',
    Count int NOT NULL    DEFAULT 0 COMMENT'项目数量',
    Unit  varchar(10) NOT NULL    DEFAULT '' COMMENT'项目单位',
    Price  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目单价',
    TotalPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'单项折扣后总应收金额',
    ActualPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目实收金额',
    PlanPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'应用单项和整单折扣后的总应收金额',
    Overdue  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目欠费金额',
    DiscountPrice  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣掉的总金额',
    Discount  decimal(18,2) NOT NULL    DEFAULT 0.00 COMMENT'项目折扣率',
    Id varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统收费单明细编号',
    Region varchar(10) NOT NULL    DEFAULT '' COMMENT'源系统区域代码',
    IsInactive varchar(10) NOT NULL    DEFAULT '' COMMENT'有效标识',
    SourceDetailId varchar(50) NOT NULL    DEFAULT '' COMMENT'原始单据删除标志',
    
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
            cur.execute(sql_T02_ChargeOrderDetail_H_Log)


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
            self.addMessage(conn, 'Bybo_T02_ChargeOrderDetail_Createtable.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = Bybo_T02ChargeOrderDetail_Createtable()
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
