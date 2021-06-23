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


class T02_MemberShipTransaction_H_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name='T02_MemberShipTransaction_H'
        self._taget_table = 'T02_MemberShipTransaction_H'

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
                MemberShipTransactionId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易记录编号',
                MembershipId varchar(50) NOT NULL    DEFAULT ''COMMENT'会员编号',
                TransactionOfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易门店编号',
                patientId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡持卡人客户编号',
                TargetPatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易的客户编号',
                DoctorId varchar(50) NOT NULL    DEFAULT ''COMMENT'医生编号',
                NurseId varchar(50) NOT NULL    DEFAULT ''COMMENT'护士编号',
                ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易关联的收费单编号',
                TransactionTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'交易类型代码',
                PayType varchar(50) NOT NULL    DEFAULT ''COMMENT'支付类型',
                Amount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易总额',
                Balance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后总余额',
                PrincipalBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                PrincipalAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后馈赠金余额',
                DepositBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                DepositAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                RecordCreatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录创建日期',
                RecordCreatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录创建时间',
                RecordUpdatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录修改日期',
                RecordUpdatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                IsInactive varchar(10) NOT NULL    DEFAULT ''COMMENT'有效标识',
                Id varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统储值卡交易记录编号',
                Region varchar(10) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',
                
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
                (        MemberShipTransactionId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易记录编号',
                MembershipId varchar(50) NOT NULL    DEFAULT ''COMMENT'会员编号',
                TransactionOfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易门店编号',
                patientId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡持卡人客户编号',
                TargetPatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易的客户编号',
                DoctorId varchar(50) NOT NULL    DEFAULT ''COMMENT'医生编号',
                NurseId varchar(50) NOT NULL    DEFAULT ''COMMENT'护士编号',
                ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易关联的收费单编号',
                TransactionTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'交易类型代码',
                PayType varchar(50) NOT NULL    DEFAULT ''COMMENT'支付类型',
                Amount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易总额',
                Balance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后总余额',
                PrincipalBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                PrincipalAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后馈赠金余额',
                DepositBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                DepositAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                RecordCreatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录创建日期',
                RecordCreatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录创建时间',
                RecordUpdatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录修改日期',
                RecordUpdatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                IsInactive varchar(10) NOT NULL    DEFAULT ''COMMENT'有效标识',
                Id varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统储值卡交易记录编号',
                Region varchar(10) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',

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
                (
                MemberShipTransactionId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易记录编号',
                MembershipId varchar(50) NOT NULL    DEFAULT ''COMMENT'会员编号',
                TransactionOfficeId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡交易门店编号',
                patientId varchar(50) NOT NULL    DEFAULT ''COMMENT'储值卡持卡人客户编号',
                TargetPatientId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易的客户编号',
                DoctorId varchar(50) NOT NULL    DEFAULT ''COMMENT'医生编号',
                NurseId varchar(50) NOT NULL    DEFAULT ''COMMENT'护士编号',
                ChargeOrderId varchar(50) NOT NULL    DEFAULT ''COMMENT'本次交易关联的收费单编号',
                TransactionTypeCode varchar(10) NOT NULL    DEFAULT ''COMMENT'交易类型代码',
                PayType varchar(50) NOT NULL    DEFAULT ''COMMENT'支付类型',
                Amount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易总额',
                Balance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后总余额',
                PrincipalBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                PrincipalAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后本金余额',
                GiftBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后馈赠金余额',
                DepositBalance decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                DepositAmount decimal(18,2) NOT NULL    DEFAULT '0'COMMENT'交易后定金余额',
                RecordCreatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录创建日期',
                RecordCreatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录创建时间',
                RecordUpdatedDate date NOT NULL   DEFAULT '1900-00-00'COMMENT'记录修改日期',
                RecordUpdatedTime datetime NOT NULL DEFAULT '1900-01-01 00:00:00'COMMENT'记录修改时间',
                IsInactive varchar(10) NOT NULL    DEFAULT ''COMMENT'有效标识',
                Id varchar(50) NOT NULL    DEFAULT ''COMMENT'源系统储值卡交易记录编号',
                Region varchar(10) NOT NULL    DEFAULT ''COMMENT'源系统区域代码',
    
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
            self.addMessage(conn, 'T02_MemberShipTransaction_H.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T02_MemberShipTransaction_H_Createtable()
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
