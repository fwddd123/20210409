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


class T02_MemberShipTransaction_H_Data2Dw(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name = 'T02_MemberShipTransaction_H'
        self._taget_table = 'T02_MemberShipTransaction_H'
    def Draw_data(self,end_date,begin_date):
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
        logger.info("%s_Data2Dw.py begintime=" % (self.__name) + beginTime)
        ods_crm = self.get_property_value('ods_crm')
        ods_ekanya = self.get_property_value('ods_ekanya')
        ods_card = self.get_property_value('Ods_card')


        try:
            # Part1 T01_Patient_H_Tmp 清空Tmp表
            sql_truncate = '''truncate table %s_Tmp;'''%(self.__name)
            print(sql_truncate)
            cur.execute(sql_truncate)

            # Part1 第一步数据回退;根据回退的时间将数据将End_date在回退时间内的数据重新改为2099-01-01 00:00:00;
            sql_P1_Rollback22099 = '''
                        update %s a
                                    set A.End_date='2099-01-01 00:00:00'
                                    where A.End_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                        and A.End_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                            ''' % (self.__name,begin_date,end_date)
            print(sql_P1_Rollback22099)
            cur.execute(sql_P1_Rollback22099)

            # Part1 第二步数据删除;在回退时间(Start_date)内的数据删除(类型为insert和update);
            sql_P1_Del = '''
                        delete from %s
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Indicate in ('insert','update')
                                                                        ''' % (self.__name,begin_date,end_date)
            print(sql_P1_Del)
            cur.execute(sql_P1_Del)

            # T01_Patient_H_Tmp 基础数据插入Tmp表
            sql2Tmp = '''
                insert into %s_tmp
                (MemberShipTransactionId
                ,MembershipId
                ,TransactionOfficeId
                ,patientId
                ,TargetPatientId
                ,DoctorId
                ,NurseId
                ,ChargeOrderId
                ,TransactionTypeCode
                ,PayType
                ,Amount
                ,Balance
                ,PrincipalBalance
                ,PrincipalAmount
                ,GiftAmount
                ,GiftBalance
                ,DepositBalance
                ,DepositAmount
                ,RecordCreatedDate
                ,RecordCreatedTime
                ,RecordUpdatedDate
                ,RecordUpdatedTime
                ,IsInactive
                ,Id
                ,Region
                ,Start_date
                ,InDw_timestamp
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                ifnull(concat(Region,LPAD(a.Id, 10, 0)),'') as MemberShipTransactionId
                ,ifnull(a.MembershipId,'') as MembershipId
                ,ifnull(concat(Region,LPAD(a.OfficeId, 10, 0)),'') as TransactionOfficeId
                ,ifnull(a.patientId,'') as patientId
                ,ifnull(a.TargetPatientId,'') as TargetPatientId
                ,ifnull(a.DoctorId,'') as DoctorId
                ,ifnull(a.Id,'') as NurseId
                ,ifnull(a.ChargeOrderId,'') as ChargeOrderId
                ,ifnull(a.TransactionType,'') as TransactionTypeCode
                ,ifnull(a.PayType,'') as PayType
                ,ifnull(a.Amount,'0') as Amount
                ,ifnull(a.Balance,'0') as Balance
                ,ifnull(a.PrincipalBalance,'0') as PrincipalBalance
                ,ifnull(a.PrincipalAmount,'0') as PrincipalAmount
                ,ifnull(a.GiftAmount,'0') as GiftAmount
                ,ifnull(a.GiftBalance,'0') as GiftBalance
                ,ifnull(a.DepositBalance,'0') as DepositBalance
                ,ifnull(a.DepositAmount,'0') as DepositAmount
                ,ifnull(DATE_FORMAT(a.RecordCreatedTime,'%%Y-%%m-%%d'),'1900-01-01') as RecordCreatedDate  
                ,ifnull(a.RecordCreatedTime,'1900-01-01 00:00:00') as RecordCreatedTime
                ,ifnull(DATE_FORMAT(a.RecordUpdatedTime,'%%Y-%%m-%%d'),'1900-01-01') as RecordUpdatedDate
                ,ifnull(a.RecordUpdatedTime,'1900-01-01 00:00:00') as RecordUpdatedTime
                ,ifnull(a.IsInactive,'') as IsInactive
                ,ifnull(a.ID,'') as Id
                ,ifnull(a.Region,'') as Region
                ,ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)                         as Start_date,
                now()                                                                    as InDw_timestamp,
                (a.Id DIV 100000) * 10                                                   as Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                md5(concat(a.Region, '_', a.id))                                         as Contrast_Id,
                'bbkq_ods_ekanya.membershiptransaction'                                            as SourceName
                from %s.membershiptransaction a
                where  ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)
                >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)
                <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                ''' % (self.__name, ods_ekanya, begin_date, end_date)
            print(sql2Tmp)
            cur.execute(sql2Tmp)

            sqlDel = '''
                        delete from %s_Log
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                                        ''' % (self.__name, begin_date, end_date)
            print(sqlDel)
            cur.execute(sqlDel)

            # T01_Patient_H_log 基础数据插入log表
            sql2Log = '''
                insert into %s_log	
                (MemberShipTransactionId
                ,MembershipId
                ,TransactionOfficeId
                ,patientId
                ,TargetPatientId
                ,DoctorId
                ,NurseId
                ,ChargeOrderId
                ,TransactionTypeCode
                ,PayType
                ,Amount
                ,Balance
                ,PrincipalBalance
                ,PrincipalAmount
                ,GiftAmount
                ,GiftBalance
                ,DepositBalance
                ,DepositAmount
                ,RecordCreatedDate
                ,RecordCreatedTime
                ,RecordUpdatedDate
                ,RecordUpdatedTime
                ,IsInactive
                ,Id
                ,Region
                ,Start_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                a.MemberShipTransactionId
                ,a.MembershipId
                ,a.TransactionOfficeId
                ,a.patientId
                ,a.TargetPatientId
                ,a.DoctorId
                ,a.NurseId
                ,a.ChargeOrderId
                ,a.TransactionTypeCode
                ,a.PayType
                ,a.Amount
                ,a.Balance
                ,a.PrincipalBalance
                ,a.PrincipalAmount
                ,a.GiftAmount
                ,a.GiftBalance
                ,a.DepositBalance
                ,a.DepositAmount
                ,a.RecordCreatedDate
                ,a.RecordCreatedTime
                ,a.RecordUpdatedDate
                ,a.RecordUpdatedTime
                ,a.IsInactive
                ,a.Id
                ,a.Region
                ,a.Start_date
                ,a.InDw_timestamp
                ,if(isnull(nullif(a.Contrast_Id, b.Contrast_Id)) = 0, 'insert', 'update') as Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_Tmp a
                left join %s b  on a.Contrast_Id = b.Contrast_Id and b.End_date='2099-01-01 00:00:00'
                where A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                    ''' % (self.__name, self.__name, self.__name, begin_date, end_date)
            print(sql2Log)
            cur.execute(sql2Log)

            # Part2 第一步数据写入根据update的数据将原有库里的对应数据的End_date置为InDw_timestamp；
            sql_P2_Update = '''
                update %s a inner join  %s_log b  on a.Contrast_Id = b.Contrast_Id
                set a.End_date=b.RecordUpdatedTime
                where b.Indicate = 'update'
                and B.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and B.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                        '''% (self.__name,self.__name,begin_date, end_date)
            print(sql_P2_Update)
            cur.execute(sql_P2_Update)

            # Part2 第二步数据写入insert、update的数据的End_date都为'2099-01-01 00:00:00'；T01_Patient_H log表入最终表
            sql_P2_Write = '''
                insert into %s																		
                (MemberShipTransactionId
                ,MembershipId
                ,TransactionOfficeId
                ,patientId
                ,TargetPatientId
                ,DoctorId
                ,NurseId
                ,ChargeOrderId
                ,TransactionTypeCode
                ,PayType
                ,Amount
                ,Balance
                ,PrincipalBalance
                ,PrincipalAmount
                ,GiftAmount
                ,GiftBalance
                ,DepositBalance
                ,DepositAmount
                ,RecordCreatedDate
                ,RecordCreatedTime
                ,RecordUpdatedDate
                ,RecordUpdatedTime
                ,IsInactive
                ,Id
                ,Region
                ,Start_date
                ,End_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                a.MemberShipTransactionId
                ,a.MembershipId
                ,a.TransactionOfficeId
                ,a.patientId
                ,a.TargetPatientId
                ,a.DoctorId
                ,a.NurseId
                ,a.ChargeOrderId
                ,a.TransactionTypeCode
                ,a.PayType
                ,a.Amount
                ,a.Balance
                ,a.PrincipalBalance
                ,a.PrincipalAmount
                ,a.GiftAmount
                ,a.GiftBalance
                ,a.DepositBalance
                ,a.DepositAmount
                ,a.RecordCreatedDate
                ,a.RecordCreatedTime
                ,a.RecordUpdatedDate
                ,a.RecordUpdatedTime
                ,a.IsInactive
                ,a.Id
                ,a.Region
                ,a.Start_date
                ,'2099-01-01 00:00:00' as End_date
                ,a.InDw_timestamp
                ,a.Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_log a
                where A.Indicate in ('insert','update')
                and A.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')

          '''% (self.__name,self.__name,begin_date, end_date)
            print(sql_P2_Write)
            cur.execute(sql_P2_Write)

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
            self.addMessage(conn, '%s_Data2Dw.py '%(self.__name), jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':

    eg = T02_MemberShipTransaction_H_Data2Dw()
    logger = eg.logger
    logger.info("任务开始")
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


    try:
        zst = time.time()
        # last_suceedate = eg.get_last_suceedate()
        # print(last_suceedate)
        F_or_N = eg.get_rowcount('T02_MemberShipTransaction_H')
        print(F_or_N)
        if F_or_N > 0:
            eg.Draw_data(end_date2020_t3, frist_data_t3)
            # eg.Draw_data(end_date, begin_date)
        else:
            eg.Draw_data(end_date2020_t2, frist_data)
        # eg.Draw_data(end_date,begin_date)
        logger.info('【运行总时间】' + str(time.time() - zst)[:4])
    except Exception as e:
        logger.error("【运行异常】" + str(e))
