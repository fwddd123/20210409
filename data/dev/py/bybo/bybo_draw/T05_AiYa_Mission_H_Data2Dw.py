#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import traceback

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


class T05_AiYa_Mission_H_Data2Dw(Bybo_base):
    def __init__(self):
        super().__init__()
        self._taget_table='T05_AiYa_Mission_H'
        self.__name='T05_AiYa_Mission_H'


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
        logger.info("%s_Data2Dw.py begintime="%(self.__name) + beginTime)
        Target_Dm =self.get_property_value('Target_Dm')
        ods_cus=self.get_property_value('ods_cus')


        try:
            # Part1 T01_Office_H 清空Tmp表
            sql_truncate = '''truncate table %s_Tmp;'''%(self.__name)
            print(sql_truncate)
            cur.execute(sql_truncate)

            # Part1 第一步数据回退;根据回退的时间将数据将End_date在回退时间内的数据重新改为2099-01-01 00:00:00;
            sql_P1_Rollback22099 = '''
                        update %s A
                                    set A.End_date='2099-01-01 00:00:00'
                                    where A.End_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                        and A.End_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                            ''' % (self.__name,begin_date, end_date)
            print(sql_P1_Rollback22099)
            cur.execute(sql_P1_Rollback22099)

            # Part1 第二步数据删除;在回退时间(Start_date)内的数据删除(类型为insert和update);
            sql_P1_Del = '''
                        delete from %s
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Indicate in ('insert','update')
                                                                        ''' % (self.__name,begin_date, end_date)
            print(sql_P1_Del)
            cur.execute(sql_P1_Del)

            # T01_Patient_H_Tmp 基础数据插入Tmp表
            sql2Tmp = '''
                INSERT into %s_tmp
                (MissionId
                ,ChargeOrderId
                ,UserId
                ,PatientId
                ,SaleClueId
                ,CityId
                ,BindPatientdate
                ,PayDateTime
                ,AiYaActualPrice
                ,MissionValue
                ,Percent
                ,PercentChanged
                ,Start_date
                ,InDw_timestamp
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                ifnull(a.Id,'') as MissionId
                ,ifnull(concat(b.name,LPAD(a.ChargeOrderId, 10, 0)),'') as ChargeOrderId
                ,ifnull(a.UserId,'') as UserId
                ,ifnull(concat(b.name,LPAD(a.PatientId, 10, 0)),'') as PatientId
                ,ifnull(a.SaleClueId,'') as SaleClueId
                ,ifnull(a.CityId,'') as CityId
                ,ifnull(DATE_FORMAT(a.BindPatientTime,'%%Y-%%m-%%d'),'1900-01-01') as BindPatientdate
                ,ifnull(DATE_FORMAT(a.PayDateTime,'%%Y-%%m-%%d'),'1900-01-01') as PayDateTime
                ,ifnull(a.ActualPrice,'') as AiYaActualPrice
                ,ifnull(a.MissionValue,'') as MissionValue
                ,ifnull(a.Percent,'') as Percent
                ,ifnull(a.PercentChanged,'') as PercentChanged,
                '%s'                                                                         as Start_date,
                now()                                                                        as InDw_timestamp,
                1                                                                            as Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                md5(a.ActualPrice)                                                                    as Contrast_Id,
                 '%s.BYBOSCHEDULEMONTHMISSIONDETAIL'          as SourceName
                from %s.byboschedulemonthmissiondetail a
                left join  bbkq_dw.t_code b on a.TenantId=b.code
                where b.codetype='Region'
                
                ''' % (self.__name, begin_date ,ods_cus, ods_cus)
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
            sql2Log = '''  insert into %s_Log
                (MissionId
                ,ChargeOrderId
                ,UserId
                ,PatientId
                ,SaleClueId
                ,CityId
                ,BindPatientdate
                ,PayDateTime
                ,AiYaActualPrice
                ,MissionValue
                ,Percent
                ,PercentChanged
                ,Start_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                ( select * from (
                select  a.MissionId
                ,a.ChargeOrderId
                ,a.UserId
                ,a.PatientId
                ,a.SaleClueId
                ,a.CityId
                ,a.BindPatientdate
                ,a.PayDateTime
                ,a.AiYaActualPrice
                ,a.MissionValue
                ,a.Percent
                ,a.PercentChanged
                ,a.Start_date
                ,a.InDw_timestamp
                ,'update' as Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_tmp a
                inner join %s b
                on b.End_date = '2099-01-01 00:00:00' and a.MissionId=b.MissionId and
                a.Contrast_Id != b.Contrast_Id
                where A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                union all
                select  a.MissionId
                ,a.ChargeOrderId
                ,a.UserId
                ,a.PatientId
                ,a.SaleClueId
                ,a.CityId
                ,a.BindPatientdate
                ,a.PayDateTime
                ,a.AiYaActualPrice
                ,a.MissionValue
                ,a.Percent
                ,a.PercentChanged
                ,a.Start_date
                ,a.InDw_timestamp
                ,'insert' as Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_tmp a
                where NOT EXISTS
                (SELECT *
                FROM %s b
                WHERE  b.End_date = '2099-01-01 00:00:00')
                and A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')) aa
)
                      ''' % (self.__name,self.__name,self.__name,begin_date,end_date,self.__name,self.__name,begin_date,end_date)
            print(sql2Log)
            cur.execute(sql2Log)

            # Part2 第一步数据写入根据update的数据将原有库里的对应数据的End_date置为InDw_timestamp；
            sql_P2_Update = '''
            update %s  a inner join  %s_log b  on a.MissionId=b.MissionId
                set a.End_date=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
            where b.Indicate = 'update'
            and B.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and B.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                        '''%(self.__name, self.__name, begin_date, begin_date, end_date)
            print(sql_P2_Update)
            cur.execute(sql_P2_Update)

            # Part2 第二步数据写入insert、update的数据的End_date都为'2099-01-01 00:00:00'；T01_Patient_H log表入最终表
            sql_P2_Write = '''
                insert into %s
                
                (MissionId
                ,ChargeOrderId
                ,UserId
                ,PatientId
                ,SaleClueId
                ,CityId
                ,BindPatientdate
                ,PayDateTime
                ,AiYaActualPrice
                ,MissionValue
                ,Percent
                ,PercentChanged
                ,Start_date
                ,End_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName
                )
                select  a.MissionId
                ,a.ChargeOrderId
                ,a.UserId
                ,a.PatientId
                ,a.SaleClueId
                ,a.CityId
                ,a.BindPatientdate
                ,a.PayDateTime
                ,a.AiYaActualPrice
                ,a.MissionValue
                ,a.Percent
                ,a.PercentChanged
                ,a.Start_date
                ,'2099-01-01 00:00:00' as End_date
                ,a.InDw_timestamp
                ,a.Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_log a
                where A.Indicate in ('insert', 'update')
                and A.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
          '''% (self.__name, self.__name,begin_date, end_date)
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
            self.addMessage(conn, '%s_Data2Dw.py'%(self.__name), jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':
    eg =T05_AiYa_Mission_H_Data2Dw()
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


    try:
        zst = time.time()
        # last_suceedate = eg.get_last_suceedate()
        # print(last_suceedate)
        F_or_N = eg.get_rowcount('T05_AiYa_Mission_H')
        print(F_or_N)
        if F_or_N > 0:
            eg.Draw_data(end_date, begin_date)
            # eg.Draw_data(end_date, begin_date)
        else:
            eg.Draw_data(begin_date, frist_data)
        # eg.Draw_data(end_date,begin_date)
        logger.info('【运行总时间】' + str(time.time() - zst)[:4])
    except Exception as e:
        logger.error("【运行异常】" + str(e))
