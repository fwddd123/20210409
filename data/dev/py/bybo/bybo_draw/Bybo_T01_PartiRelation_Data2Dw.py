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


class Bybo_T01PartiRelation_Data2Dw(Bybo_base):
    def __init__(self):
        super().__init__()

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
        logger.info("Bybo_T01_PartiRelation_Data2Dw.py begintime=" + beginTime)

        try:
            # Part1 T01_Patient_H_Tmp 清空Tmp表
            sql_truncate = '''truncate table T01_PartiRelation_H_Tmp;'''
            print(sql_truncate)
            cur.execute(sql_truncate)

            # Part1 第一步数据回退;根据回退的时间将数据将End_date在回退时间内的数据重新改为2099-01-01 00:00:00;
            sql_P1_Rollback22099 = '''
                        update T01_PartiRelation_H a
                                    set A.End_date='2099-01-01 00:00:00'
                                    where A.End_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                        and A.End_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                            ''' % (begin_date, end_date)
            print(sql_P1_Rollback22099)
            cur.execute(sql_P1_Rollback22099)

            # Part1 第二步数据删除;在回退时间(Start_date)内的数据删除(类型为insert和update);
            sql_P1_Del = '''
                        delete from t01_PartiRelation_h
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Indicate in ('insert','update')
                                                                        ''' % (begin_date, end_date)
            print(sql_P1_Del)
            cur.execute(sql_P1_Del)

            # T01_Patient_H_Tmp 基础数据插入Tmp表
            sql2Tmp = '''
                        insert into T01_PartiRelation_H_Tmp
            (PatientID, RelationPartiId,Region,Id,OnlineConsultantId,
            RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,
            Start_date, InDw_timestamp, Partitions_ID, Contrast_Id, SourceName)
            select concat(a.Region, LPAD(Id, 10, 0))                as PatientID,
       concat(a.OnlineConsultantId, LPAD(Id, 10, 0))    as RelationPartiId,
       ifnull(a.Region, '')                             as Region,
       ifnull(Id, '')                                   as Id,
       ifnull(OnlineConsultantId, '')                   as OnlineConsultantId,
       ifnull(RecordCreatedUser, '')                    as RecordCreatedUser,
       ifnull(RecordCreatedTime, '1900-00-00 00:00:00') as RecordCreatedTime,
       ifnull(RecordUpdatedTime, RecordCreatedTime)     as RecordUpdatedTime,

       ifnull(RecordUpdatedTime, RecordCreatedTime)     as Start_date,
       now()                                            as InDw_timestamp,
       (a.Id DIV 100000) * 10                           as Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
       md5(concat(a.Region, '_', id))                   as Contrast_Id,
       'bbkq_ods_ekanya.patient'                        as SourceName
        from PATIENTEXTENSION a
                                   where  ifnull(RecordUpdatedTime, RecordCreatedTime)
                                   >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                   and ifnull(RecordUpdatedTime, RecordCreatedTime)
                                   <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                   ''' % (begin_date, end_date)
            print(sql2Tmp)
            cur.execute(sql2Tmp)

            sqlDel = '''
                        delete from T01_PartiRelation_H_Log
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                                        ''' % (begin_date, end_date)
            print(sqlDel)
            cur.execute(sqlDel)

            # T01_Patient_H_log 基础数据插入log表
            sql2Log = '''
                                                 insert into T01_PartiRelation_H_Log
                        (PatientID, RelationPartiId,Region,Id,OnlineConsultantId,
                        RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,
                        Start_date, InDw_timestamp, Indicate,Partitions_ID, Contrast_Id, SourceName)
            select a.PatientID,
            a.RelationPartiId,a.Region,a.Id,a.OnlineConsultantId,
            a.RecordCreatedUser,a.RecordCreatedTime,a.RecordUpdatedTime,

                   a.Start_date,
                   a.InDw_timestamp,
                   if(isnull(nullif(a.Contrast_Id, b.Contrast_Id)) = 0, 'insert', 'update') as Indicate,
                   a.Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                   a.Contrast_Id,
                   a.SourceName
            from T01_PartiRelation_H_Tmp a
                     left join t01_PartiRelation_h b
                               on a.Contrast_Id = b.Contrast_Id and b.End_date='2099-01-01 00:00:00'
                                   where A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                    and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                      ''' % (begin_date, end_date)
            print(sql2Log)
            cur.execute(sql2Log)

            # Part2 第一步数据写入根据update的数据将原有库里的对应数据的End_date置为InDw_timestamp；
            sql_P2_Update = '''
            update T01_PartiRelation_H a inner join  t01_PartiRelation_h_log b  on a.Contrast_Id = b.Contrast_Id
    set a.End_date=b.RecordUpdatedTime
where b.Indicate = 'update'
and B.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and B.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                        '''% (begin_date, end_date)
            print(sql_P2_Update)
            cur.execute(sql_P2_Update)

            # Part2 第二步数据写入insert、update的数据的End_date都为'2099-01-01 00:00:00'；T01_Patient_H log表入最终表
            sql_P2_Write = '''
            insert into T01_PartiRelation_H
            (PatientID, RelationPartiId,Region,Id,OnlineConsultantId,
                        RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,
             Start_date,End_date,InDw_timestamp, Indicate,Partitions_ID, Contrast_Id, SourceName)
select a.PatientID,
            a.RelationPartiId,a.Region,a.Id,a.OnlineConsultantId,
            a.RecordCreatedUser,a.RecordCreatedTime,a.RecordUpdatedTime, 

       a.Start_date,
       '2099-01-01 00:00:00' as End_date,
       a.InDw_timestamp,
       a.Indicate,
       a.Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
       a.Contrast_Id,
       a.SourceName
from t01_PartiRelation_h_log a
where A.Indicate in ('insert','update')
and A.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
          '''% (begin_date, end_date)
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
            self.addMessage(conn, 'Bybo_T01_PartiRelation_Data2Dw.py ', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = Bybo_T01PartiRelation_Data2Dw()
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
        F_or_N = eg.get_rowcount('T01_PartiRelation_H')
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