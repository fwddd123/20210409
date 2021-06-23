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


class Bybo_T02Appointment_Data2Dw(Bybo_base):
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
        logger.info("Bybo_T02_Appointment_Data2Dw.py begintime=" + beginTime)
        ods_crm = self.get_property_value('ods_crm')
        ods_ekanya = self.get_property_value('ods_ekanya')


        try:
            # Part1 T01_Patient_H_Tmp 清空Tmp表
            sql_truncate = '''truncate table T02_Appointment_H_Tmp;'''
            print(sql_truncate)
            cur.execute(sql_truncate)

            # Part1 第一步数据回退;根据回退的时间将数据将End_date在回退时间内的数据重新改为2099-01-01 00:00:00;
            sql_P1_Rollback22099 = '''
                        update T02_Appointment_H a
                                    set A.End_date='2099-01-01 00:00:00'
                                    where A.End_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                        and A.End_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                            ''' % (begin_date, end_date)
            print(sql_P1_Rollback22099)
            cur.execute(sql_P1_Rollback22099)

            # Part1 第二步数据删除;在回退时间(Start_date)内的数据删除(类型为insert和update);
            sql_P1_Del = '''
                        delete from T02_Appointment_H
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Indicate in ('insert','update')
                                                                        ''' % (begin_date, end_date)
            print(sql_P1_Del)
            cur.execute(sql_P1_Del)

            # T01_Patient_H_Tmp 基础数据插入Tmp表
            sql2Tmp = '''
                        insert into T02_Appointment_H_Tmp
            (AppointmentId,TransactionId,PatientId,DoctorId,AssistantId,ConsultantId,OfficeId,FromApptId,
            ConsultItem,CheckInType,AppointmentType,IsCancelled,IsConfirmed,IsCheckedIn,IsSeated,
            IsCompleted,IsCheckedOut,IsFailed,HasTransferred,IsFirstVisit,IsPending,IsFollowuped,
            IsLeft,IsConsulting,IsRemindExcluded,
            StartDate,StartTime,EndTime,RecordCreatedDate,CheckInTime,Id,Region,IsInactive,
            RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,Start_date, InDw_timestamp, 
            Partitions_ID, Contrast_Id, SourceName)
            select concat(a.Region, LPAD(a.Id, 10, 0))                       as AppointmentId,
           ifnull(b.TransactionId, '')                               as TransactionId,
           ifnull(concat(a.Region, LPAD(a.PatientId, 10, 0)), '')    as PatientId,
           ifnull(concat(a.Region, LPAD(a.DoctorId, 10, 0)), '')     as DoctorId,
           ifnull(concat(a.Region, LPAD(a.AssistantId, 10, 0)), '')  as AssistantId,
           ifnull(concat(a.Region, LPAD(a.ConsultantId, 10, 0)), '') as ConsultantId,
           ifnull(concat(a.Region, LPAD(a.OfficeId, 10, 0)), '')     as OfficeId,
           ifnull(concat(a.Region, LPAD(a.FromApptId, 10, 0)), '')   as FromApptId,
    
           ifnull(a.ConsultItem, '')                                 as ConsultItem,
           ifnull(a.CheckInType, '')                                 as CheckInType,
           ifnull(a.AppointmentType, '')                             as AppointmentType,
    
           case a.IsCancelled when 0 then '1' else '0' end           as IsCancelled,
           case a.IsConfirmed when 0 then '1' else '0' end           as IsConfirmed,
           case a.IsCheckedIn when 0 then '1' else '0' end           as IsCheckedIn,
           case a.IsSeated when 0 then '1' else '0' end              as IsSeated,
           case a.IsCompleted when 0 then '1' else '0' end           as IsCompleted,
           case a.IsCheckedOut when 0 then '1' else '0' end          as IsCheckedOut,
           case a.IsFailed when 0 then '1' else '0' end              as IsFailed,
           case a.HasTransferred when 0 then '1' else '0' end        as HasTransferred,
           case a.IsFirstVisit when 0 then '1' else '0' end          as IsFirstVisit,
           case a.IsPending when 0 then '1' else '0' end             as IsPending,
           case a.IsFollowuped when 0 then '1' else '0' end          as IsFollowuped,
           case a.IsLeft when 0 then '1' else '0' end                as IsLeft,
           case a.IsConsulting when 0 then '1' else '0' end          as IsConsulting,
           case a.IsRemindExcluded when 0 then '1' else '0' end      as IsRemindExcluded,
    
           date_format(a.StartTime,'%%Y-%%m-%%d') as StartDate,
           ifnull(a.StartTime, '1900-00-00 00:00:00') as StartTime,
           ifnull(a.EndTime, '1900-00-00 00:00:00') as EndTime,
           ifnull(a.RecordCreatedTime, '1900-00-00 00:00:00')        as RecordCreatedDate,
           '1900-00-00 00:00:00'                                     as CheckInTime,
           ifnull(a.Id, '')                                          as Id,
           ifnull(a.Region, '')                                      as Region,
           case a.IsInactive when 0 then '1' else '0' end            as IsInactive,
                   
                   ifnull(a.RecordCreatedUser, '')                    as RecordCreatedUser,
                   ifnull(a.RecordCreatedTime, '1900-00-00 00:00:00') as RecordCreatedTime,
                   ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)     as RecordUpdatedTime,
                   ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)          as Start_date,
                   now()                                                     as InDw_timestamp,
                   (a.Id DIV 100000) * 10                                    as Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                   md5(concat(a.Region, '_', a.id))                          as Contrast_Id,
                   'bbkq_ods_ekanya.APPOINTMENT'                             as SourceName

from APPOINTMENT a
         left join appointmentprocedure b
                   on a.Id = b.AppointmentId and a.Region = b.Region
                                   where  ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)
                                   >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                   and ifnull(a.RecordUpdatedTime, a.RecordCreatedTime)
                                   <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                   ''' % (begin_date, end_date)
            print(sql2Tmp)
            cur.execute(sql2Tmp)

            sqlDel = '''
                        delete from T02_Appointment_H_Log
                         where Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                         and Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                                                        ''' % (begin_date, end_date)
            print(sqlDel)
            cur.execute(sqlDel)

            # T01_Patient_H_log 基础数据插入log表
            sql2Log = '''
                                                 insert into T02_Appointment_H_Log
                        (AppointmentId,TransactionId,PatientId,DoctorId,AssistantId,ConsultantId,OfficeId,FromApptId,
            ConsultItem,CheckInType,AppointmentType,IsCancelled,IsConfirmed,IsCheckedIn,IsSeated,
            IsCompleted,IsCheckedOut,IsFailed,HasTransferred,IsFirstVisit,IsPending,IsFollowuped,
            IsLeft,IsConsulting,IsRemindExcluded,
            StartDate,StartTime,EndTime,RecordCreatedDate,CheckInTime,Id,Region,IsInactive,
            RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,Start_date, InDw_timestamp, Indicate,
            Partitions_ID, Contrast_Id, SourceName)
            select 
            a.AppointmentId,a.TransactionId,a.PatientId,a.DoctorId,a.AssistantId,a.ConsultantId,a.OfficeId,a.FromApptId,
            a.ConsultItem,a.CheckInType,a.AppointmentType,a.IsCancelled,a.IsConfirmed,a.IsCheckedIn,a.IsSeated,
            a.IsCompleted,a.IsCheckedOut,a.IsFailed,a.HasTransferred,a.IsFirstVisit,a.IsPending,a.IsFollowuped,
            a.IsLeft,a.IsConsulting,a.IsRemindExcluded,
            a.StartDate,a.StartTime,a.EndTime,a.RecordCreatedDate,a.CheckInTime,a.Id,a.Region,a.IsInactive,
            a.RecordCreatedUser,a.RecordCreatedTime,a.RecordUpdatedTime,

                   a.Start_date,
                   a.InDw_timestamp,
                   if(isnull(nullif(a.Contrast_Id, b.Contrast_Id)) = 0, 'insert', 'update') as Indicate,
                   a.Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                   a.Contrast_Id,
                   a.SourceName
            from T02_Appointment_H_Tmp a
                     left join T02_Appointment_h b
                               on a.Contrast_Id = b.Contrast_Id and b.End_date='2099-01-01 00:00:00'
                                   where A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                                    and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                      ''' % (begin_date, end_date)
            print(sql2Log)
            cur.execute(sql2Log)

            # Part2 第一步数据写入根据update的数据将原有库里的对应数据的End_date置为InDw_timestamp；
            sql_P2_Update = '''
            update T02_Appointment_H a inner join  T02_Appointment_h_log b  on a.Contrast_Id = b.Contrast_Id
    set a.End_date=b.RecordUpdatedTime
where b.Indicate = 'update'
and B.Start_date >= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and B.Start_date <= str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                        '''% (begin_date, end_date)
            print(sql_P2_Update)
            cur.execute(sql_P2_Update)

            # Part2 第二步数据写入insert、update的数据的End_date都为'2099-01-01 00:00:00'；T01_Patient_H log表入最终表
            sql_P2_Write = '''
            insert into T02_Appointment_H
            (AppointmentId,TransactionId,PatientId,DoctorId,AssistantId,ConsultantId,OfficeId,FromApptId,
            ConsultItem,CheckInType,AppointmentType,IsCancelled,IsConfirmed,IsCheckedIn,IsSeated,
            IsCompleted,IsCheckedOut,IsFailed,HasTransferred,IsFirstVisit,IsPending,IsFollowuped,
            IsLeft,IsConsulting,IsRemindExcluded,
            StartDate,StartTime,EndTime,RecordCreatedDate,CheckInTime,Id,Region,IsInactive,
             RecordCreatedUser,RecordCreatedTime,RecordUpdatedTime,Start_date,End_date,InDw_timestamp, Indicate,
             Partitions_ID, Contrast_Id, SourceName)
select 
            a.AppointmentId,a.TransactionId,a.PatientId,a.DoctorId,a.AssistantId,a.ConsultantId,a.OfficeId,a.FromApptId,
            a.ConsultItem,a.CheckInType,a.AppointmentType,a.IsCancelled,a.IsConfirmed,a.IsCheckedIn,a.IsSeated,
            a.IsCompleted,a.IsCheckedOut,a.IsFailed,a.HasTransferred,a.IsFirstVisit,a.IsPending,a.IsFollowuped,
            a.IsLeft,a.IsConsulting,a.IsRemindExcluded,
            a.StartDate,a.StartTime,a.EndTime,a.RecordCreatedDate,a.CheckInTime,a.Id,a.Region,a.IsInactive,
            a.RecordCreatedUser,a.RecordCreatedTime,a.RecordUpdatedTime,

            a.Start_date,
            '2099-01-01 00:00:00' as End_date,
            a.InDw_timestamp,
            a.Indicate,
            a.Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
            a.Contrast_Id,
            a.SourceName
from T02_Appointment_h_log a
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
            self.addMessage(conn, 'Bybo_T02_Appointment_Data2Dw.py ', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = Bybo_T02Appointment_Data2Dw()
    logger = eg.logger
    logger.info("任务开始")
    stF = "%Y-%m-%d %H:%M:%S"
    # 默认为前一天
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF)
    end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF)

    frist_data = '2020-01-01 00:00:00'
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
        F_or_N = eg.get_rowcount('T02_Appointment_H')
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
