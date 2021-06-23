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


class T01_Mall_Member_H_Data2Dw(Bybo_base):
    def __init__(self):
        super().__init__()
        self._taget_table='T01_Mall_Member_H'
        self.__name='T01_Mall_Member_H'


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
                insert into %s_tmp
                (MemberId
                ,MemberName
                ,MemberNickname
                ,MemberCode
                ,MemberRealName
                ,PatientId
                ,Relationship
                ,MemberGradeValue
                ,Mobile
                ,Phone
                ,MemberLevel
                ,Sex
                ,Source
                ,MemberType
                ,Status
                ,RegisterDate
                ,RegisterTime
                ,UpdateTime
                ,CustomerId
                ,IntroducerCustomerId
                ,TshUserId
                ,TkzxUserId
                ,Start_date
                ,InDw_timestamp
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                ifnull(a.id,'') as MemberId
                ,ifnull(a.name,'') as MemberName
                ,ifnull(a.nickname,'') as MemberNickname
                ,ifnull(a.member_code,'') as MemberCode
                ,ifnull(a.real_name,'') as MemberRealName
                ,ifnull(concat(c.name,LPAD(b.patient_id, 10, 0)),'') as PatientId
                ,ifnull(b.relationship,'') as Relationship
                ,ifnull(a.grade_value,'0') as MemberGradeValue
                ,ifnull(a.mobile,'') as Mobile
                ,ifnull(a.phone,'') as Phone
                ,ifnull(a.grade,'') as MemberLevel
                ,ifnull(a.gender,'') as Sex
                ,ifnull(a.source,'') as Source
                ,ifnull(a.member_other_type,'') as MemberType
                ,ifnull(a.status,'') as Status
                ,ifnull(DATE_FORMAT(a.register_time,'%%Y-%%m-%%d'),'') as RegisterDate
                ,ifnull(a.register_time,'1900-01-01 00:00:00') as RegisterTime
                ,ifnull(a.update_time,'1900-01-01 00:00:00') as UpdateTime
                ,ifnull(a.customer_id,'') as CustomerId
                ,ifnull(a.introducer_customer_id,'') as IntroducerCustomerId
                ,ifnull(a.tsh_user_id,'') as TshUserId
                ,ifnull(a.tkzx_user_id,'') as TkzxUserId,
                '%s'                                                                         as Start_date,
                now()                                                                        as InDw_timestamp,
                1                                                                            as Partitions_ID, # 分区键取ID除以10W加上区域数字乘以1W
                md5(a.status)                                                                    as Contrast_Id,
                'gxkf_bybomall_youfan2mall.member'                                                as SourceName
                from gxkf_bybomall_youfan2mall.MEMBER a 
                left join   gxkf_bybomall_youfan2mall.DT_MEMBER_ETOOTH b on a.customer_id=b.customer_id
                left join  bbkq_dw.T_Code c on b.tenant_id=c.code
                ''' % (self.__name,begin_date)
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
                (MemberId
                ,MemberName
                ,MemberNickname
                ,MemberCode
                ,MemberRealName
                ,PatientId
                ,Relationship
                ,MemberGradeValue
                ,Mobile
                ,Phone
                ,MemberLevel
                ,Sex
                ,Source
                ,MemberType
                ,Status
                ,RegisterDate
                ,RegisterTime
                ,UpdateTime
                ,CustomerId
                ,IntroducerCustomerId
                ,TshUserId
                ,TkzxUserId
                ,Start_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                ( select * from (
                select a.MemberId
                ,a.MemberName
                ,a.MemberNickname
                ,a.MemberCode
                ,a.MemberRealName
                ,a.PatientId
                ,a.Relationship
                ,a.MemberGradeValue
                ,a.Mobile
                ,a.Phone
                ,a.MemberLevel
                ,a.Sex
                ,a.Source
                ,a.MemberType
                ,a.Status
                ,a.RegisterDate
                ,a.RegisterTime
                ,a.UpdateTime
                ,a.CustomerId
                ,a.IntroducerCustomerId
                ,a.TshUserId
                ,a.TkzxUserId
                ,a.Start_date
                ,a.InDw_timestamp
                ,'update' as Indicate
                ,a.Partitions_ID
                ,a.Contrast_Id
                ,a.SourceName
                from %s_tmp a
                inner join %s b
                on b.End_date = '2099-01-01 00:00:00' and a.MemberId=b.MemberId and
                a.Contrast_Id != b.Contrast_Id
                where A.Start_date >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                and A.Start_date <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                union all
                select a.MemberId
                ,a.MemberName
                ,a.MemberNickname
                ,a.MemberCode
                ,a.MemberRealName
                ,a.PatientId
                ,a.Relationship
                ,a.MemberGradeValue
                ,a.Mobile
                ,a.Phone
                ,a.MemberLevel
                ,a.Sex
                ,a.Source
                ,a.MemberType
                ,a.Status
                ,a.RegisterDate
                ,a.RegisterTime
                ,a.UpdateTime
                ,a.CustomerId
                ,a.IntroducerCustomerId
                ,a.TshUserId
                ,a.TkzxUserId
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
            update %s  a inner join  %s_log b  on a.MemberId=b.MemberId
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
                
                (MemberId
                ,MemberName
                ,MemberNickname
                ,MemberCode
                ,MemberRealName
                ,PatientId
                ,Relationship
                ,MemberGradeValue
                ,Mobile
                ,Phone
                ,MemberLevel
                ,Sex
                ,Source
                ,MemberType
                ,Status
                ,RegisterDate
                ,RegisterTime
                ,UpdateTime
                ,CustomerId
                ,IntroducerCustomerId
                ,TshUserId
                ,TkzxUserId
                ,Start_date
                ,End_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName
                )
                select a.MemberId
                ,a.MemberName
                ,a.MemberNickname
                ,a.MemberCode
                ,a.MemberRealName
                ,a.PatientId
                ,a.Relationship
                ,a.MemberGradeValue
                ,a.Mobile
                ,a.Phone
                ,a.MemberLevel
                ,a.Sex
                ,a.Source
                ,a.MemberType
                ,a.Status
                ,a.RegisterDate
                ,a.RegisterTime
                ,a.UpdateTime
                ,a.CustomerId
                ,a.IntroducerCustomerId
                ,a.TshUserId
                ,a.TkzxUserId
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
            self.addMessage(conn, 'T01_Mall_Member_H_Data2Dw.py ', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)
            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T01_Mall_Member_H_Data2Dw()
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
        F_or_N = eg.get_rowcount('T01_Mall_Member_H')
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
