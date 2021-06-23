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


class T04_CardRights_Balance_H_Data2Dw(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name = 'T04_CardRights_Balance_H'
        self._taget_table = 'T04_CardRights_Balance_H'
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
                        insert into %s_Tmp
                        (CardId
                        ,RightsTotalAmount
                        ,RightsUsedAmount
                        ,RightsLeftAmount
                        ,RecordCreatedUser
                        ,RecordCreatedTime
                        ,RecordUpdatedTime
                        ,Start_date
                        ,InDw_timestamp
                        ,Partitions_ID
                        ,Contrast_Id
                        ,SourceName)
                        select 
                        ifnull(a.card_id,'') as CardId
                        ,ifnull(a.total_number,'0') as RightsTotalAmount
                        ,ifnull(a.used_number,'0') as RightsUsedAmount
                        ,ifnull(a.total_number-a.used_number,'0') as RightsLeftAmount
                        ,ifnull(a.created_by_name,'')       as RecordCreatedUser,
                        ifnull(a.created_time, '') as RecordCreatedTime,
                        ifnull(a.updated_time, a.created_time)     as RecordUpdatedTime,
                        ifnull(a.updated_time, '1900-00-00 00:00:00')   as Start_date,
                        now()                                              as InDw_timestamp,
                        '1'        as Partitions_ID, # 用时间做分区键
                        md5(a.card_id)                     as Contrast_Id,
                        'bbkq_ods_card.card_rights'          as SourceName
                        from %s.card_rights a
                        where 
                        ifnull(a.updated_time,a.created_time)
                        >=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
                        and ifnull(a.updated_time,a.created_time)
                        <=str_to_date(\'%s\','%%Y-%%m-%%d %%H:%%i:%%s')
           ''' % (self.__name, ods_card, begin_date, end_date)
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
                (CardId
                ,RightsTotalAmount
                ,RightsUsedAmount
                ,RightsLeftAmount
                ,RecordCreatedUser
                ,RecordCreatedTime
                ,RecordUpdatedTime
                ,Start_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                
                select 
                a.CardId
                ,a.RightsTotalAmount
                ,a.RightsUsedAmount
                ,a.RightsLeftAmount
                ,a.RecordCreatedUser
                ,a.RecordCreatedTime
                ,a.RecordUpdatedTime
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
                (CardId
                ,RightsTotalAmount
                ,RightsUsedAmount
                ,RightsLeftAmount
                ,RecordCreatedUser
                ,RecordCreatedTime
                ,RecordUpdatedTime
                ,Start_date
                ,End_date
                ,InDw_timestamp
                ,Indicate
                ,Partitions_ID
                ,Contrast_Id
                ,SourceName)
                select 
                a.CardId
                ,a.RightsTotalAmount
                ,a.RightsUsedAmount
                ,a.RightsLeftAmount
                ,a.RecordCreatedUser
                ,a.RecordCreatedTime
                ,a.RecordUpdatedTime
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

    eg = T04_CardRights_Balance_H_Data2Dw()
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
        F_or_N = eg.get_rowcount('T04_CardRights_Balance_H')
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
