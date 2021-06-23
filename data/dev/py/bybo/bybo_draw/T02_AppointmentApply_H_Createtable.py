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
文件名称: Bybo_T01_Patient_Createtable. py
描述: 数据整合，具体操作类
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''


class T02_AppointmentApply_H_Createtable(Bybo_base):
    def __init__(self):
        super().__init__()
        self.__name = 'T02_AppointmentApply_H'
        self._taget_table = 'T02_AppointmentApply_H'

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
        logger.info("T02_AppointmentApply_H_Createtable.py begintime=" + beginTime)

        try:
            # T01_Patient_H DW目标表
            sql_T02_Appointment_H = '''
                         CREATE TABLE IF NOT EXISTS T02_APPOINTMENTAPPLY_H
(
 AppointmentApplyId	    varchar(50)	         NOT NULL	DEFAULT ''	COMMENT	'临床诊疗预约申请编号',
AppointmentId	varchar(50)	NOT NULL	DEFAULT 	 ''	COMMENT	'预约编码',
OfficeId	varchar(50)	    NOT NULL	DEFAULT 	  ' '	COMMENT	'预约申请治疗门店编号',
PatientId	varchar(50)	NOT NULL	DEFAULT 	 ''	COMMENT	'临床诊疗预约客户编号',
ProcedureNames	varchar(200)	NOT NULL	DEFAULT 	 ''	COMMENT	'预约项目名称',
ProcessStatus	varchar(200)	NOT NULL	DEFAULT 	  ''	COMMENT	'预约申请处理状态',
	StartDate	date NOT NULL	DEFAULT 	 '1900-01-01'	COMMENT	'预约申请治疗日期',
StartTime	datetime	NOT NULL	DEFAULT   '1900-01-01 00:00:00' COMMENT	'预约申请治疗开始时间',
EndTime	datetime	NOT NULL	DEFAULT 	'2099-01-01 00:00:00'	COMMENT	'预约申请治疗结束时间' ,
IsInactive	varchar(10)  NOT NULL	DEFAULT    ''	COMMENT	'有效标识',

    RecordCreatedUser  varchar(50) NOT NULL DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
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
            cur.execute(sql_T02_Appointment_H)



            # T01_Patient_H T01_Patient_H 临时表
            sql_T02_Appointment_H_Tmp = '''
            CREATE TABLE IF NOT EXISTS T02_APPOINTMENTAPPLY_H_Tmp
(
  AppointmentApplyId	           varchar(50)	         NOT NULL	DEFAULT 	 ''	COMMENT	'临床诊疗预约申请编号',
AppointmentId	varchar(50)	NOT NULL	DEFAULT 	 ''	COMMENT	'预约编码',
OfficeId	varchar(50)	    NOT NULL	DEFAULT 	  ''	COMMENT	'预约申请治疗门店编号',
PatientId	varchar(50)	NOT NULL	DEFAULT 	 ' '	COMMENT	'临床诊疗预约客户编号',
ProcedureNames	varchar(200)	NOT NULL	DEFAULT 	 ' '	COMMENT	'预约项目名称',
ProcessStatus	varchar(200)	NOT NULL	DEFAULT 	  ' '	COMMENT	'预约申请处理状态',
StartDate	date	 NOT NULL DEFAULT 	 '1900-01-01'	COMMENT	'预约申请治疗日期',
StartTime	datetime	NOT NULL	DEFAULT  '1900-01-01 00:00:00'	COMMENT	'预约申请治疗开始时间',
EndTime	datetime	NOT NULL	DEFAULT 	'2099-01-01 00:00:00'	COMMENT	'预约申请治疗结束时间',
IsInactive	varchar(10)  NOT NULL	DEFAULT    ''	COMMENT	'有效标识',

    RecordCreatedUser  varchar(50) NOT NULL DEFAULT '' COMMENT '记录创建人编号',
    RecordCreatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    RecordUpdatedTime  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    Start_date datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
    InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
    Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
    Contrast_Id varchar(32)  NOT NULL DEFAULT '' COMMENT '比对键',
    SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
    ) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
            '''
            cur.execute(sql_T02_Appointment_H_Tmp)

            # T01_Patient_H T01_Patient_H log表
            sql_T02_Appointment_H_Log = '''
                        CREATE TABLE IF NOT EXISTS T02_APPOINTMENTAPPLY_H_Log
            (
 AppointmentApplyId          varchar(50)	         NOT NULL	DEFAULT 	 ''	COMMENT	'临床诊疗预约申请编号',
AppointmentId	varchar(50)	NOT NULL	DEFAULT 	 ''	COMMENT	'预约编码',
OfficeId	varchar(50)	    NOT NULL	DEFAULT 	  ''	COMMENT	'预约申请治疗门店编号',
PatientId	varchar(50)	NOT NULL	DEFAULT 	 ''	COMMENT	'临床诊疗预约客户编号',
ProcedureNames	varchar(200)	NOT NULL	DEFAULT 	 ''	COMMENT	'预约项目名称',
ProcessStatus	varchar(200)	NOT NULL	DEFAULT 	  ''	COMMENT	'预约申请处理状态',
StartDate	date	 NOT NULL	DEFAULT 	 '1900-01-01'	COMMENT	'预约申请治疗日期',
StartTime	datetime	NOT NULL	DEFAULT  '1900-01-01 00:00:00'	COMMENT	'预约申请治疗开始时间',
EndTime	datetime	NOT NULL	DEFAULT 	'2099-01-01 00:00:00'	COMMENT	'预约申请治疗结束时间' ,
IsInactive	varchar(10)  NOT NULL	DEFAULT    ''	COMMENT	'有效标识',

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
            cur.execute(sql_T02_Appointment_H_Log)


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
            self.addMessage(conn, 'Bybo_T02_Appointment_Createtable.py', jobMsg, jobSt, starttime.strftime(stF),
                            endtime.strftime(stF), sub)

            cur.close()
            conn.close()


if __name__ == '__main__':
    eg = T02_AppointmentApply_H_Createtable()
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
