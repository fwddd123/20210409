import sys
import pymysql
from warnings import filterwarnings
import os
import traceback
from  Common import CommonUtil
from  DB_Enum import DatabasesEnum
from  MySqlConnect import MyConnect
import re

filterwarnings("ignore", category=pymysql.Warning)

'''
项目名称: 业务基础类 
文件名称: BaseModule.py 
描述: 业务基础类，初始化一些公共功能和属性 
创建时间: 2019年11月11日14:22:40 
@author chenxinlei
@version v1.0
@update 2019年11月11日14:22:40 
'''


class base(object):
    comm = None
    logger = None
    commDri = '/data/dev/py/param'
    base_modInfo = None
    base_busInfo = None
    dbInfo = None
    mcnfInf = {}
    bcnfInf = {}

    def __init__(self):
        sysplat = sys.platform
        if 'win' in sysplat.lower():
            path = os.path.abspath(__file__)
            self.commDri = path.split('\\pyWork\\work\\')[0] + '\\pyWork\\work\\param'
        self.base_modInfo = self.commDri + '/module_path_info.config'
        self.base_busInfo = self.commDri + '/business_info.config'
        self.dbInfo = self.commDri + '/db_connect_info.ini'

    '''
    @Description: 初始化功能
    @Param: modInfo 引入模块配置文件信息路径
    @Param: busInfo 业务配置文件信息路径
    描述: 参数不传就会读取相应的默认值 
    @Return: 无
    @author chenxinlei
    @CreateDate: 2019年11月11日14:37:08 
    @update: 2019年11月11日14:37:16
    '''

    def init(self, dbInfo=None, modInfo=None, busInfo=None):
        if dbInfo is not None:
            self.dbInfo = dbInfo
        if self.isStr(modInfo):
            modInfo = self.base_modInfo
        self.mcnfInf = CommonUtil.getProperties(modInfo)
        if self.isStr(busInfo):
            busInfo = self.base_busInfo
        self.bcnfInf = CommonUtil.getProperties(busInfo)
        base_dir_conn = self.mcnfInf.get('my_sql_connect')
        logName = self.bcnfInf.get('log_name')
        sysplat = sys.platform
        if 'win' in sysplat.lower():
            self.logger = CommonUtil.getLoggerForWin(logName)
        else:
            self.logger = CommonUtil.getLogger(logName)

        sys.path.append(base_dir_conn)
        self.comm = CommonUtil

    '''
   @Description: 获得数据库连接
   @Param: dbInfo 数据库配置文件信息路径
   描述: 参数不传就会读取相应的默认值 
   @Return: 数据库连接对象
   @author chenxinlei
   @CreateDate: 2019年11月11日14:37:08 
   @update: 2019年11月11日14:37:16
   '''

    def getConnect(self, dbdir=None, dbType=None):
        # 数据库默认配置
        if dbdir is None:
            dbdir = self.dbInfo

        conn = MyConnect.getConnect(self.comm, self.logger, dbdir, dbType)

        return conn

    '''
   @Description: 增加模块执行信息
   @Param: conn 数据库连接
   @Param: modName 作业的模块名称
   @Param: msg 记录的信息
   描述: 可以记录异常信息和其他的信息 
   @Return: 无
   @author chenxinlei
   @CreateDate: 2019年11月11日14:37:08 
   @update: 2019年11月11日14:37:16
   '''

    def addMessage(self, conn, modName, msg, jobStatus, jobStart, jobEnd, jobUse):
        localconn = None
        cur = None
        msg=re.sub('[\"\'\n]','',msg).strip()
        try:
            # localconn = base.getConnect(self)
            # cur = localconn.cursor()etl_info
            cur = conn.cursor()
            ctt = '''
              CREATE TABLE IF NOT EXISTS etl_info.message (
            	ms_id CHAR (36) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '业务主键',
            	job_name VARCHAR (50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '脚本文件名称',
                job_status INT (2) NULL DEFAULT NULL COMMENT '是否成功',
                ms_content VARCHAR (2048) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '信息内容',
                job_start_time datetime NULL DEFAULT NULL COMMENT '作业开始时间',
                job_end_time datetime NULL DEFAULT NULL COMMENT '作业结束时间',
                job_used_times INT (11) NULL DEFAULT NULL COMMENT '作业用时',
                sms_send_status INT (2) NULL DEFAULT NULL COMMENT '发短信标识：0-未发送，1-发送成功，2-发送失败',
                email_send_status INT (2) NULL DEFAULT NULL COMMENT 'email标识：0-未发送，1-发送成功，2-发送失败',
                wechat_send_status INT (2) NULL DEFAULT NULL COMMENT '微信标识：0-未发送，1-发送成功，2-发送失败',
                send_sms_time datetime NULL DEFAULT NULL COMMENT '短信发送时间',
                send_email_time datetime NULL DEFAULT NULL COMMENT '邮件发送时间',
                send_wechat_time datetime NULL DEFAULT NULL COMMENT '微信发送时间',
                create_time datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                PRIMARY KEY (ms_id)
            ) ENGINE = INNODB DEFAULT CHARACTER
            SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;
            '''
            cur.execute(ctt)
            # localconn.commit()
            conn.commit()
            sql = "INSERT INTO etl_info.message (ms_id,job_name,ms_content,job_status,job_start_time,job_end_time,job_used_times,sms_send_status,email_send_status,wechat_send_status) VALUES(UUID(),'%s','%s',%s,'%s','%s',%s,0,0,0)" % (
                modName, msg, jobStatus, jobStart, jobEnd, jobUse)
            rst = cur.execute(sql)
        except Exception as e:
            self.logger.error("增加msg出错" + str(e))

            #self.logger.error(traceback.format_exc())
            msg='失败'

            sql = "INSERT INTO etl_info.message (ms_id,job_name,ms_content,job_status,job_start_time,job_end_time,job_used_times,sms_send_status,email_send_status,wechat_send_status) VALUES(UUID(),'%s','%s',%s,'%s','%s',%s,0,0,0)" % (
                modName, msg, jobStatus, jobStart, jobEnd, jobUse)
            rst = cur.execute(sql)
        finally:
            conn.commit()




    def isStr(self, s):
        if s == None:
            return True
        elif len(s.strip()) == 0:
            return True

    '''
  @Description: 获得加载的配置信息
  @Param: propretyName 属性名称
  描述: 参数不传就会返回None，找不到也返回None 
  @Return: 属性值
  @author czf
  @CreateDate: 2019年11月11日14:37:08 
  @update: 2019年11月11日14:37:16
   '''

    def get_property_value(self, proprety_name=None):

        if proprety_name is None:
            return None
        propertyValue = None
        propertyValue = self.bcnfInf.get(proprety_name)
        return propertyValue
