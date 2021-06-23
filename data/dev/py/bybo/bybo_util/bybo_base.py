#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime
import os

sysplat = sys.platform
# 基类所在目录
commDri = '/data/dev/py/util'
if 'win32' in sysplat.lower():
    commonpath = sys.path[0]
    commDri = commonpath.split('\\bybo\\')[0] + '\\util'

if 'darwin' in sysplat.lower():
    commonpath = sys.path[0]
    commDri = commonpath.split('/bybo/')[0] + '/util'

sys.path.append(commDri)

from BaseModule import base

'''
项目名称: 数据整合  大数据平台-泰康之家
文件名称: tkzj_base.py 
描述:封装通用的方法，每个板块有一个基类，和板块同目录，该程序只是示例，各个板块封装自己的，总体的原则是方便板块使用,
  module_name = 'tkzj_util' #该处需要修改成自己板块的名称
创建时间: 2019年11月11日15:56:40  
@author czf
@version v1.0
@update 2019年11月12日15:44:40 
'''

class Bybo_base(base):
    modInfo = None
    busInfo = None
    dbdir = None
    dbType = '1'

    def __init__(self):
        super().__init__()
        # 调用父类默认读取配置文件方法
        # super().init()

        # 调用自定义方法，读取配置文件方法
        self.__sel_init()

    '''
    @Description: 获得数据库连接
    @Param: dbdir 数据库配置文件信息
    描述: 参数不传就会读取相应的默认值
    @Return: 数据库连接对象
    @author czf
    @CreateDate: 2019年11月11日14:37:08
    @update: 2019年11月11日14:37:16
       '''

    def __sel_init(self):
        # 自定义读取配置文件方法
        # paramDri = commonpath.split('/pyWork/')[0] + '/param'
        # print(paramDri)
        if 'darwin' in sysplat.lower():
            module_name = 'bybo'  # 该处需要修改成自己板块的名称
            paramDri = commonpath.split('/bybo/')[0] + '/param'

        if 'win32' in sysplat.lower():
            module_name = 'bybo'  # 该处需要修改成自己板块的名称
            paramDri = commonpath.split('\\bybo\\')[0] +  '\\param'

        if 'linux' in sysplat.lower():
            module_name = 'bybo'  # 该处需要修改成自己板块的名称
            paramDri = commDri.split('/util')[0] + '/param'

        path = os.path.join(paramDri, module_name)
        self.modInfo = os.path.join(path, 'module_path_info.config')
        self.busInfo = os.path.join(path, 'business_info.config')
        self.dbdir = os.path.join(path, 'db_connect_info.ini')
        super().init(modInfo=self.modInfo, busInfo=self.busInfo)

    '''
@Description: 获得数据库连接
@Param: dbdir 数据库配置文件信息
描述: 参数不传就会读取相应的默认值
@Return: 数据库连接对象
@author czf
@CreateDate: 2019年11月11日14:37:08
@update: 2019年11月11日14:37:16
    '''

    def get_connect(self):
        # print('getConnect')
        logger = self.logger
        # 数据库默认配置
        # conn = super().getConnect( dbdir=None, dbType=None)

        # 自配置连接
        if self.dbdir != None:
            logger.info('self.dbdir=' + self.dbdir)
        # conn = base.getConnect(self, dbdir=self.dbdir, dbType=self.dbType)
        conn = super().getConnect(dbdir=self.dbdir, dbType=self.dbType)
        return conn

    '''
   @Description: 获得加载的配置信息
   @Param: propretyName 属性名称
   描述: 参数不传就会返回None，找不到也返回None 
   @Return: 属性值
   @author czf
   @CreateDate: 2019年11月11日14:37:08 
   @update: 2019年11月11日14:37:16

    '''

    def get_property_value(self, property_name):
        property_value = None
        # propertyValue = base.getPropertyValue(self, propertyName)
        property_value = super().get_property_value(property_name)
        return property_value

    def get_last_suceedate(self,table):
        conn = super().getConnect(dbdir=self.dbdir, dbType=self.dbType)
        cur = conn.cursor()
        # cur = super().getConnect(dbdir=self.dbdir, dbType=self.dbType).cursor()
        sql = "select max(job_start_time) from etl_info.message where job_name='%s'" % (table)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return res[0][0]

    def get_rowcount(self,table):
        conn = super().getConnect(dbdir=self.dbdir, dbType=self.dbType)
        cur = conn.cursor()
        # cur = super().getConnect(dbdir=self.dbdir, dbType=self.dbType).cursor()
        sql = "select count(1) from %s " % (table)
        cur.execute(sql)
        res = cur.fetchall()
        cur.close()
        conn.close()
        return res[0][0]

    '''
    @Description: 程序运行入口
    @Return: 无
    @author czf
    @CreateDate: 2020年04月27日15:56:40
    @update: 2020年04月27日15:56:40
    '''


if __name__ == '__main__':
    obj = Bybo_base()



    # print(obj.get_property_value('ISFULL'))
    # print(obj.get_property_value('days'))
    # print(obj.get_property_value('Target_DataBase'))

