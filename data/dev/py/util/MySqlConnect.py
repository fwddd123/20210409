#!/usr/bin/python3
# coding=utf-8
'''
项目名称: 数据库连接
文件名称: BaseModule.py
描述: 数据库连接
创建时间: 2019年11月11日14:22:40
@author chenxinlei
@version v1.0
@update 2019年11月11日14:22:40
'''
import pymysql
import time
import configparser

'''
项目名称: 业务基础类
文件名称: BaseModule.py
描述: 业务基础类，初始化一些公共功能和属性
创建时间: 2019年11月11日14:22:40
@author chenxinlei
@version v1.0
@update 2019年11月11日14:22:40
'''


class MyConnect(object):
    '''
    @Description: 获得数据库连接
    @Param: comm 公共模块对象
    @Param: logger 日志对象
    @Param: dcnfInf 自定义数据库连接信息配置
    @Return: 数据库连接对象
    @author chenxinlei
    @CreateDate: 2019年11月11日14:33:14
    @update: 2019年11月11日14:33:38
    '''

    def getConnect(comm, logger, dbdir=None, dbType=None):
        dbinf = configparser.ConfigParser()
        dbinf.read(dbdir, encoding='utf-8')
        if dbType == None:
            dbType = '1'
        items = dbinf.items(dbType)
        dcnfInf = dict(items)



        msg = ''
        host = dcnfInf.get('HOSTNAME'.lower())
        port = dcnfInf.get('PORT'.lower())
        user = dcnfInf.get('USER'.lower())
        password = dcnfInf.get('PASSWORD'.lower())
        database = dcnfInf.get('DATABASE'.lower())
        _conn_status = True
        _max_retries_count = int(dcnfInf.get('max_retries_count'))
        _conn_retries_count = int(dcnfInf.get('conn_retries_count'))
        _conn_timeout = int(dcnfInf.get('conn_timeout'))
        _conn_timesleep = int(dcnfInf.get('conn_timesleep'))
        while _conn_status and _conn_retries_count <= _max_retries_count:
            try:
                # print(host,port,items)
                conn = pymysql.connect(host=host, port=int(port), user=user, password=password,
                                       database=database, charset="utf8", connect_timeout=_conn_timeout)
                logger.info('连接数据库成功')
                _conn_status = False
                return conn
            except Exception as e:
                _conn_retries_count += 1
                # time.sleep(_conn_timesleep)
                msg = str(e)
                logger.error("数据库连接异常:" + str(e))
                logger.error('重试次数:' + str(_conn_retries_count))
                time.sleep(_conn_timesleep)
            continue
        logger.error("【数据库连接异常】" + msg)