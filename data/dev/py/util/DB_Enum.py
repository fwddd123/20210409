#!/usr/bin/python3
# coding=utf-8
'''
项目名称:  数据库连接枚举类
文件名称:   
描述: [类型描述] 
创建时间:  2020年2月19日11:32:20
'''
from enum import Enum

'''
此配置的值要与db_connect_info.ini对应
'''


class DatabasesEnum(Enum):
    default = 1
    #xxxx=2 以此类推
