'''
@author:chenxinlei
公共工具模块
'''
import logging
import time
from logging import handlers


class CommonUtil(object):
    #win系统日志记录
    def getLoggerForWin(logName):
        localTime = time.localtime(time.time())
        if logName == None:
            logName = ''
        strTime = logName + '_' + time.strftime("%Y%m", localTime)
        when='D'
        backCount=3
        fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        logger = logging.getLogger(strTime)
        format_str = logging.Formatter(fmt)#设置日志格式
        logger.setLevel(logging.INFO)#设置日志级别
        sh = logging.StreamHandler()#往屏幕上输出
        sh.setFormatter(format_str) #设置屏幕上显示的格式
        th = handlers.TimedRotatingFileHandler(filename=strTime,when=when,backupCount=backCount,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
        th.setFormatter(format_str)#设置文件里写入的格式
        logger.addHandler(sh) #把对象加到logger里
        logger.addHandler(th)
        return logger
    # 获取日志对象
    def getLogger(logName):
        localTime = time.localtime(time.time())
        if logName == None:
            logName = ''
        strTime = logName + '_' + time.strftime("%Y%m", localTime)
        logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                            filename=strTime + '.info',
                            # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志, a是追加模式，默认如果不写的话，就是追加模式
                            filemode = 'a',
                            # 日志格式
                            format = '%(asctime)s - %(pathname)s[line:%(lineno)d] - % (levelname) s: % (message)s')
        logger = logging.getLogger()
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s -% (message)s')
        streamhandler.setFormatter(formatter)
        logger.addHandler(streamhandler)
        return logger

    # 读取配置信息
    def getProperties(cnf):
        properties = {}
        try:
            with open(cnf, 'r', encoding='utf-8') as pro_file:
                for line in pro_file:
                    if line.find('=') > 0:
                        strs = line.replace('\n', '').split('=')
                        properties[strs[0]] = strs[1]
        except Exception as e:
            raise e

        return properties
