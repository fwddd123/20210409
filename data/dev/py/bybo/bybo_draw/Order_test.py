#!/usr/bin/python3
# coding=utf-8
import sys
import time
import datetime

# from Bo_T01PotentialCustomerH_Data2Dw import Bo_T01PotentialCustomerH_Data2Dw


class Run_Function():

    def __init__(self):
        self._modules = ''
        self._filename = ''


    def run(self,Class_Name):
        self.modules = __import__(Class_Name)
        eg = getattr(self.modules, Class_Name)()
        logger = eg.logger
        taget_table=eg._taget_table
        logger.info("任务开始")
        stF = "%Y-%m-%d %H:%M:%S"
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        begin_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)).strftime(stF)
        end_date = (datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)).strftime(stF)
        frist_data='1900-01-01 00:00:00'
        end_date2020='2020-12-31 23:59:59'
        frist_data_t2 = '2021-01-01 00:00:00'
        end_date2020_t2='2021-03-14 23:59:59'
        frist_data_t3 = '2021-03-15 00:00:00'
        end_date2020_t3 = '2021-03-15 23:59:59'
        if len(sys.argv) == 2:
            begin_date = sys.argv[1] + ' 00:00:00'

        try:
            zst = time.time()

            F_or_N = eg.get_rowcount(taget_table)
            print(F_or_N)
            if F_or_N >0:
                # eg.Draw_data(end_date2020_t3,frist_data_t3)
                eg.Draw_data(end_date, begin_date)
            else:
                # eg.Draw_data(end_date2020_t2, frist_data)
                eg.Draw_data(begin_date, frist_data)
            logger.info('【运行总时间】' + str(time.time() - zst)[:4])
        except Exception as e:
            logger.error("【运行异常】" + str(e))

h=Run_Function()
h.run('T05_AiYa_SaleClue_H_Data2Dw')








