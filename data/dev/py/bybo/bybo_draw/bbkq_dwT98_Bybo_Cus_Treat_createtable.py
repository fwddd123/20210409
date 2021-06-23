


# DROP TABLES
sql_truncate = """ CREATE TABLE `T98_Bybo_Cus_Treat` (
  `Chrg_Dt` date DEFAULT NULL COMMENT '收费日期',
  `Chrg_Sngl_Stat` varchar(200) DEFAULT NULL COMMENT '收费单状态',
  `Chrg_Hour` int(11) DEFAULT NULL COMMENT '收费小时',
  `Comb_Pay_Ind` varchar(10) DEFAULT NULL COMMENT '组合付款标志',
  `Chrg_Typ_Cd` varchar(10) DEFAULT NULL COMMENT '收费类型代码',
  `Rtn_Fee_Typ_Cd` varchar(10) DEFAULT NULL COMMENT '退费类型代码',
  `Sell_Chnl_Cd` varchar(10) DEFAULT NULL COMMENT '销售渠道代码',
  `Chrg_Scne_Cd` varchar(10) DEFAULT NULL COMMENT '收费场景代码',
  `Fst_Choic_Pay_Md` varchar(200) DEFAULT NULL COMMENT '首选付款方式',
  `Sec_Choic_Pay_Md` varchar(200) DEFAULT NULL COMMENT '次选付款方式',
  `Oth_Pay_Md` varchar(200) DEFAULT NULL COMMENT '其他付款方式描述',
  `Dr_Id` varchar(50) DEFAULT NULL COMMENT '医生编号',
  `Treat_Advsr_Id` varchar(50) DEFAULT NULL COMMENT '治疗咨询师编号',
  `Office_Id` varchar(50) DEFAULT NULL COMMENT '诊所编号',
  `Patient_Id` varchar(50) DEFAULT NULL COMMENT '患者编号',
  `Treat_Main_Cls` varchar(200) DEFAULT NULL COMMENT '治疗项目大类',
  `Treat_Sub_Cls` varchar(200) DEFAULT NULL COMMENT '治疗项目子类',
  `Treat_Nm` varchar(200) DEFAULT NULL COMMENT '治疗项目名称',
  `Resv_Ind` varchar(10) DEFAULT NULL COMMENT '预约标志',
  `Resv_Dr_Id` varchar(50) DEFAULT NULL COMMENT '预约医生编号',
  `Resv_Advsr_Id` varchar(50) DEFAULT NULL COMMENT '预约咨询师编号',
  `Regi_Typ` varchar(200) DEFAULT NULL COMMENT '挂号类型',
  `Sngl_Item_Discnt_Amt` decimal(18,2) DEFAULT NULL COMMENT '单项折扣金额',
  `Recvbl_Amt` decimal(18,2) DEFAULT NULL COMMENT '应收金额',
  `Recv_Amt` decimal(18,2) DEFAULT NULL COMMENT '实收金额',
  `Arrears_Amt` decimal(18,2) DEFAULT NULL COMMENT '欠费金额',
  `Dr_Incom` decimal(18,2) DEFAULT NULL COMMENT '医生收入',
  `Create_Time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '时间戳'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='拜博客户就诊汇总';"""
sql_P1_Rollback22099 = """

                                                            """
sql_P1_Del = """
               
                                                                        """
sql2Tmp = """

                                   """

# INSERT RECORDS str_to_date('{end_date}','%Y-%m-%d %H:%i:%s')

songplay_table_insert = ("""
""")
user_table_insert = ("""
""")
song_table_insert = ("""
""")
artist_table_insert = ("""
""")
time_table_insert = ("""
""")

# FIND SONGS
song_select = ("""
""")

# QUERY LISTS


order_table_Patient = [sql_truncate]
                       # sql_P1_Rollback22099: {'begin_date': 'self.begin_date', 'end_date': 'self.end_date'},
                       # sql_P1_Del: {'begin_date': 'self.begin_date', 'end_date': 'self.end_date'},
                       # sql2Tmp:['begin_date','end_date']
order_table_Patient = [sql_truncate]

