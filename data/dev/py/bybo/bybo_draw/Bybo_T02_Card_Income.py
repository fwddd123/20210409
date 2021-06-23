


# DROP TABLES
T02_Card_Income_sql_create= """ CREATE TABLE IF NOT EXISTS T02_Card_Income(
DEPT_NAME varchar(50)  NULL    DEFAULT ''COMMENT'事业部',
Abbreviation varchar(50)  NULL    DEFAULT ''COMMENT'门店',
privateid varchar(50)  NULL    DEFAULT ''COMMENT'病案号',
Name varchar(50)  NULL    DEFAULT ''COMMENT'客户姓名',
Birth date  NULL    DEFAULT '1900-01-01'COMMENT'出生日期',
RecordCreatedTime  varchar(50)  NULL    DEFAULT ''COMMENT'缴费时间',
new_transaction_no varchar(50)  NULL    DEFAULT ''COMMENT'账单号',
new_rights_type  varchar(50)  NULL    DEFAULT ''COMMENT'卡类型',
new_card_template_idName  varchar(50)  NULL    DEFAULT ''COMMENT'卡名称',
new_card_channelsName varchar(50)  NULL    DEFAULT ''COMMENT'电商名称',
new_card_no varchar(50)  NULL    DEFAULT ''COMMENT'卡号',
new_external_price decimal(18.2)  NULL    DEFAULT '0'COMMENT'原外部售价',
new_external_price1 decimal(18.2)  NULL    DEFAULT '0'COMMENT'外部售价',
new_sale_price decimal(18.2)  NULL    DEFAULT '0'COMMENT'内部售价',
new_group_code varchar(50)  NULL    DEFAULT ''COMMENT'组号',
new_bybo_items_idname varchar(50)  NULL    DEFAULT ''COMMENT'权益核销项目',
ItemSuperType varchar(50)  NULL    DEFAULT ''COMMENT'项目大类',
itemname  varchar(50)  NULL    DEFAULT ''COMMENT'项目名称',
ItemSubCategory varchar(50)  NULL    DEFAULT ''COMMENT'产品大类',
new_card_saleprice varchar(50)  NULL    DEFAULT ''COMMENT'权益单价',
FeeType  varchar(50)  NULL    DEFAULT ''COMMENT'操作',
PayDateTime datetime  NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'划扣时间',
PayDate date  NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'划扣日期',
DeductionCode varchar(50)  NULL    DEFAULT ''COMMENT'划扣编码',
StepName varchar(50)  NULL    DEFAULT ''COMMENT'划扣步骤',
count varchar(50)  NULL    DEFAULT ''COMMENT'划扣比例',
DoctorName varchar(50)  NULL    DEFAULT ''COMMENT'划扣医生姓名',
NurseName varchar(50)  NULL    DEFAULT ''COMMENT'护士',
ConsultantName varchar(50)  NULL    DEFAULT ''COMMENT'咨询师',
ActualPrice decimal(18.2)  NULL    DEFAULT '0'COMMENT'卡券划扣金额',
InDw_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '进入DW的时间',
Partitions_ID int NOT NULL DEFAULT 0 COMMENT '分区键',
SourceName varchar(50) NOT NULL DEFAULT '' COMMENT '数据源'
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='卡券划扣收入';"""
T02_Card_Income_sql_create_detail_tmp= """ """
T02_Card_Income_sql_create_tmp= """CREATE TABLE IF NOT EXISTS T02_Card_Income_tmp(
DEPT_NAME varchar(50)  NULL    DEFAULT ''COMMENT'事业部',
Abbreviation varchar(50)  NULL    DEFAULT ''COMMENT'门店',
privateid varchar(50)  NULL    DEFAULT ''COMMENT'病案号',
Name varchar(50)  NULL    DEFAULT ''COMMENT'客户姓名',
Birth date  NULL    DEFAULT '1900-01-01'COMMENT'出生日期',
RecordCreatedTime  varchar(50)  NULL    DEFAULT ''COMMENT'缴费时间',
new_transaction_no varchar(50)  NULL    DEFAULT ''COMMENT'账单号',
new_rights_type  varchar(50)  NULL    DEFAULT ''COMMENT'卡类型',
new_card_template_idName  varchar(50)  NULL    DEFAULT ''COMMENT'卡名称',
new_card_channelsName varchar(50)  NULL    DEFAULT ''COMMENT'电商名称',
new_card_no varchar(50)  NULL    DEFAULT ''COMMENT'卡号',
new_external_price decimal(18.2)  NULL    DEFAULT '0'COMMENT'原外部售价',
new_external_price1 decimal(18.2)  NULL    DEFAULT '0'COMMENT'外部售价',
new_sale_price decimal(18.2)  NULL    DEFAULT '0'COMMENT'内部售价',
new_group_code varchar(50)  NULL    DEFAULT ''COMMENT'组号',
new_bybo_items_idname varchar(50)  NULL    DEFAULT ''COMMENT'权益核销项目',
ItemSuperType varchar(50)  NULL    DEFAULT ''COMMENT'项目大类',
itemname  varchar(50)  NULL    DEFAULT ''COMMENT'项目名称',
ItemSubCategory varchar(50)  NULL    DEFAULT ''COMMENT'产品大类',
new_card_saleprice varchar(50)  NULL    DEFAULT ''COMMENT'权益单价',
FeeType  varchar(50)  NULL    DEFAULT ''COMMENT'操作',
PayDateTime datetime  NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'划扣时间',
PayDate date  NULL    DEFAULT '1900-01-01 00:00:00'COMMENT'划扣日期',
DeductionCode varchar(50)  NULL    DEFAULT ''COMMENT'划扣编码',
StepName varchar(50)  NULL    DEFAULT ''COMMENT'划扣步骤',
count varchar(50)  NULL    DEFAULT ''COMMENT'划扣比例',
DoctorName varchar(50)  NULL    DEFAULT ''COMMENT'划扣医生姓名',
NurseName varchar(50)  NULL    DEFAULT ''COMMENT'护士',
ConsultantName varchar(50)  NULL    DEFAULT ''COMMENT'咨询师',
ActualPrice decimal(18.2)  NULL    DEFAULT '0'COMMENT'卡券划扣金额')
ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='卡券划扣收入临时表' """

T02_Card_Income_dtail_tmp_truncate= """truncate bbkq_dm.T98_Bybo_Cus_Treat_ChDetail_tmp; 	 
"""
T02_Card_Income_dtail_tmp_insert= """         
"""
T02_Card_Income_tmp_truncate= """truncate bbkq_dw.T02_Card_Income_tmp; 	 
"""

# INSERT RECORDS str_to_date('{end_date}','%Y-%m-%d %H:%i:%s')

T02_Card_Income_insert_tmp = """insert into bbkq_dw.t02_card_income_tmp
(DEPT_NAME
,Abbreviation
,privateid
,Name
,Birth
,RecordCreatedTime
,new_transaction_no
,new_rights_type
,new_card_template_idName
,new_card_channelsName
,new_card_no
,new_external_price
,new_external_price1
,new_sale_price
,new_group_code
,new_bybo_items_idname
,ItemSuperType
,itemname
,ItemSubCategory
,new_card_saleprice
,FeeType
,PayDateTime
,PayDate
,DeductionCode
,StepName
,count
,DoctorName
,NurseName
,ConsultantName
,ActualPrice)
            SELECT
            /*一级*/
            
            f.DEPT_NAME 事业部,
            o.Abbreviation 门店,
            r.privateid 病案号,
            r.`Name` 客户姓名,
            DATE( r.Birth ) 出生日期,
            l.RecordCreatedTime 缴费时间,
            a.new_transaction_no 账单号,
            (CASE
            a.new_rights_type 
            WHEN '1' THEN
            '满减卡' 
            WHEN '2' THEN
            '折扣卡' 
            WHEN '3' THEN
            '储值卡' 
            WHEN '4' THEN
            '项目卡' 
            WHEN '5' THEN
            '组合卡' ELSE '其他' 
            END) 卡类型,
            n.new_card_template_idName 卡名称,
            n.new_card_channelsName 电商名称,
            n.new_card_no 卡号,
            n.new_external_price 原外部售价,
            IF
            ( sign( n.new_external_price - 1000000 ) < 1, n.new_external_price, n.new_sale_price ) 外部售价,
            n.new_sale_price 内部售价,
            /*二级*/
            a.new_group_code 组号,
            b.new_bybo_items_idname 权益核销项目,
            d.ItemSuperType 项目大类,
            d.itemname 项目名称,
            i.ItemSubCategory 产品大类,
            new_card_saleprice 权益单价,
            /*三级*/
            (CASE
            c.FeeType 
            WHEN 9 THEN
            '执行' ELSE '撤销' 
            END) 操作,
            c.PayDateTime 划扣时间,
            date(c.PayDateTime) 划扣日期,
            d.DeductionCode 划扣编码,
            d.StepName 划扣步骤,
            l.count 划扣比例,
            d.DoctorName 划扣医生姓名,
            c1.NurseName 护士,
            d.ConsultantName 咨询师,
            round(
            IF
            (
            n.new_external_price = n.new_external_price - n.new_sale_price,
            IF
            (
            sign( n.new_external_price - 1000000 ) >= 1,
            0,
            if(
            (
            ( SELECT sum( new_total_number ) total_number FROM bbkq_ods_crm.new_my_rights WHERE statecode = 0 AND new_my_card_id = n.new_my_cardId ) *  (
            SELECT
            count( b1.new_number ) maxnumber 
            FROM
            bbkq_ods_crm.new_my_rights_hxrecord a1,
            bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            WHERE
            a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            AND a1.new_tenant_id = a.new_tenant_id 
            AND a1.new_payment_id = a.new_payment_id 
            AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            AND a1.new_equity_number >= 0 
            )  
            )=0,0,
            n.new_external_price / ( SELECT sum( new_total_number ) total_number FROM bbkq_ods_crm.new_my_rights WHERE statecode = 0 AND new_my_card_id = n.new_my_cardId ) * abs( p.Amount ) / (
            c1.ActualPrice / (
            SELECT
            count( b1.new_number ) maxnumber 
            FROM
            bbkq_ods_crm.new_my_rights_hxrecord a1,
            bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            WHERE
            a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            AND a1.new_tenant_id = a.new_tenant_id 
            AND a1.new_payment_id = a.new_payment_id 
            AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            AND a1.new_equity_number >= 0 
            ) 
            ) 
            )
            ),
            (
            case r.new_rights_type
            WHEN '1' THEN
            if(d.GroupIndex=d.ItemGroupIndex=1,r.new_settlement_price,0)
            WHEN '2' THEN
            if(d.GroupIndex=d.ItemGroupIndex=1,r.new_settlement_price,0)
            WHEN '3' THEN
            b.new_deductions_price*(select base/(base+quota) from bbkq_ods_card.base_rights where id=r.new_base_rights_id)
            WHEN '4' THEN
            /*r.new_project_total_money*b.new_card_saleprice/r.new_project_total_money*/
            b.new_card_saleprice*b.new_use_number
            END 
            )
            *
            IF
            (
            ifnull( n.new_external_price, 0 ) = 0,
            n.new_sale_price,
            if(
            (n.new_sale_price * if(r.new_rights_type='3',b.new_use_number,(
            SELECT
            sum( b1.new_use_number ) maxnumber 
            FROM
            bbkq_ods_crm.new_my_rights_hxrecord a1,
            bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            WHERE
            a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            AND a1.new_tenant_id = a.new_tenant_id 
            AND a1.new_payment_id = a.new_payment_id 
            AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            AND a1.new_equity_number >= 0 
            )
            ) )=0,0,
            
            IF
            ( sign( ifnull( n.new_external_price, 0 ) - 1000000 ) < 1, n.new_external_price, n.new_sale_price ) 
            ) / (n.new_sale_price * if(r.new_rights_type='3',b.new_use_number,(
            SELECT
            sum( b1.new_use_number ) maxnumber 
            FROM
            bbkq_ods_crm.new_my_rights_hxrecord a1,
            bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            WHERE
            a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            AND a1.new_tenant_id = a.new_tenant_id 
            AND a1.new_payment_id = a.new_payment_id 
            AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            AND a1.new_equity_number >= 0 
            )
            ) )
            /*b.new_use_number */
            )
            ) *
            IF
            ( c.FeeType = 9, l.count,- l.count ),
            2 
            )
            -- IF
            -- 	(
            -- 	n.new_external_price = n.new_external_price - n.new_sale_price,
            -- IF
            -- 	(
            -- 	sign( n.new_external_price - 1000000 ) >= 1,
            -- 	0,
            -- 	n.new_external_price / ( SELECT sum( new_total_number ) total_number FROM bbkq_ods_crm.new_my_rights WHERE statecode = 0 AND new_my_card_id = n.new_my_cardId ) * abs( p.Amount ) / (
            -- 	c1.ActualPrice / (
            -- SELECT
            -- 	count( b1.new_number ) maxnumber 
            -- FROM
            -- 	bbkq_ods_crm.new_my_rights_hxrecord a1,
            -- 	bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            -- WHERE
            -- 	a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            -- 	AND a1.new_tenant_id = a.new_tenant_id 
            -- 	AND a1.new_payment_id = a.new_payment_id 
            -- 	AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            -- 	AND a1.new_equity_number >= 0 
            -- 	) 
            -- 	) 
            -- 	),
            -- 	(
            -- case r.new_rights_type
            -- 	WHEN '1' THEN
            -- 	 if(d.GroupIndex=d.ItemGroupIndex=1,r.new_settlement_price,0)
            -- 	WHEN '2' THEN
            -- 	 if(d.GroupIndex=d.ItemGroupIndex=1,r.new_settlement_price,0)
            -- 	WHEN '3' THEN
            -- 	 b.new_deductions_price*(select base/(base+quota) from bbkq_ods_card.base_rights where id=r.new_base_rights_id)
            -- 	WHEN '4' THEN
            -- 	 /*r.new_project_total_money*b.new_card_saleprice/r.new_project_total_money*/
            -- 	 b.new_card_saleprice*b.new_use_number
            -- 	END 
            -- 	)
            -- *
            -- IF
            -- 	(
            -- 	ifnull( n.new_external_price, 0 ) = 0,
            -- 	n.new_sale_price,
            -- IF
            -- 	( sign( ifnull( n.new_external_price, 0 ) - 1000000 ) < 1, n.new_external_price, n.new_sale_price ) 
            -- 	) / n.new_sale_price / if(r.new_rights_type='3',b.new_use_number,(
            -- SELECT
            -- 	sum( b1.new_use_number ) maxnumber 
            -- FROM
            -- 	bbkq_ods_crm.new_my_rights_hxrecord a1,
            -- 	bbkq_ods_crm.new_my_rights_items_hxrecord b1 
            -- WHERE
            -- 	a1.new_my_rights_hxrecordId = b1.new_my_rights_hxrecord_id 
            -- 	AND a1.new_tenant_id = a.new_tenant_id 
            -- 	AND a1.new_payment_id = a.new_payment_id 
            -- 	AND bis.new_bybo_itemsid = b1.new_bybo_items_id 
            -- 	AND a1.new_equity_number >= 0 
            -- 	)) /*b.new_use_number */
            -- 	
            -- 	) *
            -- IF
            -- 	( c.FeeType = 9, l.count,- l.count ),
            -- 	2 
            -- 	) ActualPrice
            FROM
            bbkq_ods_ekanya.chargeorder c
            INNER JOIN bbkq_ods_ekanya.chargeorder c1 ON c.SourceChargeOrderId = c1.id 
            AND c.Region = c1.Region
            INNER JOIN bbkq_ods_ekanya.chargeorderdetail d ON c.id = d.ChargeOrderId 
            AND c.Region = d.Region 
            AND ( c.FeeType, d.IsPending ) IN ( ( 9, 0 ), ( 10, 1 ) )
            INNER JOIN bbkq_ods_ekanya.chargeorderdetail k ON k.SourceDetailId = d.SourceDetailId 
            AND k.Region = d.Region
            INNER JOIN bbkq_ods_ekanya.chargeorderdetail l ON k.SourceDetailId = l.id 
            AND k.Region = l.Region
            INNER JOIN bbkq_ods_ekanya.paymentdetail p ON p.TargetId = k.id 
            AND k.Region = p.Region 
            AND p.PaymentType = '卡券抵扣'
            INNER JOIN bbkq_ods_crm.new_my_rights_hxrecord a ON /*a.new_payment_id = c1.id */
            a.new_payment_id = CONCAT( c1.id, '' ) 
            AND a.new_tenant_id = c1.TenantId
            INNER JOIN bbkq_ods_crm.new_my_card n ON n.new_my_cardid = a.new_my_card_id 
            AND a.new_equity_number >= 0
            INNER JOIN bbkq_ods_crm.new_my_rights r ON r.new_my_rightsId = a.new_my_rights_id
            INNER JOIN bbkq_ods_crm.new_my_rights_items_hxrecord b ON a.new_my_rights_hxrecordId = new_my_rights_hxrecord_id /*AND l.itemGroupIndex = new_number */
            INNER JOIN bbkq_ods_crm.new_bybo_items bis ON bis.new_bybo_itemsid = b.new_bybo_items_id 
            AND d.ProcedureCode = bis.new_item_code
            INNER JOIN bbkq_ods_ekanya.patient r ON r.id = c.patientid 
            AND r.Region = c.Region
            INNER JOIN bbkq_ods_ekanya.office o ON o.id = c.OfficeId 
            AND o.Region = c.Region
            INNER JOIN BBKQ_DM_EKANYA.DM02_OFFICE f ON f.officeid = o.id 
            AND f.REGION = o.REGION
            LEFT JOIN bbkq_ods_ekanya.chargeitem i ON i.id = d.chargeitemid 
            AND i.Region = d.region 
            WHERE
            date(c.PayDateTime) between  '{self.begin_date}' and '{self.end_date}'
            /*AND privateid = '20092105040028393' AND new_card_no = 'BBZH210201TBW8K' and a.new_transaction_no='0035607'
            /*and  a.new_payment_id=3803170*/
            -- 		set D_START_DATE=str_to_date(CONCAT(C_START_DATE,' 00:00:00'), '%Y-%m-%d %H:%i:%s') ; 
            -- set D_END_DATE=str_to_date(CONCAT(C_END_DATE,' 23:59:59'),'%Y-%m-%d %H:%i:%s'); 
            GROUP BY
            c.id,
            b.new_my_rights_hxrecord_id,
            b.new_bybo_items_id 
            ORDER BY
            f.DEPT_NAME,
            l.RecordCreatedTime;

"""
T02_Card_Income_del = """ delete from  bbkq_dw.t02_card_income where Partitions_ID  between  '{self.begin_date}' and '{self.end_date}'
"""
T02_Card_Income_insert="""
insert into  bbkq_dw.t02_card_income
(DEPT_NAME
,Abbreviation
,privateid
,Name
,Birth
,RecordCreatedTime
,new_transaction_no
,new_rights_type
,new_card_template_idName
,new_card_channelsName
,new_card_no
,new_external_price
,new_external_price1
,new_sale_price
,new_group_code
,new_bybo_items_idname
,ItemSuperType
,itemname
,ItemSubCategory
,new_card_saleprice
,FeeType
,PayDateTime
,PayDate
,DeductionCode
,StepName
,count
,DoctorName
,NurseName
,ConsultantName
,ActualPrice
,InDw_timestamp
,Partitions_ID
,SourceName
)

select 
a.DEPT_NAME
,a.Abbreviation
,a.privateid
,a.Name
,a.Birth
,a.RecordCreatedTime
,a.new_transaction_no
,a.new_rights_type
,a.new_card_template_idName
,a.new_card_channelsName
,a.new_card_no
,a.new_external_price
,a.new_external_price1
,a.new_sale_price
,a.new_group_code
,a.new_bybo_items_idname
,a.ItemSuperType
,a.itemname
,a.ItemSubCategory
,a.new_card_saleprice
,a.FeeType
,a.PayDateTime
,a.PayDate
,a.DeductionCode
,a.StepName
,a.count
,a.DoctorName
,a.NurseName
,a.ConsultantName
,a.ActualPrice
,now() as InDw_timestamp
,paydate as Partitions_ID
,'bbkq_ods_ekanya.chargeorder ' as SourceName
from  bbkq_dw.t02_card_income_tmp a
        """

# QUERY LISTS
T98_Bybo_CRM_Patient_Card_sql_create=""""""










T02_Card_Income_Create = [T02_Card_Income_sql_create,T02_Card_Income_sql_create_tmp]
                       # sql_P1_Rollback22099: {'begin_date': 'self.begin_date', 'end_date': 'self.end_date'},
                       # sql_P1_Del: {'begin_date': 'self.begin_date', 'end_date': 'self.end_date'},
                       # sql2Tmp:['begin_date','end_date']
T02_Card_Income = [T02_Card_Income_tmp_truncate,T02_Card_Income_insert_tmp,T02_Card_Income_del,T02_Card_Income_insert]

