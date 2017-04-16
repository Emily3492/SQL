
select cast(year(a.CREATED_ON) as varchar)+'年'+cast(month(a.CREATED_ON) as varchar)+'月' 月,a.BILL_ITEM_SID 账单编号,a.BILL_ITEM_NAME 账单名称,a.ROOM_NO 房号,a.BILL_ITEM_DESC 账单描述,a.BILL_ITEM_MONEY 账单金额,
a.PAY_TIME 支付时间,a.CREATED_ON 创建时间,a.PAY_BILL 支付订单号,a.BILL_DATE_SPAN 账单周期范围,a.REMARK 备注,a.BILL_ITEM_MONTH 销帐状态,
(CASE a.BILL_ITEM_STATUS
                 WHEN '0' THEN
                  '已上传'
                 WHEN '1' THEN
                  '待支付'
                 WHEN '2' THEN
                  '已支付'
                 WHEN '9' THEN
                  '取消'
                 WHEN '5' THEN
                  '订单提交'
                 ELSE
                  ''
               END) AS 账单状态,
(CASE b.Type
                 WHEN '0' THEN
                  '生活缴费'
                 WHEN '1' THEN
                  '服务窗'
                 WHEN '2' THEN
                  '悦嘉家App'
                 ELSE
                  ''
               END) AS 缴费入口,c.APARTMENT_NAME
from HOME_PROPERTY_BILL_ITEM a
left join BillPayZFB b on b.SheetID=a.PAY_BILL
left join HOME_APARTMENT c on c.APARTMENT_SID=a.APARTMENT_SID
where  a.CREATED_ON<'20170221'
--and a.CREATED_ON>='20160101'
and a.BILL_ITEM_STATUS =2
--and a.ROOM_NO like ('%二期%')--东方福邸一期二期分开统计
and a.PAY_TIME is not null--只计算有支付时间的部分
