/*
 * 目 标 表: EASYETL.MS_CUST_ZL_COST_M
 * 源    表:
 * 脚本生成 : 郑健
 * 生成日期 : 2015-04-24
 * 修复日期 ：2015-08-27
 */

--创建月分区，如分区已存在则什么都不做
--select etl_partition('EASYETL.MS_CUST_ZL_COST_M','M','$ETL_DATE','CREATE');

--清理本期数据
DELETE FROM MS_CUST_ZL_COST_M WHERE DATA_DT = '$ETL_DATE';
COMMIT;

--设置参数值
set v_month_last_end = select to_char(trunc(to_date('$ETL_DATE', 'yyyymmdd'), 'MM') - 1,'yyyymmdd');
set v_cust_cnt = select count(*) from ms_cust_aset_m where data_dt='$ETL_DATE';
set v_bgn_dt = select substr('$ETL_DATE',1,6)||'01';
set v_Last_Dt = select to_char(to_date(substr('$ETL_DATE', 1, 6) || '01','yyyymmdd')-1,'yyyymmdd');
set v_month_bgn = select substr('$ETL_DATE', 1, 6) || '01';
commit;

--创建临时表
CREATE GLOBAL TEMPORARY TABLE mod_hs_300( HS_300_RATE numeric(20,4));
insert into mod_hs_300
SELECT  decode(sum(decode(a.data_dt,
                          '$v_month_last_end',
                          a.hs_300,
                          0)),
               0,
               0,
               (sum(decode(a.data_dt, '$ETL_DATE', a.hs_300, 0)) -
               sum(decode(a.data_dt,
                           '$v_month_last_end',
                           a.hs_300,
                           0))) /
               sum(decode(a.data_dt,
                          '$v_month_last_end',
                          a.hs_300,
                          0))) as hs_300_rate
   FROM (Select a.Data_Dt,
                a.Mkt_Type_Cd,
                Last_Value(b.End_Price ) Over(Order By a.Data_Dt) As hs_300
           From (Select a.Data_Dt, '000001' As Mkt_Type_Cd   --应杨宏要求将沪深300改为上证指数20150430
                   From Meta_Sor_Tran_Dt_Def a
                  Where a.Cluster_Id = 'CLU_ACRM'
                    And a.Sor_Id = 'ODS'
                    And a.Data_Dt Between
                        to_char(trunc(to_date('$ETL_DATE',
                                              'yyyymmdd'),
                                      'MM') - 10,
                                'yyyymmdd') And '$ETL_DATE') a
           Left Join Sta_Gti_Mkt_Pnt b
             On a.Data_Dt = b.Data_Dt
            And a.Mkt_Type_Cd = b.Mkt_Type_Cd --and b.End_Price is not null --edited by gaoqin ignore nulls 替换
          Order By a.Data_Dt) a
  group by Mkt_Type_Cd;

   ;

	 --普通\限售\债券盈利
CREATE GLOBAL TEMPORARY TABLE mod_cust_pft_tmp as
	SELECT	a.pty_id
		,sum(case when b.up_prdt_type_id='PA040000' and substr(a.flag,2,2) in ('01','04','05') then a.tran_pft else 0 end) as gp_tran_pft
		,sum(case when b.up_prdt_type_id='PA040000' and substr(a.flag,2,2)='07' then a.tran_pft else 0 end) as lmt_tran_pft
		,sum(case when b.up_prdt_type_id in ('PA030000','PA080000') and substr(a.flag,2,2) in ('01','04','05') then a.tran_pft else 0 end) as zq_tran_pft
   	FROM MS_CUST_ZL_PRDT_PFT_M a
	     ,mf_prdt_attr_h b
  	where a.data_dt = '$ETL_DATE' and b.bgn_dt<='$ETL_DATE' and b.end_dt>'$ETL_DATE' and a.prdt_id=b.prdt_id
  	group by a.pty_id
	;

CREATE GLOBAL TEMPORARY TABLE mod_tran_pft_m_sub(PTY_ID        VARCHAR(32),
  FINA_TRAN_PFT NUMeric(20,4),
  FUND_TRAN_PFT NUMeric(20,4),
  in_TRAN_PFT NUMeric(20,4)
);

--1. 计算账面盈利
insert into mod_tran_pft_m_sub (
	pty_id
 	,fina_tran_pft
	,fund_tran_pft
	,in_tran_pft
)
SELECT a.pty_id,
       sum(fina_tran_pft)  fina_tran_pft,
       sum(fund_tran_pft) fund_tran_pft,
       sum(in_tran_pft) in_tran_pft
  FROM (
	SELECT	a.pty_id
       		,a.prdt_id
       		,case when (e.prdt_type_id = 'PA070100' or e.up_prdt_type_id in('PA090000','PA100000')) and  a.prdt_id <> 'TA940018' then
			a.mkt_val - coalesce(b.mkt_val, 0) - coalesce(c.mkt_val, 0)
			else 0
		end as fina_tran_pft
		,case	when (e.up_prdt_type_id = 'PA050000' or e.prdt_type_id='PA040500') and a.sys_source_cd='02' then
			a.mkt_val - coalesce(b.mkt_val, 0) - coalesce(c.mkt_val, 0)
			else 0
		end as fund_tran_pft
		,case	when (e.up_prdt_type_id = 'PA050000' or e.prdt_type_id='PA040500') and a.sys_source_cd<>'02' then
			a.mkt_val - coalesce(b.mkt_val, 0) - coalesce(c.mkt_val, 0)
		else 0
		end as in_tran_pft
	from (
	select t.pty_id ,t.sys_source_cd,t.prdt_id,sum(t.mkt_val) as mkt_val
	FROM (SELECT	a.pty_id
			,a.sys_source_cd
			,a.prdt_id
			, sum(a.mkt_val*coalesce(b.exchange_rate,1))  as mkt_val
                  FROM	ms_cust_ccy_prdt_anahold_d a
		left outer join view_conv_rmb b on a.data_dt=b.data_dt and a.ccy=b.ccy
		where 	a.data_dt = '$ETL_DATE' and a.sys_source_cd in ('01','02','03','04','08') --普通\根网
		group by a.pty_id,a.sys_source_cd,a.prdt_id
                union all
                SELECT	a.pty_id
			,a.sys_source_cd
			, a.prdt_id
			, 0 as mkt_val
                  FROM ms_cust_ccy_prdt_anaunhold_d a
		where 	a.sys_source_cd in ('01','02','03','04','08') --普通\根网
                 	and a.data_dt in
                       (select d_date
                          from ana_dim_date
                         where d_date between '$v_month_bgn' and '$ETL_DATE')
		group by a.pty_id,a.sys_source_cd,a.prdt_id
                union all
                SELECT	a.pty_id
			,'05' as sys_source_cd
			, a.prdt_id
			, a.mkt_val
                  FROM	MS_CUST_PRDT_FC_HOLD_D a
                 where	data_dt = '$ETL_DATE'
                   	And (a.Hold_Cnt <> 0 Or a.Deb_Mnt <> 0)
                union all
                SELECT	a.pty_id
			,'05' as sys_source_cd
			, a.prdt_id
			, 0 as mkt_val
                  FROM MS_CUST_PRDT_FC_UNHOLD_D a
                 where a.data_dt in
                       (select d_date
                          from ana_dim_date
                         where d_date between '$v_month_bgn' and '$ETL_DATE')
		) t
         group by pty_id,sys_source_cd, prdt_id
	) a
  left join (
  	SELECT	a.pty_id
		,a.sys_source_cd
		,a.prdt_id
		, sum(a.mkt_val*coalesce(b.exchange_rate,1))  as mkt_val
	  FROM	ms_cust_ccy_prdt_anahold_d a
	left outer join view_conv_rmb b on a.data_dt=b.data_dt and a.ccy=b.ccy
	where 	a.data_dt = '$v_Last_Dt' and a.sys_source_cd in ('01','02','03','04','08') --普通\根网
	group by a.pty_id,a.sys_source_cd,a.prdt_id
	) b
   on a.pty_id = b.pty_id and a.prdt_id = b.prdt_id and a.sys_source_cd=b.sys_source_cd
  left join MS_CUST_PRDT_FC_HOLD_D c
    on c.data_dt = '$v_Last_Dt' and a.pty_id = c.pty_id and a.sys_source_cd='05' and a.prdt_id = c.prdt_id
  left join (select prdt_id,up_prdt_type_id,prdt_type_id,row_number() over(partition by prdt_id order by end_dt desc) as rk
	from mf_prdt_attr_h e
    	where e.BGN_DT <= '$ETL_DATE' and e.end_dt > '$v_Last_Dt'
		and (e.prdt_type_id in ('PA040500', 'PA070100') or e.up_prdt_type_id in ( 'PA050000','PA090000','PA100000'))
	) e
	on e.rk=1 and a.prdt_id = e.prdt_id
 where e.prdt_id is not null
   and a.prdt_id <> 'TA940018'
   ) a
   group by a.pty_id;

--2. 计算交易盈利
insert into mod_tran_pft_m_sub (
	pty_id
 	,fina_tran_pft
	,fund_tran_pft
	,in_tran_pft
)
SELECT a.pty_id,
       sum(fina_tran_pft)  fina_tran_pft,
       sum(fund_tran_pft) fund_tran_pft,
       sum(in_tran_pft) in_tran_pft
  FROM (
	SELECT	a.pty_id
       		,a.prdt_id
       		,case when (e.prdt_type_id = 'PA070100' or e.up_prdt_type_id in('PA090000','PA100000')) and  a.prdt_id <> 'TA940018' then
       			coalesce(a.Sell_Amt, 0) - coalesce(a.buy_amt, 0) -
       			coalesce(a.Buy_Rake, 0) - coalesce(a.sell_Rake, 0)
			else 0
		end as fina_tran_pft
		,case	when (e.up_prdt_type_id = 'PA050000' or e.prdt_type_id='PA040500') and a.sys_source_cd='02' then
			coalesce(a.Sell_Amt, 0) - coalesce(a.buy_amt, 0) -
       			coalesce(a.Buy_Rake, 0) - coalesce(a.sell_Rake, 0)
			else 0
		end as fund_tran_pft
		,case	when (e.up_prdt_type_id = 'PA050000' or e.prdt_type_id='PA040500') and a.sys_source_cd<>'02' then
			coalesce(a.Sell_Amt, 0) - coalesce(a.buy_amt, 0) -
       			coalesce(a.Buy_Rake, 0) - coalesce(a.sell_Rake, 0)
		else 0
		end as in_tran_pft
	from (Select a.Pty_Id
		    ,a.sys_source_cd
                    ,a.Prdt_Id
                    ,sum(a.Buy_Amt) as Buy_Amt
                    ,sum(a.Sell_Amt) as Sell_Amt
                    ,sum(a.Buy_Rake + coalesce(a.buy_fare, 0)) As Buy_Rake
                    ,sum(a.sell_Rake + coalesce(a.sell_fare, 0)) As sell_Rake
               FROM Mf_Cust_Tran_Core_m a
              where a.data_dt = '$ETL_DATE'
              group by a.pty_id,a.sys_source_cd, a.prdt_id
		union all
                         --新股入账信息
			Select a.Pty_Id,
				'01' as sys_source_cd,
                                a.Stk_Id As Prdt_Id,
                                sum(decode(a.busi_cd, '406004021', 0, a.Barg_amt)*coalesce(b.exchange_rate,1)) As Buy_Amt,
                                SUM(decode(a.busi_cd, '406004021', a.Barg_amt, 0)*coalesce(b.exchange_rate,1)) As Sell_Amt,
                                0 As Buy_Rake,
                                0 As Sell_Rake
                           From sta_stk_jnls a
			left outer join view_conv_rmb b on a.ccy=b.ccy and a.data_dt=b.data_dt
                          Where a.Data_Dt Between '$v_month_bgn' And '$ETL_DATE'
                            AND a.busi_cd IN ('406004021', '406004022')
                          group by a.pty_id, a.Stk_Id
		union all
                         --配股，红股，分红
                         Select a.Pty_Id,
				a.sys_source_cd,
                                a.Prdt_Id,
                                (a.Tran_In + a.Allot_Share_Amt) * b.Exchange_Rate As Buy_Amt, --转托管入金额配股金额。红股也不计入买入金额，目的是：有红股时，买入均价不变
                                a.Bonus_Amt * b.Exchange_Rate + a.Tran_Out * b.Exchange_Rate As Sell_Amt, --现金分红+转托管出金额
                                0 As Buy_Rake,
                                0 As Sell_Rake
                           From Mf_Cust_sys_Fin_d a, View_Conv_Rmb b
                          Where a.Data_Dt >= '$v_month_bgn'
                            And a.Data_Dt <= '$ETL_DATE'
                            And a.Data_Dt = b.Data_Dt
                            And a.Ccy = b.Ccy
	) a
  left join (select prdt_id,up_prdt_type_id,prdt_type_id,row_number() over(partition by prdt_id order by end_dt desc) as rk
	from mf_prdt_attr_h e
    	where e.BGN_DT <= '$ETL_DATE' and e.end_dt > '$v_Last_Dt'
		and (e.prdt_type_id in ('PA040500', 'PA070100') or e.up_prdt_type_id in ( 'PA050000','PA090000','PA100000'))
	) e
	on e.rk=1 and a.prdt_id = e.prdt_id
 where e.prdt_id is not null
   and a.prdt_id <> 'TA940018'
   ) a
   group by a.pty_id;

--3.合并盈利计算
CREATE GLOBAL TEMPORARY TABLE mod_tran_pft_m(PTY_ID        VARCHAR(32),
  FINA_TRAN_PFT NUMeric(20,4),
  FUND_TRAN_PFT NUMeric(20,4),
  in_TRAN_PFT NUMeric(20,4)
);
insert into mod_tran_pft_m
select pty_id,sum(fina_tran_pft),sum(fund_tran_pft),sum(in_tran_pft)
from mod_tran_pft_m_sub a
group by pty_id;

--计算客户的月最大成本收益率
CREATE GLOBAL TEMPORARY TABLE mod_max_cost_rate
as
select a.pty_id
	,decode( (a.tot_aset_bgn+a.pur_aset_bgn) + COALESCE(b.max_cost_aset_m,0),0,0,
          	((a.tot_aset_end+a.pur_aset_end) - (a.tot_aset_bgn+a.pur_aset_bgn) -COALESCE(b.in_aset_m,0)+COALESCE(b.out_aset_m,0) )
	 	/ ( (a.tot_aset_bgn+a.pur_aset_bgn) + COALESCE(b.max_cost_aset_m,0) )
	) as max_cost_rate_m
	,a.data_dt
    from   ms_cust_aset_m a
    left outer join  MS_CUST_CHAR_IO_ASET_M b
         on a.pty_id=b.pty_id and b.data_dt='$ETL_DATE'
    where a.data_dt='$ETL_DATE'
;  --月最大成本收益率


  Insert Into MS_CUST_ZL_COST_M
        (
               DATA_DT                         --数据日期
              ,PTY_ID                          --客户号
              ,pur_aset_bgn                    --月初资产
              ,pur_aset_end                    --月末资产
              ,fin_in_amt                      --月累计转入资产
              ,fin_out_amt                     --月累计转出资产
              ,aset_pft                        --月资产收益
              ,Max_Cost_Rate                   --月最大成本收益率
              ,MAX_COST_RATE_NUM               --月资产收益率排名
              ,STK_TRAN_CNT_M                  --股票月交易频次
              ,avg_aset_scale_m                --月平均持仓率
              ,RISK_HABIT_char                 --客户风险偏好
              ,HOLD_IDST_CHAR                  --客观偏好行业
              ,aset_pft_rate                   --本月收益与上月收益的比率（本月收益－上月收益）/上月收益
              ,hs_300_rate                     --沪深300指数涨跌幅
              ,JN_YIELD                        --金牛组合月收益
              ,pur_ttf_profit                 --天天发940018月收益
              ,pur_fina_profit                --理财产品月收益
              ,GP_TRAN_PFT                      --股票月收益
              ,fund_tran_pft                    --场外基金月收益
              ,lmt_TRAN_PFT                      --限售股票月收益
              ,zq_TRAN_PFT                      --债券月收益
	)
   SELECT '$ETL_DATE',
          a.pty_id,
          a.tot_aset_bgn+a.pur_aset_bgn as pur_aset_bgn,
          a.tot_aset_end+a.pur_aset_end as pur_aset_end,
          coalesce(h.in_aset_m, 0) as fin_in_amt,
          coalesce(h.out_aset_m, 0) as fin_out_amt,
          a.tot_aset_end + a.pur_aset_end - a.tot_aset_bgn - a.pur_aset_bgn - coalesce(h.in_aset_m, 0) + coalesce(h.out_aset_m, 0) as aset_pft,
          coalesce(j.max_cost_rate_m, 0) as max_cost_rate,
          decode(j.rk,null,0,1-j.rk/$v_cust_cnt::numeric) as MAX_COST_RATE_NUM,
          coalesce(k1.stk_tran_cnt_m, 0) as stk_tran_cnt_m,
          coalesce(l.avg_aset_scale_m, 0) as avg_aset_scale_m,
          m.risk_habit_cd2 as RISK_HABIT_char,
          k.hold_idst_char,
          round(decode(coalesce(n.aset_pft, 0),
                       0,
                       0,
                       (a.tot_aset_end + a.pur_aset_end - a.tot_aset_bgn - a.pur_aset_bgn -
                       coalesce(h.in_aset_m, 0) + coalesce(h.out_aset_m, 0) -
                       coalesce(n.aset_pft, 0)) / coalesce(n.aset_pft, 0)),
                4) as aset_pft_rate,
          round(coalesce(o.hs_300_rate, 0), 4) as hs_300_rate,
          coalesce(b.yield,0) as JN_yield,
          coalesce(c.occur_shares,0) as pur_ttf_profit,
          coalesce(x.fina_tran_pft,0) as pur_fina_profit,
          coalesce(d.gp_TRAN_PFT,0)+coalesce(x.in_tran_pft,0) as GP_TRAN_PFT,	--股票月收益
          coalesce(x.fund_tran_pft,0) as fund_tran_pft, --基金月收益
          0 as lmt_TRAN_PFT,	--限售股票月收益
          0 as zq_TRAN_PFT	--债券月收益
     FROM  MS_CUST_ASET_M a
     left join mod_hs_300 o on 1 = 1
    left join(
     SELECT CASE
           WHEN b.CAPITALTOTAL IS NULL THEN
            (a.CAPITALTOTAL - a.FUNDSIZEFIRST) / NULLIF(a.FUNDSIZEFIRST, 0)
           ELSE
            (a.CAPITALTOTAL - b.CAPITALTOTAL) / NULLIF(b.CAPITALTOTAL, 0)
         END AS YIELD --收益率
    FROM (SELECT A.FUNDSIZEFIRST, A.CAPITALTOTAL, A.PORTID
            FROM STA_PORT_SIMULATIONINFO A
           WHERE A.PUBDATE =
                 ( --本月最后一天
                  select max(pubdate)
                    from STA_PORT_SIMULATIONINFO
                   where portid = 1
                     and trunc(pubdate) >=
                         trunc(to_date('$ETL_DATE', 'YYYYMMDD'), 'mm')
                     and trunc(pubdate) <
                         trunc(add_months(to_date('$ETL_DATE', 'YYYYMMDD'), 1),
                               'mm'))
             AND A.PORTID = 1) a
    FULL JOIN (SELECT B.CAPITALTOTAL, B.PORTID
                 FROM STA_PORT_SIMULATIONINFO B
                WHERE B.PUBDATE =
                      ( --上月最后一天
                       select max(pubdate)
                         from STA_PORT_SIMULATIONINFO
                        where portid = 1
                          and trunc(pubdate) <
                              trunc(to_date('$ETL_DATE', 'YYYYMMDD'), 'mm'))
                  AND B.PORTID = 1) b
      ON a.PORTID = b.PORTID
      ) b on 1=1
     left join MS_CUST_CHAR_IO_ASET_M h
       on h.Data_Dt = '$ETL_DATE'
      and h.pty_id = a.pty_id
     left outer join (select a.pty_id,a.max_cost_rate_m,rank() over(order by a.max_cost_rate_m desc) as rk
                     from mod_max_cost_rate a
                     where a.data_dt = '$ETL_DATE'
                    ) j on a.pty_id=j.pty_id
     left join (Select a.pty_id
                       ,b.describe as hold_idst_char --行业偏好
                 From ms_cust_char_rank_m a
                 left outer join sta_code b on a.char_value=b.code
                Where a.char_id ='809050'
                  And a.data_dt = '$ETL_DATE' and a.rank=1
                ) k
       on k.pty_id = a.pty_id
     left join MS_CUST_CHAR_ASET_M l
       on l.data_dt = '$ETL_DATE'
      and l.pty_id = a.pty_id
left join (Select a.pty_id,b.describe as  risk_habit_cd2
                From ms_cust_char_obj_h a
                left outer join sta_code b on a.risk_habit_cd2=b.code
                where a.bgn_dt <='$ETL_DATE'
                      and a.end_dt > '$ETL_DATE') m --风险偏好
       on m.pty_id = a.pty_id
     left join MS_CUST_ZL_COST_M n
       on n.data_dt = '$v_month_last_end'
      and n.pty_id = a.pty_id
      left outer join ms_cust_char_tran_m k1 --交易频次
      on k1.pty_id=a.pty_id and k1.data_dt='$ETL_DATE'
       left join (
       select a.pty_id,
              sum(decode(a.busi_cd,
              '406000143',
              a.BARG_CNT,
              '406000142',
              -a.BARG_CNT,
              a.BARG_CNT)*coalesce(b.exchange_rate,1)) as occur_shares ---天天发增值收益
  from STA_FUND_JNLS a
  left join view_conv_rmb b on a.data_dt =b.data_dt and a.ccy=b.ccy
 where a.data_dt between '$v_bgn_dt' and '$ETL_DATE'
   and a.fund_id = 'TA940018'
   and a.busi_cd in ('406000142', '406000143')
   group by a.pty_id
  ) c on a.pty_id =c.pty_id
  left join mod_cust_pft_tmp d on a.pty_id =d.pty_id   --普通\限售\债券盈利
  left join mod_tran_pft_m x on a.pty_id =x.pty_id
    where a.data_dt = '$ETL_DATE';
  commit;

--SELECT ETL_PARTITION('EASYETL.MS_CUST_ZL_COST_M','M','$ETL_DATE','ANALYZE');
