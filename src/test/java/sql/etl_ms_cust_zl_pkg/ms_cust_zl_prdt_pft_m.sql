/*
 * 目 标 表: EASYETL.MS_CUST_ZL_PRDT_PFT_M
 * 源    表:
 * 脚本生成 : 曹炀
 * 生成日期 : 2015-05-04
 */

     SET v_month_bgn = select (substr('$ETL_DATE', 1, 6) || '01');
     SET v_Last_Dt =select to_char(to_date((substr('$ETL_DATE', 1, 6) || '01'),'yyyymmdd')-1,'yyyymmdd');
     COMMIT;

     DELETE FROM MS_CUST_ZL_PRDT_PFT_M WHERE DATA_DT = '$ETL_DATE';
     COMMIT;



  --创建临时表
  --1.融资融券交易汇总
  create global temporary table ms_cust_prdt_tran_fc_idx_d_TEMP as
      Select a.Pty_Id,
             a.Stk_Id As Prdt_Id,
             Sum(Decode(a.Busi_Cd, '406004002', a.Barg_Amt * b.Exchange_Rate, 0)) As Stk_Buy_Amt, --证券买入金额
             Sum(Decode(a.Busi_Cd, '406004002', a.Barg_Cnt, 0)) As Stk_Buy_Mnt, --证券买入数量
             Sum(Decode(a.Busi_Cd, '406004002', 1, 0)) As Stk_Buy_Cnt, --证券买入次数
             Sum(Decode(a.Busi_Cd, '406004002', a.Rake * b.Exchange_Rate, 0)) As Stk_Buy_Rake, --证券买入佣金
             Sum(Decode(a.Busi_Cd, '406004002', a.Pur_Rake * b.Exchange_Rate, 0)) As Stk_Buy_Pur_Rake, --证券买入净佣金
             Sum(Decode(a.Busi_Cd, '406004001', a.Barg_Amt * b.Exchange_Rate, 0)) As Stk_Sell_Amt, --证券卖出金额
             Sum(Decode(a.Busi_Cd, '406004001', a.Barg_Cnt, 0)) As Stk_Sell_Mnt, --证券卖出数量
             Sum(Decode(a.Busi_Cd, '406004001', 1, 0)) As Stk_Sell_Cnt, --证券卖出次数
             Sum(Decode(a.Busi_Cd, '406004001', a.Rake * b.Exchange_Rate, 0)) As Stk_Sell_Rake, --证券卖出佣金
             Sum(Decode(a.Busi_Cd, '406004001', a.Pur_Rake * b.Exchange_Rate, 0)) As Stk_Sell_Pur_Rake, --证券卖出净佣金
             -------------------------------------------------------
             Sum(Decode(a.Busi_Cd, '406004201', a.Barg_Cnt * Decode(coalesce(c.End_Price, 0), 0, coalesce(c.Nav, 0), coalesce(c.End_Price, 0)) * b.Exchange_Rate, 0)) As Assure_In_Amt, --担保转入金额
             Sum(Decode(a.Busi_Cd, '406004201', a.Barg_Cnt, 0)) As Assure_In_Mnt, --担保转入数量
             Sum(Decode(a.Busi_Cd, '406004201', 1, 0)) As Assure_In_Cnt, --担保转入次数
             Sum(Decode(a.Busi_Cd, '406004202', a.Barg_Cnt * Decode(coalesce(c.End_Price, 0), 0, coalesce(c.Nav, 0), coalesce(c.End_Price, 0)) * b.Exchange_Rate, 0)) As Assure_Out_Amt, --担保转出金额
             Sum(Decode(a.Busi_Cd, '406004202', a.Barg_Cnt, 0)) As Assure_Out_Mnt, --担保转出数量
             Sum(Decode(a.Busi_Cd, '406004202', 1, 0)) As Assure_Out_Cnt, --担保转出次数
             --------------------------------------------------------
             Sum(Decode(a.Busi_Cd, '406004211', a.Barg_Amt * b.Exchange_Rate, 0)) As Finance_Buy_Amt, --融资买入金额
             Sum(Decode(a.Busi_Cd, '406004211', a.Barg_Cnt, 0)) As Finance_Buy_Mnt, --融资买入数量
             Sum(Decode(a.Busi_Cd, '406004211', 1, 0)) As Finance_Buy_Cnt, --融资买入次数
             Sum(Decode(a.Busi_Cd, '406004211', a.Rake * b.Exchange_Rate, 0)) As Finance_Buy_Rake, --融资买入佣金
             Sum(Decode(a.Busi_Cd, '406004211', a.Pur_Rake * b.Exchange_Rate, 0)) As Finance_Buy_Pur_Rake, --融资买入净佣金
             Sum(Decode(a.Busi_Cd, '406004212', a.Barg_Amt * b.Exchange_Rate, 0)) As Finance_Sell_Amt, --融资卖出金额
             Sum(Decode(a.Busi_Cd, '406004212', a.Barg_Cnt, 0)) As Finance_Sell_Mnt, --融资卖出数量
             Sum(Decode(a.Busi_Cd, '406004212', 1, 0)) As Finance_Sell_Cnt, --融资卖出次数
             Sum(Decode(a.Busi_Cd, '406004212', a.Rake * b.Exchange_Rate, 0)) As Finance_Sell_Rake, --融资卖出佣金
             Sum(Decode(a.Busi_Cd, '406004212', a.Pur_Rake * b.Exchange_Rate, 0)) As Finance_Sell_Pur_Rake, --融资卖出净佣金
             ---------------------------------------------------------
             Sum(Decode(a.Busi_Cd, '406004213', a.Barg_Amt * b.Exchange_Rate, 0)) As Shortsell_Buy_Amt, --融券买入金额
             Sum(Decode(a.Busi_Cd, '406004213', a.Barg_Cnt, 0)) As Shortsell_Buy_Mnt, --融券买入数量
             Sum(Decode(a.Busi_Cd, '406004213', 1, 0)) As Shortsell_Buy_Cnt, --融券买入次数
             Sum(Decode(a.Busi_Cd, '406004213', a.Rake * b.Exchange_Rate, 0)) As Shortsell_Buy_Rake, --融券买入佣金
             Sum(Decode(a.Busi_Cd, '406004213', a.Pur_Rake * b.Exchange_Rate, 0)) As Shortsell_Buy_Pur_Rake, --融券买入净佣金
             Sum(Decode(a.Busi_Cd, '406004214', a.Barg_Amt * b.Exchange_Rate, 0)) As Shortsell_Sell_Amt, --融券卖出金额
             Sum(Decode(a.Busi_Cd, '406004214', a.Barg_Cnt, 0)) As Shortsell_Sell_Mnt, --融券卖出数量
             Sum(Decode(a.Busi_Cd, '406004214', 1, 0)) As Shortsell_Sell_Cnt, --融券卖出次数
             Sum(Decode(a.Busi_Cd, '406004214', a.Rake * b.Exchange_Rate, 0)) As Shortsell_Sell_Rake, --融券卖出佣金
             Sum(Decode(a.Busi_Cd, '406004214', a.Pur_Rake * b.Exchange_Rate, 0)) As Shortsell_Sell_Pur_Rake, --融券卖出净佣金
             ---------------------------------------------------------
             Sum(Decode(a.Busi_Cd, '406004215', a.Barg_Amt * b.Exchange_Rate, 0)) As Payoff_Buy_Amt, --平仓买入金额
             Sum(Decode(a.Busi_Cd, '406004215', a.Barg_Cnt, 0)) As Payoff_Buy_Mnt, --平仓买入数量
             Sum(Decode(a.Busi_Cd, '406004215', 1, 0)) As Payoff_Buy_Cnt, --平仓买入次数
             Sum(Decode(a.Busi_Cd, '406004215', a.Rake * b.Exchange_Rate, 0)) As Payoff_Buy_Rake, --平仓买入佣金
             Sum(Decode(a.Busi_Cd, '406004215', a.Pur_Rake * b.Exchange_Rate, 0)) As Payoff_Buy_Pur_Rake, --平仓买入净佣金
             Sum(Decode(a.Busi_Cd, '406004216', a.Barg_Amt * b.Exchange_Rate, 0)) As Payoff_Sell_Amt, --平仓卖出金额
             Sum(Decode(a.Busi_Cd, '406004216', a.Barg_Cnt, 0)) As Payoff_Sell_Mnt, --平仓卖出数量
             Sum(Decode(a.Busi_Cd, '406004216', 1, 0)) As Payoff_Sell_Cnt, --平仓卖出次数
             Sum(Decode(a.Busi_Cd, '406004216', a.Rake * b.Exchange_Rate, 0)) As Payoff_Sell_Rake, --平仓卖出佣金
             Sum(Decode(a.Busi_Cd, '406004216', a.Pur_Rake * b.Exchange_Rate, 0)) As Payoff_Sell_Pur_Rake, --平仓卖出净佣金
             Sum(Decode(a.Busi_Cd, '406004014', a.Barg_Cnt, 0)) As Allot_Stk_Mnt, --配股入帐数量
             Sum(Decode(a.Busi_Cd, '406004014', a.Barg_Cnt * a.Ent_Pri, 0)) As Allot_Stk_Amt, --配股入帐金额
             Sum(Decode(a.Busi_Cd, '406004015', a.Barg_Cnt, 0)) As Bonus_Stk_Mnt, --红股入帐数量
             Sum(Decode(a.Busi_Cd, '406004015', a.Barg_Cnt * Decode(coalesce(c.End_Price, 0), 0, coalesce(c.Nav, 0), coalesce(c.End_Price, 0)) * b.Exchange_Rate, 0)) As Bonus_Stk_Amt, --红股入帐金额
             Sum(Decode(a.Busi_Cd, '406004016', a.Barg_Cnt, 0)) As New_Stk_Mnt, --新股入帐数量
             Sum(Decode(a.Busi_Cd, '406004016', a.Barg_Cnt * a.Ent_Pri, 0)) As New_Stk_Amt, --新股入帐金额
             -------------------------------------------------------
             Sum(Decode(a.Busi_Cd, '406004206', a.Barg_Cnt * Decode(coalesce(c.End_Price, 0), 0, coalesce(c.Nav, 0), coalesce(c.End_Price, 0)) * b.Exchange_Rate, 0)) As Direct_Pay_Amt, --直接还券金额
             Sum(Decode(a.Busi_Cd, '406004206', a.Barg_Cnt, 0)) As Direct_Pay_Mnt, --直接还券数量
             Sum(Decode(a.Busi_Cd, '406004206', 1, 0)) As Direct_Pay_Cnt --直接还券次数
        From Sta_Fc_Jnls a
        Left Join Mf_Prdt_Mkt_d c
          On a.Stk_Id = c.Prdt_Id
         And a.Data_Dt = c.Data_Dt, View_Conv_Rmb b
       Where a.Busi_Cd In ('406004001', --证券卖出（股票交割、客户资金）统计交易量
                           '406004002', --证券买入（股票交割、客户资金）统计交易量
                           '406004201', --担保转入 统计交易量
                           '406004202', --担保转出 统计交易量
                           '406004211', --融资买入 统计交易量
                           '406004212', --融资卖出 统计交易量
                           '406004213', --融券买入 统计交易量
                           '406004214', --融券卖出 统计交易量
                           '406004215', --平仓买入 统计交易量
                           '406004216', --平仓卖出 统计交易量
                           '406004014', --配股入帐
                           '406004015', --红股入帐
                           '406004016', --新股入帐
                           '406004206'  --直接还券
                           )
         And a.Data_Dt = b.Data_Dt
         And a.Ccy = b.Ccy
         And a.Data_Dt Between '$v_month_bgn' And '$ETL_DATE'          --add by yzq 20150731
       Group By a.Pty_Id, a.Stk_Id;

  --2.普通账号月交易汇总 交易金额折算为人民币
  create global temporary table mf_cust_tran_tmp as
      SELECT pty_id,
			sys_source_cd,
                        prdt_id,
                        sum(Buy_Amt) as Buy_Amt,
                        sum(Sell_Amt) as Sell_Amt,
                        sum(Buy_Rake) as Buy_Rake,
                        sum(sell_Rake) as sell_Rake,
                        sum(buy_cnt) as buy_cnt,
                        sum(sell_cnt) as sell_cnt,
                        sum(Tran_Pft) as Tran_Pft
                   FROM (Select a.Pty_Id,
				a.sys_source_cd,
                                a.Prdt_Id,
                                a.Buy_Amt,
                                a.Sell_Amt,
                                a.Buy_Rake + coalesce(a.buy_fare, 0) As Buy_Rake,
                                a.sell_Rake + coalesce(a.sell_fare, 0) As sell_Rake,
                                a.buy_cnt,
                                a.sell_cnt,
                                0   as Tran_Pft  --现金分红
                           FROM Mf_Cust_Tran_Core_m a
                          where a.data_dt = '$ETL_DATE' And a.sys_source_cd in ( '01','04' ) --柜台和根网
				and a.prdt_id not like 'HG%'
                         Union All
                         --新股入账信息
			Select a.Pty_Id,
				'01' as sys_source_cd,
                                a.Stk_Id As Prdt_Id,
                                sum(decode(a.busi_cd, '406004021', 0, a.Barg_amt)*coalesce(b.exchange_rate,1)) As Buy_Amt,
                                SUM(decode(a.busi_cd, '406004021', a.Barg_amt, 0)*coalesce(b.exchange_rate,1)) As Sell_Amt,
                                0 As Buy_Rake,
                                0 As Sell_Rake,
                                SUM(decode(a.busi_cd, '406004021', 0, '406002434',0,1)) as buy_cnt,
                                SUM(decode(a.busi_cd, '406004021', 1, 0)) as sell_cnt,
                                0                 as Tran_Pft  --现金分红
                           From sta_stk_jnls a
			left outer join view_conv_rmb b on a.ccy=b.ccy and a.data_dt=b.data_dt
                          Where a.Data_Dt Between '$v_month_bgn' And '$ETL_DATE'
                            AND a.busi_cd IN ('406004034', '406004021', '406004022','406004121', '406002434')  --股息红利税补缴
                          group by a.pty_id, a.Stk_Id
                         Union All
                         --配股，红股，分红
                         Select a.Pty_Id,
				a.sys_source_cd,
                                a.Prdt_Id,
                                (a.Tran_In + a.Allot_Share_Amt) *
                                b.Exchange_Rate As Buy_Amt, --转托管入金额配股金额。红股也不计入买入金额，目的是：有红股时，买入均价不变
                                a.Tran_Out * b.Exchange_Rate As Sell_Amt, --转托管出金额
                                0 As Buy_Rake,
                                0 As Sell_Rake,
                                0 As Buy_cnt, --转托管入、配股数量计入买入次数，红股不计入买入次数
                                0 As Sell_cnt, --转托管出次数
                                a.Bonus_Amt * b.Exchange_Rate As Tran_Pft --现金分红
                           From Mf_Cust_sys_Fin_d a, View_Conv_Rmb b
                          Where a.Data_Dt >= '$v_month_bgn'
                            And a.Data_Dt <= '$ETL_DATE'
                            And a.Data_Dt = b.Data_Dt
                            And a.Ccy = b.Ccy
                         Union All
                         Select a.Pty_Id,
				'01' as sys_source_cd,
                                a.Prdt_Id,
                                a.Assure_Out_Amt As Buy_Amt, --担保转入金额
                                a.Assure_In_Amt  As Sell_Amt, --担保转出金额
                                0                As Buy_Rake,
                                0                As Sell_Rake,
                                a.Assure_Out_cnt As Buy_cnt, --担保转入次数
                                a.Assure_In_cnt  As Sell_cnt, --担保转出次数
                                0                 as Tran_Pft  --现金分红
                           From ms_cust_prdt_tran_fc_idx_d_TEMP a --客户交易信息
                         Union All
                         --回购交易累计金额
			Select a.Pty_Id,
				'01' as sys_source_cd,
                                case when a.stk_id like '%204%' then 'ZQ888880' else 'ZQ131990' end as prdt_id,
                                0 As buy_Amt,
                                SUM(a.CLEAR_BALANCE) As sell_Amt,
                                0 As Buy_Rake,
                                0 As Sell_Rake,
                                0 as buy_cnt,
                                0 as sell_cnt,
                                0                 as Tran_Pft  --现金分红
                           From sta_stk_jnls a
			left outer join view_conv_rmb b on a.ccy=b.ccy and a.data_dt=b.data_dt
                          Where a.Data_Dt Between '$v_month_bgn' And '$ETL_DATE'
                            AND a.busi_cd IN ('406004105','406004106','406004103','406004104')
			    and a.stk_id in ('HG204001','HG204002','HG204003','HG204004','HG204007','HG204014','HG204028','HG204091','HG204182',
			    			'204001','204002','204003','204004','204007','204014','204028','204091','204182',
			    			'HG131800','HG131801','HG131802','HG131803','HG131805','HG131806','HG131809','HG131810','HG131811',
			    			'131800','131801','131802','131803','131805','131806','131809','131810','131811')
                          group by a.pty_id, 3
                  )fo
                  group by pty_id,sys_source_cd, prdt_id
	;

     ----普通账户下的股票产品
     Insert Into MS_CUST_ZL_PRDT_PFT_M
        (
            DATA_DT
           ,PTY_ID  --客户号
           ,PRDT_ID --产品号
           ,FLAG    ---普通和信用标识
           ,HOLD_DAYS --当月持股天数
           ,BUY_CNT  --买入数量
           ,SELL_CNT --卖出数量
           ,TRAN_PFT --盈利金额
        )
    SELECT '$ETL_DATE',
           a.pty_id,
           a.prdt_id,
	   case when a.sys_source_cd='01' then '001'
	   	when a.sys_source_cd='04' then '004'
	   	when a.sys_source_cd='07' then '007'
		else '001'
	   end as flag,
           a.days,
           COALESCE(b.buy_cnt, 0) as buy_cnt,
           COALESCE(b.sell_cnt, 0) as sell_cnt,
           a.mkt_val - COALESCE(e.mkt_val,0) + COALESCE(b.sell_amt, 0) - COALESCE(b.buy_amt, 0) +
           COALESCE(b.Tran_Pft, 0) - COALESCE(b.Buy_Rake, 0)-COALESCE(b.sell_Rake,0) as tran_pft --期末市值-期初市值+卖出金额-买入金额+现金分红-期间买入卖出该股票的佣金之和
      FROM (SELECT pty_id,
                   prdt_id,
		   sys_source_cd,
                   sum(days) as days,
                   sum(mkt_val) as mkt_val  --期末市值
              FROM (	SELECT	a.pty_id,
				a.sys_source_cd,
                     		a.prdt_id,
				--月末日期-(持仓日期或者月初日期的最小值)+1=当前持仓产品的持仓天数
                     		to_date(a.data_dt, 'yyyymmdd') - to_date(Greatest(hold_dt, '$v_month_bgn'), 'yyyymmdd') + 1 as days,
                     		a.mkt_val * coalesce(b.exchange_rate,1) as mkt_val
                	FROM ms_cust_ccy_prdt_anahold_d a
			left outer join view_conv_rmb b on a.data_dt=b.data_dt and a.ccy=b.ccy
               		where a.data_dt = '$ETL_DATE' and a.sys_source_cd in ('01','04','07') --普通\根网\限售
              	    union all
              		SELECT	a.pty_id,
				a.sys_source_cd,
                     		a.prdt_id,
				--取(清仓日期-月初日期+1) 与 持仓日期的最小值=当月已经清仓产品的持仓天数
                     		least((to_date(a.data_dt, 'yyyymmdd') - to_date('$v_month_bgn', 'yyyymmdd') + 1), hold_days) as days,
                     		0 as mkt_val
                	FROM ms_cust_ccy_prdt_anaunhold_d a
               		where a.data_dt between '$v_month_bgn' and '$ETL_DATE' and a.sys_source_cd in ('01','04','07') --普通\根网\限售
		    union all
              		SELECT	a.pty_id,
				'01' as sys_source_cd,
                     		a.stk_id as prdt_id,
				--取(清仓日期-月初日期+1) 与 持仓日期的最小值=当月已经清仓产品的持仓天数
                     		0 as days,
                     		0 as mkt_val
			from sta_stk_jnls a
               		where a.data_dt between '$v_month_bgn' and '$ETL_DATE'
		    )fw
            	group by pty_id, prdt_id,sys_source_cd
            ) a --客户本月持仓过或者交易过的产品
      left join ( SELECT	a.pty_id,
				a.sys_source_cd,
                     		a.prdt_id,
				--月末日期-(持仓日期或者月初日期的最小值)+1=当前持仓产品的持仓天数
                     		to_date(a.data_dt, 'yyyymmdd') - to_date(Greatest(hold_dt, '$v_month_bgn'), 'yyyymmdd') + 1 as days,
                     		a.mkt_val * coalesce(b.exchange_rate,1) as mkt_val
                	FROM ms_cust_ccy_prdt_anahold_d a
			left outer join view_conv_rmb b on a.data_dt=b.data_dt and a.ccy=b.ccy
               		where a.data_dt = '$v_Last_Dt' and a.sys_source_cd in ('01','04','07') --普通\根网\限售
		) e --客户上月末持仓
	on a.pty_id =e.pty_id and a.prdt_id =e.prdt_id and a.sys_source_cd=e.sys_source_cd
      left join mf_cust_tran_tmp b on a.pty_id = b.pty_id and a.prdt_id = b.prdt_id and a.sys_source_cd=b.sys_source_cd --普通账户月交易汇总
      Left Join Mf_Prdt_Attr_h d
        On a.Prdt_Id = d.Prdt_Id
       And d.bgn_dt <= '$ETL_DATE'
       and d.end_dt > '$ETL_DATE'
     Where d.up_prdt_type_id in ('PA030000','PA040000','PA080000') and d.prdt_type_id <> 'PA040500'
	;

--普通账户下无持仓债券产品的收益(债券质押入库、出库)
     Insert Into MS_CUST_ZL_PRDT_PFT_M
        (
            DATA_DT
           ,PTY_ID  --客户号
           ,PRDT_ID --产品号
           ,FLAG    ---普通和信用标识
           ,HOLD_DAYS --当月持股天数
           ,BUY_CNT  --买入数量
           ,SELL_CNT --卖出数量
           ,TRAN_PFT --盈利金额
        )
    SELECT '$ETL_DATE',
           a.pty_id,
           a.prdt_id,
	   '001' as flag,
           99 as days,
           sum(a.buy_cnt),
           sum(a.sell_cnt),
           sum(a.sell_amt - a.buy_amt - a.Buy_Rake - a.sell_Rake) as tran_pft --卖出金额-买入金额-期间买入卖出该股票的佣金之和
     from mf_cust_tran_core_m a
      Left Join Mf_Prdt_Attr_h d
        On a.Prdt_Id = d.Prdt_Id
       And d.bgn_dt <= '$ETL_DATE'
       and d.end_dt > '$ETL_DATE'
     Where d.up_prdt_type_id in ('PA030000') and a.sys_source_cd='01'
	and not exists (select 1 from ms_cust_zl_prdt_pft_m t where t.data_dt='$ETL_DATE' and a.pty_id=t.pty_id and a.prdt_id=t.prdt_id)
	and a.data_dt='$ETL_DATE'
	group by a.pty_id,a.prdt_id
	;

       ----信用账户下的股票产品
     Insert Into MS_CUST_ZL_PRDT_PFT_M
        (
            DATA_DT
           ,PTY_ID  --客户号
           ,PRDT_ID --产品号
           ,FLAG    ---普通和信用标识
           ,HOLD_DAYS --当月持股天数
           ,BUY_CNT  --买入数量
           ,SELL_CNT --卖出数量
           ,TRAN_PFT --盈利金额
        )
       SELECT '$ETL_DATE',
           a.pty_id,
           a.prdt_id,
           '105' as flag,
           a.days,
           COALESCE(c.buy_cnt, 0) as buy_cnt,
           COALESCE(c.sell_cnt, 0) as sell_cnt,
	   case when d.up_prdt_type_id='PA030000' then
           		COALESCE(e.mkt_val_end,0) - COALESCE(e.mkt_val_bgn,0) - COALESCE(c.Buy_Amt, 0) + COALESCE(c.Sell_Amt, 0)
			-COALESCE(c.buy_rake,0)-COALESCE(c.sell_Rake,0)-COALESCE(c.Tran_Fare,0)
		else
           		COALESCE(e.hold_cnt_end,0)*COALESCE(g.end_price,0)-COALESCE(e.hold_cnt_bgn,0)*COALESCE(f.end_price,0)- COALESCE(c.Buy_Amt, 0) + COALESCE(c.Sell_Amt, 0)
			-COALESCE(c.buy_rake,0)-COALESCE(c.sell_Rake,0)-COALESCE(c.Tran_Fare,0)
	   end as tran_pft --期末市值-期初市值-期间买入金额+期间卖出金额-买入佣金-卖出佣金-利息

    FROM (
        SELECT     pty_id,
                   prdt_id,
                   sum(days) as days
        FROM (
              SELECT a.pty_id,
                     a.prdt_id,
                     to_date(a.data_dt, 'yyyymmdd') -
                     to_date(Greatest(hold_dt, '$v_month_bgn'), 'yyyymmdd') + 1 as days --月末日期-(持仓日期或者月初日期的最小值)+1=当前持仓产品的持仓天数
                FROM MS_CUST_PRDT_FC_HOLD_D a
               where a.data_dt = '$ETL_DATE'
               And (a.Hold_Cnt <> 0 Or a.Deb_Mnt <> 0)
              union all
              SELECT a.pty_id,
                     a.prdt_id,
                     least((to_date(a.data_dt, 'yyyymmdd') -
                           to_date('$v_month_bgn', 'yyyymmdd') + 1),
                           hold_days) as days --取(清仓日期-月初日期+1) 与 持仓日期的最小值=当月已经清仓产品的持仓天数
                FROM MS_CUST_PRDT_FC_UNHOLD_D a
               where a.data_dt in
               (select d_date
                        from ana_dim_date
                       where d_date between '$v_month_bgn' and '$ETL_DATE')
              )fw
              group by pty_id, prdt_id
	 ) a
     left join (
              SELECT a.pty_id,
                     a.prdt_id,
                     sum(decode(a.data_dt,
                            '$v_Last_Dt',
                            a.Hold_Cnt - a.Stc_Deb_Mnt,
                            0)) as hold_cnt_bgn, ---期初持仓数量
                     sum(decode(a.data_dt,
                            '$ETL_DATE',
                            a.Hold_Cnt - a.Stc_Deb_Mnt,
                            0)) as hold_cnt_end, --期末持仓数量
                     sum(decode(a.data_dt,
                            '$v_Last_Dt',
                            a.mkt_val,
                            0)) as mkt_val_bgn, ---期初市值
                     sum(decode(a.data_dt,
                            '$ETL_DATE',
                            a.mkt_val,
                            0)) as mkt_val_end --期末市值
                FROM Ms_Cust_Prdt_Fc_Hold_d a
               where a.data_dt in ('$v_Last_Dt', '$ETL_DATE')
               group by a.pty_id, a.prdt_id
              ) e on a.pty_id =e.pty_id and a.prdt_id =e.prdt_id
     left join  Mf_Prdt_Mkt_d f
                   On f.Data_Dt = '$v_Last_Dt'
                  And f.Prdt_Id = a.Prdt_Id
     left join  Mf_Prdt_Mkt_d g
                   On g.Data_Dt = '$ETL_DATE'
                  And g.Prdt_Id = a.Prdt_Id
     left join (  Select
                     Pty_Id,
                     Prdt_Id,
                     Sum(Buy_cnt) As Buy_cnt,
                     Sum(Buy_Amt) As Buy_Amt,
                     Sum(Buy_Rake) As Buy_Rake,
                     Sum(sell_cnt) As sell_cnt,
                     Sum(Sell_Amt) As Sell_Amt,
                     Sum(Sell_Rake) As Sell_Rake,
                     Sum(Tran_Fare) As Tran_Fare
                From (Select
                             a.Pty_Id,
                             a.Stk_Id As Prdt_Id,
                             sum(decode(a.Tran_Dir_Cd, '603010',decode(a.Busi_Cd,'406004014',0,'406002434',0,1),0)) AS BUY_CNT,   --买入次数
                             Sum(Decode(a.Tran_Dir_Cd, '603010', (Case When a.Busi_Cd In ('406004014', '406004016') Then a.Ent_Pri * a.Barg_Cnt When a.Busi_Cd In ('406004201', '406004202', '406004015') Then a.Barg_Cnt * d.End_Price * c.Exchange_Rate Else a.Barg_Amt * c.Exchange_Rate End), 0)) As Buy_Amt,
                             Sum(Decode(a.Tran_Dir_Cd, '603010', (a.Rake + coalesce(a.fare1, 0) + coalesce(a.fare2, 0) + coalesce(a.fare3, 0) + coalesce(a.farex, 0)) * c.Exchange_Rate, 0)) As Buy_Rake,
                             sum(decode(a.Tran_Dir_Cd, '603020',decode(a.Busi_Cd,'406004014',0,'406004018',0,1),0)) AS sell_CNT,   --卖出次数
                             Sum(Decode(a.Tran_Dir_Cd, '603020', (Case When a.Busi_Cd In ('406004201', '406004202') Then a.Barg_Cnt * d.End_Price * c.Exchange_Rate Else a.Barg_Amt * c.Exchange_Rate End), 0)) As Sell_Amt,
                             Sum(Decode(a.Tran_Dir_Cd, '603020', (a.Rake + coalesce(a.fare1, 0) + coalesce(a.fare2, 0) + coalesce(a.fare3, 0) + coalesce(a.farex, 0)) * c.Exchange_Rate, 0)) As Sell_Rake,
                             0 As Tran_Fare
                        From Sta_Fc_Jnls a
                        Left Join View_Conv_Rmb c
                          On a.Data_Dt = c.Data_Dt
                         And a.Ccy = c.Ccy
                        Left Join Mf_Prdt_Mkt_d d
                          On a.Data_Dt = d.Data_Dt
                         And a.Stk_Id = d.Prdt_Id
                       Where a.Busi_Cd In ('406004002', --证券买入（股票交割、客户资金）
                                           '406004201', --担保转入
                                           --'406004206', --直接还券
                                           '406004211', --融资买入
                                           '406004213', --融券买入
                                           '406004215', --平仓买入
                                           '406004014', --配股入帐
                                           --'406004015', --红股入帐
                                           --'406004016',  --新股入帐
                                           '406004001', --证券卖出（股票交割、客户资金）
                                           '406004202', --担保转出
                                           '406004212', --融资卖出
                                           '406004214', --融券卖出
                                           '406004216' --平仓卖出
										   '406004034',
                                           '406004021',
                                           '406004022',
					   '406004018', --股息入帐
					   '406002434', --股息红利税补缴
					   '406004121'  --新股IPO配售确认        add by xumeng 20161202 增加新股中签的买入金额
                                           )
                         And a.Data_Dt In (Select d_date From ana_dim_date Where d_date Between  '$v_month_bgn' and '$ETL_DATE')
                       Group By  a.Pty_Id, a.Stk_Id
                       Union All
                      Select
                             a.Pty_Id,
                             a.Prdt_Id,
                             0 as buy_cnt,
                             0 As Buy_Amt,
                             0 As Buy_Rake,
                             0 as sell_cnt,
                             0 As Sell_Amt,
                             0 As Sell_Rake,
                             Sum(a.Deb_Int_d) As Tran_Fare
                        From Ms_Cust_Prdt_Fc_Hold_d a --客户持仓信息
                       Where a.Data_Dt In (Select D_Date From Ana_Dim_Date Where D_Date Between  '$v_month_bgn' and '$ETL_DATE')
                         And (a.Hold_Cnt <> 0 Or a.Deb_Mnt <> 0)
                       Group By a.Pty_Id, a.Prdt_Id)fw
                  Group By  Pty_Id, Prdt_Id
       ) c   on a.pty_id = c.pty_id  and a.prdt_id = c.prdt_id
        Left Join Mf_Prdt_Attr_h d
        On a.Prdt_Id = d.Prdt_Id
       And d.bgn_dt <= '$ETL_DATE'
     and d.end_dt > '$ETL_DATE'
     Where d.up_prdt_type_id in ('PA030000','PA040000','PA080000') and d.prdt_type_id <> 'PA040500'
	;

     commit;

--表分析
EXECEPTION_RAISE_OFF;
vacuum analyze easyetl.MS_CUST_ZL_PRDT_PFT_M;
EXECEPTION_RAISE_ON;
