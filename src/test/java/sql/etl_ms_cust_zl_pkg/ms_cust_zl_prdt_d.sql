/*
 * 目 标 表: EASYETL.MS_CUST_ZL_PRDT_D
 * 源    表:
 * 脚本生成 : 徐猛
 * 生成日期 : 2016-10-11
 */

	DELETE FROM MS_CUST_ZL_PRDT_D where data_dt='$ETL_DATE';


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
         And a.Data_Dt = '$ETL_DATE'
         And b.Data_Dt = '$ETL_DATE'
       Group By a.Pty_Id, a.Stk_Id;

	--普通股票交易记录
	create temp table temp_gp_tran_1 as
      		SELECT	a.pty_id
                        ,a.prdt_id
                        ,sum(a.buy_cnt) as buy_cnt	--买入次数
                        ,sum(a.sell_cnt) as sell_cnt	--卖出次数
                   FROM (Select a.Pty_Id,
                                a.Prdt_Id,
                                a.buy_cnt,
                                a.sell_cnt
                           FROM Mf_Cust_Tran_Core_d a
                          where a.data_dt = '$ETL_DATE'
                            And a.Sys_Source_Cd <> '05' --排除融资融券系统的交易
                         Union All
                         --新股入账信息
                         Select a.Pty_Id,
                                a.Stk_Id As Prdt_Id,
                                SUM(decode(a.busi_cd, '406004021', 0, 1)) as buy_cnt,
                                SUM(decode(a.busi_cd, '406004021', 1, 0)) as sell_cnt
                           From sta_stk_jnls a
                          Where a.Data_Dt = '$ETL_DATE'
                            AND a.busi_cd IN ('406004034', '406004021', '406004022')
                          group by a.pty_id, a.Stk_Id
                         Union All
                         Select a.Pty_Id,
                                a.Prdt_Id,
                                a.Assure_Out_cnt As Buy_cnt, --担保转入次数
                                a.Assure_In_cnt  As Sell_cnt --担保转出次数
                           From ms_cust_prdt_tran_fc_idx_d_TEMP a --客户交易信息
			) a
	      Left Join Mf_Prdt_Attr_h b
		On a.Prdt_Id = b.Prdt_Id
	       And b.bgn_dt <= '$ETL_DATE'
	       and b.end_dt > '$ETL_DATE'
     		Where	b.up_prdt_type_id = 'PA040000'
              group by a.pty_id, a.prdt_id
	;

	--融资融券股票交易记录
	create temp table temp_gp_tran_2 as
      		SELECT	a.pty_id
                        ,a.prdt_id
                        ,sum(a.buy_cnt) as buy_cnt	--买入次数
                        ,sum(a.sell_cnt) as sell_cnt	--卖出次数
              	from	(Select
                             a.Pty_Id,
                             a.Stk_Id As Prdt_Id,
                             sum(decode(a.Tran_Dir_Cd, '603010',decode(a.Busi_Cd,'406004014',0,1),0)) AS BUY_CNT,   --买入次数
                             sum(decode(a.Tran_Dir_Cd, '603020',decode(a.Busi_Cd,'406004014',0,1),0)) AS sell_CNT   --卖出次数
                        From Sta_Fc_Jnls a
                       Where a.Busi_Cd In ('406004002', --证券买入（股票交割、客户资金）
                                           '406004201', --担保转入
                                           --'406004206', --直接还券
                                           '406004211', --融资买入
                                           '406004213', --融券买入
                                           '406004215', --平仓买入
                                           '406004014', --配股入帐
                                           --'406004015', --红股入帐
                                          -- '406004016',  --新股入帐
                                           '406004001', --证券卖出（股票交割、客户资金）
                                           '406004202', --担保转出
                                           '406004212', --融资卖出
                                           '406004214', --融券卖出
                                           '406004216', --平仓卖出
                                           '406004034',
                                           '406004021',
                                           '406004022'
                                           )
                         And a.Data_Dt = '$ETL_DATE'
                       Group By  a.Pty_Id, a.Stk_Id
       			) a
	      Left Join Mf_Prdt_Attr_h b
		On a.Prdt_Id = b.Prdt_Id
	       And b.bgn_dt <= '$ETL_DATE'
	       and b.end_dt > '$ETL_DATE'
     		Where	b.up_prdt_type_id = 'PA040000'
              group by a.pty_id,a.prdt_id
	;


     	INSERT INTO MS_CUST_ZL_PRDT_D (
		DATA_DT
       		,PTY_ID
       		,BUY_CNT
       		,sell_cnt
     	)
     	SELECT	'$ETL_DATE'
            	,A.PTY_ID
            	,sum(buy_cnt) buy_cnt
            	,sum(sell_cnt)  sell_cnt
     	FROM	(
		select	pty_id,buy_cnt,sell_cnt from temp_gp_tran_1
		union all
		select	pty_id,buy_cnt,sell_cnt from temp_gp_tran_2
		) a
     	GROUP BY A.PTY_ID
     	;
     	commit;


SELECT ETL_PARTITION('EASYETL.MS_CUST_ZL_PRDT_D','M','$ETL_DATE','ANALYZE');

