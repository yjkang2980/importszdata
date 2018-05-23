/*
 * 目 标 表: EASYETL.MS_CUST_CCY_ASET_D
 * 源    表:
 * 脚本生成 : 曹炀
 * 生成日期 : 2015-04-22
 */
--清理本期数据
DELETE FROM MS_CUST_CCY_ASET_D WHERE DATA_DT = '$ETL_DATE';
COMMIT;

INSERT INTO MS_CUST_CCY_ASET_D
(
       DATA_DT,
       PTY_ID,
       CCY,
       TOT_ASET,
       TOT_MKT_VAL,
       BAL,
       AVL_AMT,
       FRZ_AMT,
       DRAW_AMT,
       MKT_PFT,
       STK_MKT_VAL,
       ETF_MKT_VAL,
       LMT_MKT_VAL,
       DIR_MKT_VAL,
       FC_MKT_VAL,
       FUND_MKT_VAL,
       FINA_MKT_VAL,
       FUND_IN_TRANSIT,
       SH_MKT_VAL,
       SZ_MKT_VAL
)
Select '$ETL_DATE'
      ,pty_id
      ,CCY
      ,sum(TOT_ASET)                              as TOT_ASET
      ,sum(TOT_MKT_VAL)                           as TOT_MKT_VAL
      ,sum(BAL)                                   as BAL
      ,sum(AVL_AMT)                               as AVL_AMT
      ,sum(FRZ_AMT)                               as FRZ_AMT
      ,sum(DRAW_AMT)                              as DRAW_AMT
      ,sum(MKT_PFT)                               as MKT_PFT
      ,sum(STK_MKT_VAL)                           as STK_MKT_VAL
      ,sum(ETF_MKT_VAL)                           as ETF_MKT_VAL
      ,sum(LMT_MKT_VAL)                           as LMT_MKT_VAL
      ,sum(DIR_MKT_VAL)                           as DIR_MKT_VAL
      ,sum(FC_MKT_VAL)                            as FC_MKT_VAL
      ,sum(FUND_MKT_VAL)                          as FUND_MKT_VAL
      ,sum(FINA_MKT_VAL)                          as FINA_MKT_VAL
      ,decode(sign(Sum(FUND_IN_TRANSIT)),-1,0,Sum(FUND_IN_TRANSIT))  As FUND_IN_TRANSIT
      ,sum(SH_MKT_VAL)                            as SH_MKT_VAL        --沪市市值
      ,sum(SZ_MKT_VAL)                            as SZ_MKT_VAL        --深市市值
From
(
    Select a.pty_id
          ,a.ccy
          ,a.bal        As TOT_ASET
          ,0            As TOT_MKT_VAL
          ,a.bal        As BAL
          ,a.avl_amt    As AVL_AMT
          ,a.frz_amt    As FRZ_AMT
          ,a.draw_amt   As DRAW_AMT
          ,0            As MKT_PFT
          ,0            As STK_MKT_VAL
          ,0            As ETF_MKT_VAL
          ,0            As LMT_MKT_VAL
          ,0            As DIR_MKT_VAL
          ,0            As FC_MKT_VAL
          ,0            As FUND_MKT_VAL
          ,0            As FINA_MKT_VAL
          ,0            As FUND_IN_TRANSIT
          ,0            as SH_MKT_VAL
          ,0            as SZ_MKT_VAL
      From mf_cust_cash_h a
     Where a.bgn_dt <= '$ETL_DATE' And a.end_dt > '$ETL_DATE' And
           ( a.bal != 0.00 Or a.avl_amt != 0.00 Or a.frz_amt != 0.00 Or a.draw_amt != 0.00 )

     Union All

    Select a.pty_id
          ,case when c.PRDT_TYPE_ID='PA040300' then '401156'
                      else a.ccy
                 end   as ccy
	  ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*a.mkt_val       As TOT_ASET
	  ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*a.mkt_val       As TOT_MKT_VAL
          ,0               As BAL
          ,0               As AVL_AMT
          ,0               As FRZ_AMT
          ,0               As DRAW_AMT
          ,a.mkt_pft       As MKT_PFT
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '01', a.mkt_val ,0) As STK_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '04', a.mkt_val ,0) As ETF_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '07', a.mkt_val ,0) As LMT_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '06', a.mkt_val ,0) As DIR_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '05', a.mkt_val ,0) As FC_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '02', a.mkt_val ,0) As FUND_MKT_VAL
          ,decode(c.PRDT_TYPE_ID,'PA040300',coalesce(b.exchange_rate,1),1)*decode(a.sys_source_cd, '03', a.mkt_val ,0) As FINA_MKT_VAL
          ,0                                       As FUND_IN_TRANSIT
          ,case when c.MARKET_ID in ('602010','602040','602050') and c.UP_PRDT_TYPE_ID='PA040000'
                then a.mkt_val
                else 0 end as  SH_MKT_VAL        --沪市市值
          ,case when c.MARKET_ID in ('602020','602030','602060') and c.UP_PRDT_TYPE_ID='PA040000'
                then a.mkt_val
                else 0 end as  SZ_MKT_VAL        --深市市值

      From mf_cust_hold_d a
	Left Outer Join view_conv_rmb b On b.ccy='401344' And b.data_dt = '$ETL_DATE'
          ,mf_prdt_attr_h c
     Where a.data_dt = '$ETL_DATE' And
           (a.mkt_val != 0.00 Or a.mkt_pft != 0.00)
       and a.prdt_id = c.prdt_id
       and c.bgn_dt <= '$ETL_DATE' and c.end_dt > '$ETL_DATE'
) a
Group By pty_id,CCY;


COMMIT;


--表分析
EXECEPTION_RAISE_OFF;
select etl_partition('EASYETL.MS_CUST_CCY_ASET_D', 'M', '$ETL_DATE', 'ANALYZE');
EXECEPTION_RAISE_ON;
