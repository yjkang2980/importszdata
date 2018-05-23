/*  客户月初到当前的收益
 * 目 标 表: EASYETL.MS_CUST_ZL_COST_D
 * 源    表:
 * 脚本生成 : 徐猛
 * 生成日期 : 2016-09-23
 */


--设置参数值
set v_month_bgn = select substr('$ETL_DATE', 1, 6) || '01';
set v_6m_bgn = select to_char(add_months(to_date('$ETL_DATE','yyyymmdd'),-6),'yyyymmdd');
set v_month_last_end = select to_char(trunc(to_date('$ETL_DATE', 'yyyymmdd'), 'MM') - 1,'yyyymmdd');
commit;

--清理6个月之前的数据,清理当月数据
	DELETE FROM MS_CUST_ZL_COST_D ;


     /*求月转入转出资产,收益率*/
        --客户资金转入转出日明细
	CREATE GLOBAL TEMPORARY TABLE cust_char_io_aset_x as
             SELECT pty_id
                    ,data_dt
                    ,SUM(fin_in_amt)      AS fin_in_amt
                    ,SUM(fin_out_amt)     AS fin_out_amt
                    ,sum(max_cost)        as max_cost
             FROM
             (
                 SELECT a.pty_id
                        ,a.data_dt
                        ,a.Cash_In + a.Tran_In + a.Assign_In+a.lmt_in_amt AS fin_in_amt
                        ,a.Cash_Out + a.Tran_Out + a.Assign_Out AS fin_out_amt
                        ,a.Cash_In + a.Tran_In + a.Assign_In +a.lmt_in_amt - (a.Cash_Out + a.Tran_Out + a.Assign_Out) as max_cost
                 FROM MS_CUST_FIN_D a --普通账户
                 WHERE a.data_dt between '$v_month_bgn' AND '$ETL_DATE'
                 Union All
                 Select a.pty_id
                        ,a.Data_Dt
                        ,a.cash_in As Fin_In_Amt
                        ,a.cash_out As Fin_Out_Amt
                        ,a.cash_in - a.cash_out as max_cost
                 From Vw_Ms_Cust_Fc_Fin_Idx_d a --信用账户
                 WHERE a.data_dt between '$v_month_bgn' AND '$ETL_DATE'
             ) a
             GROUP BY pty_id,data_dt
	;

        --客户资金转入转出日明细，及客户当月日累计转入转出资金差值
	CREATE GLOBAL TEMPORARY TABLE cust_char_io_aset_y as
             select pty_id,data_dt,fin_in_amt,fin_out_amt,sum(max_cost) over(partition by pty_id order by data_dt) as max_cost
             from cust_char_io_aset_x x
	;
	CREATE GLOBAL TEMPORARY TABLE MOD_MS_CUST_CHAR_IO_ASET_D as
        Select
              '$ETL_DATE'		as data_dt                      --数据日期
              ,y.PTY_ID                        				--客户号
              ,sum(y.fin_in_amt)     	as in_aset_m			--转入资产月累计
              ,sum(y.fin_out_amt)    	as out_aset_m			--转出资产月累计
              ,(case when max(y.max_cost) > 0 then max(y.max_cost)  else 0 end )       as MAX_COST_ASET_M  --调整成本
        From  cust_char_io_aset_y y
        group by y.pty_id
	;
        commit;

     /*求月资产收益和月最大成本收益*/
     Insert Into MS_CUST_ZL_COST_D
        (
               DATA_DT                         --数据日期
              ,PTY_ID                          --客户号
              ,aset_pft                        --月资产收益
              ,Max_Cost_Rate                   --月最大成本收益率
        )
     SELECT '$ETL_DATE',
            a.pty_id,
            a.tot_aset_end + a.pur_aset_end - a.tot_aset_bgn - a.pur_aset_bgn - coalesce(b.in_aset_m, 0) + coalesce(b.out_aset_m, 0) as aset_pft,
            decode((a.tot_aset_bgn+a.pur_aset_bgn)+coalesce(b.max_cost_aset_m,0),0,0,
                       ((a.tot_aset_end+a.pur_aset_end)-(a.tot_aset_bgn+a.pur_aset_bgn)-coalesce(b.IN_ASET_M,0)+coalesce(b.OUT_ASET_M,0))
                       /((a.tot_aset_bgn+a.pur_aset_bgn)+coalesce(b.max_cost_aset_m,0))
               ) as max_cost_rate_m  --最大成本收益率
     FROM
          (SELECT '$ETL_DATE',
           a.pty_id,
           coalesce(b.tot_aset, 0) as tot_aset_end,
           coalesce(d.tot_aset, 0) as tot_aset_bgn,
           coalesce(e.pur_aset, 0) as pur_aset_end,
           coalesce(g.pur_aset, 0) as pur_aset_bgn
      FROM mf_cust_attr_h a
      left join Ms_Cust_Aset_d b
        On b.Data_Dt = '$ETL_DATE'
       and b.pty_id = a.pty_id
      left join Ms_Cust_Aset_d d
        On d.Data_Dt = '$v_month_last_end'
       and d.pty_id = a.pty_id
      left join ms_cust_fc_aset_d e
        On e.Data_Dt = '$ETL_DATE'
       and e.pty_id = a.pty_id
      Left Join ms_cust_fc_aset_d g
        on g.Data_Dt = '$v_month_last_end'
       And g.pty_id = a.Pty_Id
     where a.bgn_dt <= '$ETL_DATE'
       and a.end_dt > '$ETL_DATE'
      )a
     left join MOD_MS_CUST_CHAR_IO_ASET_D b
       on b.Data_Dt = '$ETL_DATE'
      and b.pty_id = a.pty_id

     ;
   Commit;

----进行表分析
EXECEPTION_RAISE_OFF;
Vacuum analyze easyetl.ms_cust_zl_cost_d;
EXECEPTION_RAISE_ON;
