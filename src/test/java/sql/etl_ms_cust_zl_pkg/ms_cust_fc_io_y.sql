/*
 * 目 标 表: EASYETL.MS_CUST_FC_IO_Y
 * 源    表:
 * 脚本生成 : 陈鹏
 * 生成日期 : 2016-12-01
 */

--创建月分区，如分区已存在则什么都不做
--select etl_partition('EASYETL.MS_CUST_FC_IO_Y','Y','$ETL_DATE','CREATE');

 --设置参数值
set v_month_bgn = select substr('$ETL_DATE', 1, 6)||'01' ;
set v_year_bgn_dt= select substr('$ETL_DATE', 1, 4)||'0101' ;

--清理本期数据
DELETE FROM MS_CUST_FC_IO_Y WHERE DATA_DT between '$v_month_bgn' and  '$ETL_DATE';
COMMIT;

insert into MS_CUST_FC_IO_Y
SELECT '$ETL_DATE',
       pty_id,
       sum(fin_in_amt) as fin_in_amt,
       sum(fin_out_amt) as fin_out_amt,
       greatest(max(Fin_In), 0) as max_cost_fc
  From (Select Pty_Id,
               Data_Dt,
               fin_in_amt,
               fin_out_amt,
               Sum(Fin_In) Over(Partition By Pty_Id Order By Data_Dt) As Fin_In
          From (Select Pty_Id,
                       Data_Dt,
                       SUM(fin_in_amt) AS fin_in_amt,
                       SUM(fin_out_amt) AS fin_out_amt,
                       Sum(Fin_In) As Fin_In
                  From (Select a.Pty_Id,
                               a.Data_Dt,
                               a.Assure_In_Amt as fin_in_amt,
                               a.Assure_Out_Amt as fin_out_amt,
                               a.Assure_In_Amt - a.Assure_Out_Amt As Fin_In
                          From Vw_Ms_Cust_Fc_Tran_Idx_d a
                         Where a.Data_Dt Between '$v_year_bgn_dt' And '$ETL_DATE'
                        Union All
                        Select a.Pty_Id,
                               a.Data_Dt,
                               a.Cash_In as fin_in_amt,
                               a.Cash_Out as fin_out_amt,
                               a.Cash_In - a.Cash_Out As Fin_In
                          From Vw_Ms_Cust_Fc_Fin_Idx_d a
                         Where a.Data_Dt Between '$v_year_bgn_dt' And
                               '$ETL_DATE') tt
                 Group By Pty_Id, Data_Dt) t
         order by Pty_Id, Data_Dt) t1
 group by pty_id;
 COMMIT;