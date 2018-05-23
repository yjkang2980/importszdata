/*
 * 目 标 表: EASYETL.MS_CUST_FC_IO_M
 * 源    表:
 * 脚本生成 : 郑健
 * 生成日期 : 2015-04-24
 */

--创建月分区，如分区已存在则什么都不做
--select etl_partition('EASYETL.MS_CUST_FC_IO_M','M','$ETL_DATE','CREATE');

 --设置参数值
set v_month_bgn = select substr('$ETL_DATE', 1, 6)||'01' ;
commit;

--清理本期数据
DELETE FROM MS_CUST_FC_IO_M WHERE DATA_DT between '$v_month_bgn' and  '$ETL_DATE'; --modify by chenp 20161201
COMMIT;



--插入数据
Insert Into MS_CUST_FC_IO_M
(
       DATA_DT                        --数据日期
      ,PTY_ID                        --客户号
      ,fin_in_amt                    --月累计转入资产
      ,fin_out_amt                   --月累计转出资产
      ,max_cost_fc                   --月最大调整成本
)
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
                         Where a.Data_Dt Between '$v_month_bgn' And '$ETL_DATE'
                        Union All
                        Select a.Pty_Id,
                               a.Data_Dt,
                               a.Cash_In as fin_in_amt,
                               a.Cash_Out as fin_out_amt,
                               a.Cash_In - a.Cash_Out As Fin_In
                          From Vw_Ms_Cust_Fc_Fin_Idx_d a
                         Where a.Data_Dt Between '$v_month_bgn' And
                               '$ETL_DATE') tt
                 Group By Pty_Id, Data_Dt) t
         order by Pty_Id, Data_Dt) t1
 group by pty_id;
    Commit;

--表分析
--SELECT ETL_PARTITION('EASYETL.MS_CUST_FC_IO_M','M','$ETL_DATE','ANALYZE');
