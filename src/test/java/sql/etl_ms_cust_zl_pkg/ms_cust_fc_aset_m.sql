/*
 * 目 标 表: EASYETL.MS_CUST_FC_ASET_M
 * 源    表:
 * 脚本生成 : 高勤
 * 生成日期 : 2015-05-04
 */

--创建月分区，如分区已存在则什么都不做
--select etl_partition('EASYETL.MS_CUST_FC_ASET_M','M','$ETL_DATE','CREATE');

--清理本期数据
DELETE FROM MS_CUST_FC_ASET_M WHERE DATA_DT = '$ETL_DATE';
COMMIT;

--设置参数值
set v_month_bgn = select substr('$ETL_DATE', 1, 6)||'01' ;
commit;

--插入数据
Insert Into MS_CUST_FC_ASET_M
  (
         DATA_DT                         --数据日期
        ,PTY_ID                          --客户号
        ,aset_pft                        --月资产收益
        ,Max_Cost_Rate                   --月最大成本收益率
  )
select '$ETL_DATE',
      a.pty_id,
      coalesce(b.pur_aset, 0) - coalesce(c.pur_aset, 0) -coalesce(d.fin_in_amt, 0) + coalesce(d.fin_out_amt, 0) as aset_pft,
      decode(coalesce(c.pur_aset, 0) + coalesce(d.max_cost_fc, 0),
             0,
             0,
             Round((coalesce(b.pur_aset, 0) - coalesce(c.pur_aset, 0) -
                   coalesce(d.fin_in_amt, 0) + coalesce(d.fin_out_amt, 0)) /
                   (coalesce(c.pur_aset, 0) + coalesce(d.max_cost_fc, 0)),
                   4)) As Max_Cost_Rate
 from mf_cust_attr_h a
 left join ms_cust_info_ext_c aa on a.pty_id = aa.pty_id
 left outer join ms_cust_fc_aset_d b
   On b.Data_Dt = '$ETL_DATE'
  and a.pty_id = b.pty_id
 Left outer Join ms_cust_fc_aset_d c
   on c.Data_Dt =
      To_Char(To_Date('$v_month_bgn', 'yyyymmdd') - 1, 'yyyymmdd')
  And a.Pty_Id = c.pty_id
 left outer join MS_CUST_FC_IO_M d
   ON D.DATA_DT = '$ETL_DATE'
  AND A.PTY_ID = D.PTY_ID
where  a.bgn_dt<='$ETL_DATE'
  and  a.end_dt >'$ETL_DATE'
  and aa.fc_flag = 'Y'
  and abs(decode(coalesce(c.pur_aset, 0) + coalesce(d.max_cost_fc, 0),
                 0,
                 0,
                 Round((coalesce(b.pur_aset, 0) - coalesce(c.pur_aset, 0) -
                       coalesce(d.fin_in_amt, 0) + coalesce(d.fin_out_amt, 0)) /
                       (coalesce(c.pur_aset, 0) + coalesce(d.max_cost_fc, 0)),
                       4))) <= 10;

Commit;

--表分析
--SELECT ETL_PARTITION('EASYETL.MS_CUST_FC_ASET_M','M','$ETL_DATE','ANALYZE');
