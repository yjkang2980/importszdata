/*
 * 目 标 表: EASYETL.MS_CUST_ASET_M
 * 源    表:
 * 脚本生成 : 曹炀
 * 生成日期 : 2015-04-22
 */
---全客户月末资产情况表
    --变量定义
      SET v_month_last_end = SELECT to_char(trunc(to_date('$ETL_DATE', 'yyyymmdd'), 'MM') - 1,'yyyymmdd');
			COMMIT;

   --清理本期数据
      DELETE FROM MS_CUST_ASET_M WHERE DATA_DT = '$ETL_DATE';
			COMMIT;

     Insert Into MS_CUST_ASET_M
        (
            DATA_DT      ,
            PTY_ID       ,
            TOT_ASET_END ,--本月末普通账户资产
            TOT_ASET_BGN ,--上月末普通账户资产
            PUR_ASET_END ,
            PUR_ASET_BGN
        )
    SELECT '$ETL_DATE',
           a.pty_id,
           COALESCE(b.tot_aset, 0) as tot_aset_end,
           COALESCE(d.tot_aset, 0) as tot_aset_bgn,
           COALESCE(e.pur_aset, 0) as pur_aset_end,
           COALESCE(g.pur_aset, 0) as pur_aset_bgn
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
       and a.end_dt > '$ETL_DATE';
    Commit;

--表分析
EXECEPTION_RAISE_OFF;
vacuum analyze easyetl.MS_CUST_ASET_M;
EXECEPTION_RAISE_ON;