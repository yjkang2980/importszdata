/*
 * 目 标 表: EASYETL.MS_CUST_ZL_FC_COST_M
 * 源    表:
 * 脚本生成 : 曹炀
 * 生成日期 : 2015-05-04
 */

 		 SET v_month_bgn = SELECT (substr('$ETL_DATE', 1, 6) || '01');

     DELETE FROM MS_CUST_ZL_FC_COST_M WHERE DATA_DT = '$ETL_DATE';
     COMMIT;

     Insert Into MS_CUST_ZL_FC_COST_M
        (
               DATA_DT                         --数据日期
              ,PTY_ID                          --客户号
              ,pur_aset_bgn                    --月初净资产
              ,pur_aset_end                    --月末净资产
              ,fin_in_amt                      --月累计转入资产
              ,fin_out_amt                     --月累计转出资产
              ,aset_pft                        --月资产收益
              ,Max_Cost_Rate                   --月最大成本收益率
        )
     SELECT '$ETL_DATE',
            a.pty_id,
            COALESCE(c.pur_aset, 0) as pur_aset_bgn ,
            COALESCE(b.pur_aset, 0) as pur_aset_end ,
            COALESCE(d.fin_in_amt,0) as fin_in_amt,
            COALESCE(d.fin_out_amt,0) as fin_out_amt,
            COALESCE(e.aset_pft,0) as aset_pft,
            COALESCE(e.max_cost_rate,0) as max_cost_rate
    FROM mf_cust_attr_h a
    left join ms_cust_info_ext_c aa on a.pty_id=aa.pty_id
    left join  ms_cust_fc_aset_d b On  b.Data_Dt = '$ETL_DATE'
                                         and a.pty_id=b.pty_id
    Left Join  ms_cust_fc_aset_d c on  c.Data_Dt = '$v_month_bgn'
                                       And a.Pty_Id = c.pty_id
    left join  MS_CUST_FC_IO_M d on d.Data_Dt = '$ETL_DATE' and  a.pty_id = d.pty_id
    left join  MS_CUST_FC_ASET_M  e on e.data_dt = '$ETL_DATE' and  a.pty_id = e.pty_id
    where   a.bgn_dt<='$ETL_DATE'
    and     a.end_dt >'$ETL_DATE'
    and     aa.fc_flag='Y';
    Commit;


--表分析
EXECEPTION_RAISE_OFF;
vacuum analyze easyetl.MS_CUST_ZL_FC_COST_M;
EXECEPTION_RAISE_ON;