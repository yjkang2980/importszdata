/*
 * 目 标 表: EASYETL.MS_CUST_CCY_FC_ASET_D
 * 源    表:
 * 脚本生成 : 曹炀
 * 生成日期 : 2015-04-22
 * 修改日期 : 2015-11-17 gaoqin
 */
            --如果是非营业日就取最近一个营业日日期

            SET v_Tran_Dt_Flag =
					Select COALESCE(Tran_Dt_Flag, 'Y')
            From Meta_Sor_Tran_Dt_Def
           Where Cluster_Id = 'GP_ACRM'
             And Sor_Id = 'ODS'
             And Data_Dt = '$ETL_DATE';

           SET v_Last_Date = Select Max(Data_Dt)
              From Meta_Sor_Tran_Dt_Def a
             Where Cluster_Id = 'CLU_ACRM'
               And Sor_Id = 'ODS'
               And Data_Dt < '$ETL_DATE'
               And Tran_Dt_Flag = 'Y';
               COMMIT;


         --清理本期数据
			Delete from MS_CUST_CCY_FC_ASET_D where Data_Dt = '$ETL_DATE';
            COMMIT;

            Insert Into MS_CUST_CCY_FC_ASET_D
              (Data_Dt,
               Pty_Id,
               CCY,
               Ass_Bal,
               Bal,
               Cls_Mkt_Val,
               Cls_Mkt_Val_Asur,
               Fina_Deb,
               Stc_Deb,
               Cls_Fare_Deb,
               Cls_Oth_Deb,
               Cls_Fina_Int,
               Cls_Shtcel_Int,
               Cls_Fare_Int,
               Cls_Oth_Int,
               Aset_Deb_Amt,
               Tot_Deb_Amt,
               Pur_Aset,
               Pur_Aset_Bef,
               Finance_Inte_Bal,
               Shortsell_Inte_Bal,
               Close_Rate,
               Avl_Amt,
               Frz_Amt,
               Draw_Amt)
              Select '$ETL_DATE',
                     Pty_Id,
                     CCY,
                     Ass_Bal,
                     Bal,
                     Cls_Mkt_Val,
                     Cls_Mkt_Val_Asur,
                     Fina_Deb,
                     Stc_Deb,
                     Cls_Fare_Deb,
                     Cls_Oth_Deb,
                     Cls_Fina_Int,
                     Cls_Shtcel_Int,
                     Cls_Fare_Int,
                     Cls_Oth_Int,
                     Aset_Deb_Amt,
                     Tot_Deb_Amt,
                     Pur_Aset,
                     Pur_Aset_Bef,
                     Finance_Inte_Bal,
                     Shortsell_Inte_Bal,
                     Close_Rate,
                     Avl_Amt,
                     Frz_Amt,
                     Draw_Amt
                From MS_CUST_CCY_FC_ASET_D
               Where Data_Dt = '$v_Last_Date'
               AND '$v_Tran_Dt_Flag' <> 'Y';

               Commit;

        --非营业日则退出,无需再执行后面的过程
        EXITIF( '$v_Tran_Dt_Flag' <> 'Y' );

        --营业日执行过程
        CREATE GLOBAL TEMPORARY TABLE MS_CUST_CCY_FC_ASET_D_TMP AS
        SELECT DATA_DT AS v_etl_date,
               (SELECT MAX(DATA_DT)
                  FROM META_SOR_TRAN_DT_DEF
                 WHERE CLUSTER_ID = 'CLU_ACRM'
                   AND SOR_ID = 'ODS'
                   AND DATA_DT <= A.DATA_DT
                   AND TRAN_DT_FLAG = 'Y') AS v_recall_date
          FROM META_SOR_TRAN_DT_DEF A
         WHERE CLUSTER_ID = 'CLU_ACRM'
           AND SOR_ID = 'ODS'
           AND DATA_DT > (SELECT MAX(DATA_DT) AS DATA_DT
                            FROM META_SOR_TRAN_DT_DEF A
                           WHERE CLUSTER_ID = 'CLU_ACRM'
                             AND SOR_ID = 'ODS'
                             AND DATA_DT < '$ETL_DATE'
                             AND NVL(TRAN_DT_FLAG, 'Y') = 'Y')
           AND DATA_DT <= '$ETL_DATE'
         ORDER BY DATA_DT;

        --清除当期数据
        delete from ms_cust_ccy_fc_aset_d where data_dt in (select v_etl_date from MS_CUST_CCY_FC_ASET_D_TMP);


        --客户的可用资金、可取资金、冻结资金非营业日时取上一个最近的营业日数据。
        --营业日时取当天数据。
        --重跑非营业日数据时，不可以取非营业日当天的数据。

          Insert Into MS_CUST_CCY_FC_ASET_D
            (Data_Dt,
             Pty_Id,
             CCY,
             Ass_Bal,
             Bal,
             Cls_Mkt_Val,
             Cls_Mkt_Val_Asur,
             Fina_Deb,
             Stc_Deb,
             Cls_Fare_Deb,
             Cls_Oth_Deb,
             Cls_Fina_Int,
             Cls_Shtcel_Int,
             Cls_Fare_Int,
             Cls_Oth_Int,
             Aset_Deb_Amt,
             Tot_Deb_Amt,
             Pur_Aset,
             Pur_Aset_Bef,
             Finance_Inte_Bal,
             Shortsell_Inte_Bal,
             Close_Rate,
             Avl_Amt,
             Frz_Amt,
             Draw_Amt)
            Select Data_Dt,
                   Pty_Id,
                   CCY,
                   Sum(Ass_Bal)             As Ass_Bal,
                   Sum(Bal)                 As Bal,
                   Sum(Cls_Mkt_Val)         As Cls_Mkt_Val,
                   Sum(Cls_Mkt_Val_Asur)    As Cls_Mkt_Val_Asur,
                   Sum(Fina_Deb)            As Fina_Deb,
                   Sum(Stc_Deb)             As Stc_Deb,
                   Sum(Cls_Fare_Deb)        As Cls_Fare_Deb,
                   Sum(Cls_Oth_Deb)         As Cls_Oth_Deb,
                   Sum(Cls_Fina_Int)        As Cls_Fina_Int,
                   Sum(Cls_Shtcel_Int)      As Cls_Shtcel_Int,
                   Sum(Cls_Fare_Int)        As Cls_Fare_Int,
                   Sum(Cls_Oth_Int)         As Cls_Oth_Int,
                   Sum(Aset_Deb_Amt)        As Aset_Deb_Amt,
                   Sum(Tot_Deb_Amt)         As Tot_Deb_Amt,
                   Sum(Pur_Aset)            As Pur_Aset,
                   Sum(Pur_Aset_Bef)        As Pur_Aset_Bef,
                   Sum(Finance_Inte_Bal)    As Finance_Inte_Bal,
                   Sum(Shortsell_Inte_Bal)  As Shortsell_Inte_Bal,
                   Sum(Close_Rate)          As Close_Rate,
                   Sum(Avl_Amt)             As Avl_Amt,
                   Sum(Frz_Amt)             As Frz_Amt,
                   Sum(Draw_Amt)            As Draw_Amt
              From (Select b.v_etl_date As data_dt,
                           a.Pty_Id,
                           a.ccy,
                           Sum(a.Ass_Bal ) As Ass_Bal, --当前证券担保市值
                           Sum(a.Bal) As Bal, --期终资金余额
                           Sum(a.Cls_Mkt_Val) As Cls_Mkt_Val, --收市证券市值
                           Sum(a.Cls_Mkt_Val_Asur) As Cls_Mkt_Val_Asur, --收市证券市值(折)
                           Sum(a.Fina_Deb ) As Fina_Deb, --当前融资负债
                           Sum(a.Stc_Deb) As Stc_Deb, --当前融券负债余额
                           Sum(a.Cls_Fare_Deb) As Cls_Fare_Deb, --收市费用负债
                           Sum(a.Cls_Oth_Deb) As Cls_Oth_Deb, --收市其他负债
                           Sum(a.Cls_Fina_Int ) As Cls_Fina_Int, --收市融资负债利息
                           Sum(a.Cls_Shtcel_Int) As Cls_Shtcel_Int, --收市融券负债利息
                           Sum(a.Cls_Fare_Int) As Cls_Fare_Int, --收市费用负债利息
                           Sum(a.Cls_Oth_Int) As Cls_Oth_Int, --收市其他负债利息
                           Sum(a.Deb_Amt) Aset_Deb_Amt, --资产负债
                           Sum((a.Ass_Bal - a.Pur_Aset)) As Tot_Deb_Amt, --总负债
                           Sum(a.Pur_Aset) As Pur_Aset, --当日净资产
                           Sum(a.Pur_Aset_Bef) As Pur_Aset_Bef, --前日净资产
                           0 As Finance_Inte_Bal, --融资利息
                           0 As Shortsell_Inte_Bal, --融券费用
                           Round(Decode(Sum(a.Ass_Bal - a.Pur_Aset),
                                        0,
                                        -1,
                                        Sum(a.Ass_Bal) / Sum(a.Ass_Bal - a.Pur_Aset)),
                                 4) As Close_Rate,
                           0 As Avl_Amt,
                           0 As Frz_Amt,
                           0 As Draw_Amt
                      From His_Fc_Aset_Deb a,MS_CUST_CCY_FC_ASET_D_TMP b
                     Where a.Data_Dt = b.v_recall_date
                       And (a.Ass_Bal <> 0 Or a.Ass_Bal - a.Pur_Aset <> 0)
                     Group By  b.v_etl_date,a.Pty_Id,a.ccy
                     Union All
                    Select b.v_etl_date  As data_dt,
                           a.Pty_Id,
                           a.ccy,
                           0          As Ass_Bal,
                           0          As Bal,
                           0          As Cls_Mkt_Val,
                           0          As Cls_Mkt_Val_Asur,
                           0          As Fina_Deb,
                           0          As Stc_Deb,
                           0          As Cls_Fare_Deb,
                           0          As Cls_Oth_Deb,
                           0          As Cls_Fina_Int,
                           0          As Cls_Shtcel_Int,
                           0          As Cls_Fare_Int,
                           0          As Cls_Oth_Int,
                           0          As Aset_Deb_Amt,
                           0          As Tot_Deb_Amt,
                           0          As Pur_Aset,
                           0          As Pur_Aset_Bef,
                           Sum(a.Fina_Deb_Int_d) As Finance_Inte_Bal,
                           Sum(a.Stc_Deb_Int_d) As Shortsell_Inte_Bal,
                           0          As Close_Rate,
                           0          As Avl_Amt,
                           0          As Frz_Amt,
                           0          As Draw_Amt
                      From Mf_Cust_Fc_Hold_d a,MS_CUST_CCY_FC_ASET_D_TMP b
                     Where a.Data_Dt = b.v_etl_date
                      And Sys_Source_Cd = '05'
                     Group By b.v_etl_date,a.Pty_Id,a.ccy
                     Union All
                    Select b.v_etl_date  As data_dt,
                           a.Pty_Id,
                           a.ccy,
                           0          As Ass_Bal,
                           0          As Bal,
                           0          As Cls_Mkt_Val,
                           0          As Cls_Mkt_Val_Asur,
                           0          As Fina_Deb,
                           0          As Stc_Deb,
                           0          As Cls_Fare_Deb,
                           0          As Cls_Oth_Deb,
                           0          As Cls_Fina_Int,
                           0          As Cls_Shtcel_Int,
                           0          As Cls_Fare_Int,
                           0          As Cls_Oth_Int,
                           0          As Aset_Deb_Amt,
                           0          As Tot_Deb_Amt,
                           0          As Pur_Aset,
                           0          As Pur_Aset_Bef,
                           0          As Finance_Inte_Bal,
                           0          As Shortsell_Inte_Bal,
                           0          As Close_Rate,
                           a.Avl_Amt  As Avl_Amt,
                           a.Frz_Amt  As Frz_Amt,
                           a.Draw_Amt As Draw_Amt
                      From His_Aset_Acct_Fc a,MS_CUST_CCY_FC_ASET_D_TMP b
                     Where a.Data_Dt = b.v_recall_date
                       And (a.Avl_Amt <> 0 Or a.Frz_Amt <> 0 Or a.Draw_Amt <> 0)
                   )fo
             where '$v_Tran_Dt_Flag' = 'Y'
             Group By data_dt, Pty_Id,ccy ;

        Commit;




--表分析
EXECEPTION_RAISE_OFF;
vacuum analyze easyetl.MS_CUST_CCY_FC_ASET_D;
EXECEPTION_RAISE_ON;
