/*
 * 目 标 表: EASYETL.MS_CUST_ZL_PRDT_fina_M
 * 源    表:
 * 脚本生成 : 徐猛
 * 生成日期 : 2016-10-12
 */

     SET v_month_bgn = select (substr('$ETL_DATE', 1, 6) || '01');
     COMMIT;

     DELETE FROM ms_cust_zl_prdt_fina_m WHERE DATA_DT = '$ETL_DATE';
     COMMIT;



	--创建临时表
	--客户持有理财产品数量
	create temp table mod_MS_CUST_ZL_PRDT_FINA_M as select * from MS_CUST_ZL_PRDT_FINA_M where 1<>1;
	insert into mod_MS_CUST_ZL_PRDT_FINA_M (
		pty_id
		,prdt_cnt
	)
	SELECT	pty_id
			,count(distinct(a.prdt_id)) as prdt_cnt
	FROM	(SELECT	a.pty_id
					,a.prdt_id
			FROM	ms_cust_prdt_hold_d a
			where	data_dt = '$ETL_DATE'
			union all
			SELECT	a.pty_id
					,a.prdt_id
			FROM	ms_cust_prdt_unhold_d a
			where	a.data_dt in (select d_date
								from ana_dim_date
								where d_date between '$v_month_bgn' and '$ETL_DATE')
			union all
			SELECT	a.pty_id
					,a.prdt_id
			FROM	MS_CUST_PRDT_FC_HOLD_D a
			where	data_dt = '$ETL_DATE'
					And (a.Hold_Cnt <> 0 Or a.Deb_Mnt <> 0)
			union all
			SELECT	a.pty_id
					,a.prdt_id
			FROM	MS_CUST_PRDT_FC_UNHOLD_D a
			where	a.data_dt in (select d_date
								from ana_dim_date
								where d_date between '$v_month_bgn' and '$ETL_DATE')
			) a,
			mf_prdt_attr_h b
	where	a.prdt_id = b.prdt_id
			and b.bgn_dt <= '$ETL_DATE'
			and b.end_dt > '$ETL_DATE'
			and b.prdt_type_id = 'PA070100'
			and a.prdt_id <> 'TA940018'
	group by a.pty_id
	;

	--理财产品买卖次数
	insert into mod_MS_CUST_ZL_PRDT_FINA_M (
		pty_id
		,buy_cnt
		,sell_cnt
	)
	SELECT	a.pty_id
			,sum(a.buy_cnt) as buy_cnt
			,sum(a.sell_cnt) as sell_cnt
	FROM	mf_cust_tran_core_m a
	left join mf_prdt_attr_h b
		on	b.bgn_dt <= '$ETL_DATE'
			and b.end_dt > '$ETL_DATE'
			and a.prdt_id = b.prdt_id
	where	a.data_dt = '$ETL_DATE'
			and a.sys_source_cd in ('02', '03')
			and b.prdt_type_id = 'PA070100'
			and a.prdt_id <> 'TA940018'
	group by a.pty_id
	;

--普通账户下无持仓债券产品的收益(债券质押入库、出库)
     Insert Into MS_CUST_ZL_PRDT_FINA_M
        (
			data_dt
      		,pty_id
      		,prdt_cnt
      		,buy_cnt
      		,sell_cnt
        )
    SELECT	'$ETL_DATE'
			,pty_id
			,sum(prdt_cnt)
			,sum(buy_cnt)
			,sum(sell_cnt)
	from	mod_MS_CUST_ZL_PRDT_FINA_M
	group by pty_id
	;


    commit;

