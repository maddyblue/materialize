
sqlcmd query --database mz "select sys.fn_cdc_get_min_lsn ('a')"
sqlcmd query --database mz "select sys.fn_cdc_get_max_lsn()"
sqlcmd query --database mz "select * from cdc.fn_cdc_get_all_changes_dbo_a"

sqlcmd query --database mz "SELECT * FROM cdc.fn_cdc_get_all_changes_dbo_a(0, NULL, 'all')"
