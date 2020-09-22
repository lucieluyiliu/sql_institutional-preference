
SELECT connectstr AS $$dbname=wrds
                       options=-csearch_path=factset
                       user=yiliulu
                  
                      host=wrds-pgdata.wharton.upenn.edu
                      port=9737$$;

SELECT public.dblink_connect_u ('wrdsu',$$dbname=wrds
                       options=-csearch_path=factset
                       user=yiliulu
                      password=
                      host=wrds-pgdata.wharton.upenn.edu
                      port=9737$$);


SELECT DISTINCT a.iso_country
FROM edm_standard_entity AS a, own_mktcap AS b
WHERE b.own_mv IS NULL
AND a.factset_entity_id=b.factset_entity_id;

CREATE  TABLE factset.h_security_cusip_hist AS
  SELECT * FROM
dblink('wrds',$$
select * FROM factset.h_security_cusip_hist AS source(CUSIP
                                           varchar(9) fs_perm_sec_id varchar(20),
                                           start_date date,
                                           end_Date date,
                                           most_recent varchar(1));
$$);

CREATE TABLE factset.own_unadj_fund_holdings_hist AS
  SELECT *
  FROM public.dblink('dbname=wrds
                       options=-csearch_path=factset
                       user=yiliulu
                      password=Dl4zkadl?
                      host=wrds-pgdata.wharton.upenn.edu
                      port=9737','SELECT * FROM own_unadj_fund_holdings_hist') AS t(factset_fund_id varchar(8),cusip varchar(9), holding double precision, report_date timestamp);


