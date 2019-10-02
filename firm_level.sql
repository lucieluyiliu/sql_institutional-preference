/*create table fref_security_type that contains cusip,issue_type and fref_sec_type*/
SET search_path TO factset, public;
CREATE VIEW fref_security_type AS
   SELECT a.cusip, b.fref_security_type, c.issue_type
   FROM h_security_cusip_hist AS a, wrds_securities AS b, own_unadj_basic AS c
WHERE a.fs_perm_sec_id=b.fs_perm_sec_id
     AND b.cusip=c.cusip;

/*subsets of securities to consider*/
/*only include equity, preferred equity and deposit receipts*/
CREATE TABLE factset.own_basic_selected AS
  SELECT *
  FROM own_unadj_basic
  /*only relevant issue types are:AD, EQ,PF*/
  WHERE issue_type IN ('EQ','AD')
OR issue_type ='PF' AND cusip IN (
     SELECT DISTINCT cusip
     FROM factset.fref_security_type
     WHERE fref_security_type IN ('SHARE','PREFEQ')
                                  );

/*combine main dual-listing companies REPLACE DLC ID WITH ENTITY ID*/
UPDATE own_basic_selected AS a
    SET factset_entity_id=(
       SELECT b.factset_entity_id
          FROM dlc AS b
          WHERE a.factset_entity_id=b.dlc_entity_id
       )
    WHERE EXISTS (
         SELECT 1 FROM dlc AS b
         WHERE a.factset_entity_id=b.dlc_entity_id
             )
;

/*BEGIN DATE as 200001 fixed*/
CREATE FUNCTION sqtr() RETURNS integer AS
  $$
  DECLARE sqtr integer:=200001;
  BEGIN
  RETURN sqtr;
  END;
  $$
  LANGUAGE plpgsql;

/*end date be the latest available 13f month*/

CREATE FUNCTION eqtr() RETURNS integer AS
  $$DECLARE maxdate date ;
            eqtr integer;
    BEGIN
    SELECT  max(report_date) FROM own_unadj_13f_holdings_hist into maxdate;
    eqtr=extract(year FROM maxdate)*100+extract(quarter FROM maxdate)::int;
    RETURN eqtr;
    END;
  $$
  LANGUAGE plpgsql;
SELECT eqtr();
$$WITH maxdate AS (
   SELECT MAX(report_date) AS raw
   FROM factset.own_unadj_13f_holdings_hist
)
SELECT EXTRACT(year FROM raw)*100+extract(quarter FROM raw) AS INTEGER::int  AS yearmon FROM maxdate;$$;


/*creates 13f and fund */
CREATE TABLE unadj_13f_holdings_hist AS
    WITH withquarter AS(
      SELECT *, CAST(extract(year FROM report_date)*100+extract(quarter FROM report_date) AS int)  AS quarter
      FROM own_unadj_13f_holdings_hist
      WHERE cusip IN(
  SELECT cusip FROM own_basic_selected
    )
    ),
      sqtr AS (SELECT sqtr()),
      eqtr AS (SELECT eqtr())
SELECT *
FROM withquarter
WHERE quarter BETWEEN (SELECT * FROM sqtr) AND (SELECT * FROM eqtr);

/*Creates international fund table*/
CREATE TABLE unadj_fund_holdings_hist AS
      WITH withquarter AS(
      SELECT *, cast(extract(year FROM report_date)*100+extract(quarter FROM report_date) AS int) AS quarter
      FROM own_unadj_fund_holdings_hist
      WHERE cusip IN(
  SELECT cusip FROM factset.own_basic_selected
    )
    )
SELECT *
FROM withquarter
WHERE quarter BETWEEN (SELECT sqtr()) AND (SELECT eqtr());

/*market capitalization procedures*/
/*historical price table*/
CREATE TABLE prices_historical AS
/*last aailable price each month*/
  WITH withmonth AS(
    SELECT cusip, cast(extract(year FROM price_date)*100+extract(month FROM price_date) AS int) AS month, max(price_date) AS maxdate
      FROM own_unadj_prices
          GROUP BY cusip, month
    )
  SELECT a.cusip, b.month,a.price,a.price_date
  FROM own_unadj_prices AS a,withmonth AS b
  WHERE a.cusip=b.cusip AND a.price_date=b.maxdate
  ORDER BY cusip,month;

/*ownership market cap*/
CREATE TABLE own_mktcap_raw AS
  WITH own_mktcap AS
    (
     SELECT a.*, b.factset_entity_id,b.issue_type,a.shares_outstanding*a.price AS own_mv
     FROM own_unadj_prices AS a, factset.own_basic_selected AS b
     WHERE a.cusip=b.cusip
     AND b.issue_type !='AD'
    )
    /*sum mv at entity level*/
    /*excluding the the AD issue is fine*/
    /*because if IPO  is ADR, factset has two records, one is EQ, the other is DR*/
  SELECT factset_entity_id, sum(own_mv) AS own_mv,
         price_date,
         cast(extract(year FROM price_date)*100+extract(month FROM price_date)AS int) AS month,
         cast(extract(year FROM price_date)*100+extract(quarter FROM price_date)AS int) AS quarter,
         CASE WHEN extract(month FROM price_date) in (3,6,9,12) THEN 1
         ELSE 0 END AS eoq
  FROM own_mktcap
  WHERE factset_entity_id IS NOT NULL
  GROUP BY factset_entity_id,price_date
  ORDER BY factset_entity_id,price_date;

UPDATE own_mktcap_raw
  SET own_mv=NULL
  WHERE own_mv <=0;

SELECT count(*)
 FROM own_mktcap_raw
WHERE own_mv IS NULL;  /*167845 missing entries*/

/*IDENTIFIERS*/
CREATE TABLE wrds_1 AS
  SELECT *
  FROM wrds_securities
  WHERE factset_entity_id IS NOT NULL
  and fref_security_type in ('SHARE','ADR','DR','GDR','NVDR','PREFEQ');

/*entities with only one security*/
  CREATE TABLE wrds_2 AS
  SELECT factset_entity_id, count(*)
  FROM wrds_1
  GROUP BY factset_entity_id
  HAVING count(*)=1;

/*join of primary and unique securities*/
/*have to do it separately because of the sec_id=primary_id restriction*/
CREATE TABLE wrds_3 AS
  SELECT factset_entity_id, fs_perm_sec_id, fref_security_type, inactive_flag, isin, sedol, cusip
  FROM wrds_1
  WHERE fs_primary_equity_id=fs_perm_sec_id AND factset_entity_id NOT IN (
      SELECT factset_entity_id
      FROM wrds_2
  )
  UNION ALL (
   SELECT factset_entity_id,fs_perm_sec_id, fref_security_type, inactive_flag, isin,sedol, cusip
  FROM wrds_1
   WHERE factset_entity_id IN (SELECT factset_entity_id FROM wrds_2)
  );
/*union unique security and primary security with the remaining records*/
CREATE TABLE wrds_4 AS
  SELECT factset_entity_id, fs_perm_sec_id, fref_security_type, inactive_flag, isin, sedol, cusip

  FROM wrds_3
   UNION ALL
  SELECT factset_entity_id, fs_perm_sec_id, fref_security_type, inactive_flag, isin, sedol, cusip
  FROM wrds_1 WHERE factset_entity_id NOT IN (SELECT factset_entity_id FROM wrds_3);

/*order and create rank*/
CREATE TABLE wrds_final AS
  SELECT *,rank() OVER(PARTITION BY factset_entity_id ORDER BY factset_entity_id, fref_security_type DESC, inactive_flag DESC, isin DESC, sedol DESC, CUSIP DESC)AS rank
  FROM wrds_4
  ORDER BY factset_entity_id, rank;

/*creates header table with entity_id and principal security identifiers*/
  CREATE TABLE  entity_identifiers AS
  SELECT a.*, b.isin, b.sedol,b.cusip
  FROM edm_standard_entity  AS a
  LEFT JOIN wrds_final AS b ON (b.rank=1 and a.factset_entity_id=b.factset_entity_id);


/*--------------------------------------------------------------------*/
/*who has zero matketcap? either price or sshs_out zero*/
--turns out that MISSING MKT CAP are everywhere


/*filling missing market cap two methods: firm level and security level*/
/*security level: need to find cusip sedol isin for all cusips in the ownership database*/
/*a table with entity_id,cusip, date of missing market cap entries used for later merge with datastream*/
  CREATE TABLE misscap AS  /*this table is at SECURITY level so missing value more than missing entity mv*/
    SELECT a.factset_entity_id,b.cusip, b.price_date
    FROM own_mktcap AS a, own_unadj_prices AS b, own_basic_selected AS c
    WHERE a.own_mv IS NULL
       AND a.factset_entity_id=c.factset_entity_id
       AND b.cusip=c.cusip
       AND a.price_date=b.price_date
       AND c.issue_type!='AD';
SELECT COUNT (*) FROM misscap; /*170659 missing*/

ALTER TABLE misscap
ADD COLUMN fs_perm_sec_id varchar(20),
ADD COLUMN sedol varchar(7),
ADD COLUMN isin varchar (12);

UPDATE misscap AS a
SET fs_perm_sec_id=b.fs_perm_sec_id
FROM h_security_cusip_hist AS b
WHERE a.cusip=b.cusip;/*still 5038 cusip without fs_perm_Sec_id*/
UPDATE misscap AS a
SET sedol=b.sedol, isin=b.isin
FROM wrds_securities AS b
WHERE a.fs_perm_sec_id=b.fs_perm_sec_id;

SELECT count(*) FROM misscap WHERE cusip IS NOT NULL AND isin IS NOT NULL AND SEDOL IS NOT NULL LIMIT 10; /*160375 security level miss*/


/*create table of identifiers of missing market cap entries*/
/*the thing is that for some non-US stocks factset uses FD*** to replace cusip*/
/*unfortunately, this cannot be recognized by datastream*/
--CREATE TABLE dsidentifiers AS  /*use price table cusip together with primary listing isin sedol*/
-- SELECT a.cusip, a.isin, a.sedol, b.factset_entity_id
--  FROM misscap AS a, own_basic_selected AS b
 -- WHERE a.cusip=b.cusip;
  --ORDER BY factset_entity_id, price_date, isin,sedol;
/*basically, selecting all cusips and the isin,sedol of missing value at security level*/
/*currently factset-datastream map is done on primary listing isin sedols*/
  --CREATE TABLE dsid_unique AS
--SELECT DISTINCT CUSIP,ISIN,SEDOL
--FROM dsidentifiers;

/*export dsid_unique to symbol_unique.csv to feed into eikon api*/
/*creates table that contains the cusip, ric code and date of missing market cap*/
CREATE TABLE dslookup AS
  SELECT a.factset_entity_id, a.cusip,a.price_date, b.ric
  FROM misscap AS a, ric_map AS b
  WHERE a.cusip=b.cusip
  AND b.ric !='none';
SELECT COUNT(*) FROM dslookup;/*153811 missing entries that can be mapped to DS ric */

/*entity level match*/
/*alternatively, find missing values using entity_id*/
CREATE TABLE misscap_entity AS
  SELECT a.factset_entity_id, a.price_date,b.cusip, b.isin, b.sedol
  FROM own_mktcap AS a, entity_identifiers AS b
  WHERE a.own_mv IS NULL
  AND a.factset_entity_id=b.factset_entity_id;
SELECT COUNT(*) FROM misscap_entity;/*167845*/
SELECT count(*) FROM
CREATE TABLE dslookup_entity AS
  SELECT a.factset_entity_id, a.cusip,a.price_date, b.ric
  FROM misscap_entity AS a, ric_map AS b
  WHERE a.cusip=b.cusip
  AND b.ric !='none';
SELECT COUNT(*) FROM dslookup_entity; /*119742 entity level match*/

/*confirm that one entity one missing each date*/

/*export dslookup_entity to feed into ds excel addin as dslookup_entity.csv*/
/*turns out that we do not subscribe to datastream api */
/*spent two days querying missing data from datastream excel addin. 119742 records*/
/*import missing data dsfill_entity.csv into cedar as dsfill_entity*/

/*local host*/
CREATE TABLE dsfill_entity
  (factset_entity_id varchar(8),cusip varchar(9),date date, ric varchar(24),mv double precision );
COPY dsfill_entity FROM 'C:\Users\ylu115\Documents\Institutional preference\factset_io\dsfill_entity.csv' WITH (FORMAT csv,HEADER TRUE,NULL ' ');

UPDATE  dsfill_entity
  SET mv=mv*1000000; /*transform datastream million usd market value to factset units of USD*/

ALTER TABLE dsfill_entity
  ADD COLUMN month int,
  ADD COLUMN quarter int,
  ADD COLUMN eoq int;

UPDATE dsfill_entity
  SET month = CAST(extract(year FROM date)*100+extract(month FROM date)AS int),
      quarter= (CAST(SELECT extract(year FROM date )*100+extract(quarter FROM date) AS int)),
      eoq =(CASE WHEN extract(month FROM date) in (3,6,9,12) THEN 1
         ELSE 0 END);


/*even in factset there are many very tiny stocks with missing mkt cap*/
SELECT * FROM own_mktcap
WHERE own_mv=(SELECT min(own_mv) FROM own_mktcap);
--example of tiny mkt cap
-- SELECT * FROM own_basic_selected
-- WHERE factset_entity_id='06KLCT-E';
--
-- SELECT * FROM own_unadj_prices WHERE cusip='FDS0HV042';

--own_mktcap /*in total 167927 missing value in own_mktcap*/
SELECT count(*)
FROM own_mktcap
WHERE own_mv IS NULL;

SELECT count(*) FROM own_mktcap
  FROM misscap; /*187631 missing this is because misscap includes all cusips of entities with missing cap*/


/*fill in missing market cap with DS data*/
 UPDATE own_mktcap AS b
SET  own_mv = a.mv
        FROM dsfill_entity as a
        WHERE a.factset_entity_id=b.factset_entity_id
          AND a.date=b.price_date
          AND a.mv IS NOT NULL
          AND own_mv IS NULL;


SELECT count(*) FROM own_mktcap WHERE own_mv IS NULL; /*still 92393 NULL mkt cap values attrition due to missing datastream value or failure to match*/


/*tested that returning mv should be unique, indeed unique*/

VACUUM ANALYZE dsfill_entity;

/*-----market cap finished-----*/
/*last 13f and mf reports only contains date*/
  CREATE VIEW max13f AS
  SELECT factset_entity_id, quarter,max(report_date) AS maxofdlr
         FROM factset.unadj_13f_holdings_hist
  GROUP BY factset_entity_id,quarter;
/*this again i'm not sure whether it's necessary given that reports are quarterly*/
/*makes sense if you wanna match market cap, which is monthly.*/

/*quarterly frequency contains everything*/
CREATE VIEW aux13f AS /*find the month of last report each quarter*/
   SELECT b.*, cast(extract(year FROM b.report_date)*100+extract(month FROM b.report_date)AS int) AS month
   FROM max13f AS a, own_unadj_13f_holdings_hist AS b
   WHERE a.factset_entity_id=b.factset_entity_id
AND a.maxofdlr=b.report_date;

CREATE VIEW maxmf AS
  SELECT factset_fund_id,quarter,max(report_date)AS maxofdlr
  FROM unadj_fund_holdings_hist
  GROUP BY factset_fund_id,quarter;

CREATE TABLE auxmf AS/*month of last report*/
  SELECT b.*, cast(extract(year FROM b.report_date)*100+extract(month FROM b.report_date) AS int) AS month
  FROM maxmf AS a, factset.unadj_fund_holdings_hist AS b
  WHERE a.factset_fund_id=b.factset_fund_id
       AND a.maxofdlr=b.report_date;
/*finding the month of last quarterly report only to match price and market cap.*/

/*compute 13f holdings ratio*/
CREATE TABLE v1_holdings13f AS
SELECT t1.factset_entity_id,
       t1.cusip,
       t1.holding*t3.price/t4.own_mv AS io,
       t1.quarter,
       t1.holding
       --t4.own_mv AS mv
FROM aux13f AS t1 ,own_basic_selected AS t2, prices_historical AS t3, own_mktcap AS t4
WHERE t1.cusip=t3.cusip  /*match price*/
   AND t1.cusip=t2.cusip /*match mktcap via basic table*/
  AND t2.factset_entity_id=t4.factset_entity_id
AND t1.month=t3.month
AND t1.month=t4.month;


CREATE TABLE v1_holdingsmf AS
  SELECT t1.factset_fund_id,
         t1.cusip,
         t1.quarter,
         t1.holding,
         t1.holding*t3.price/t4.own_mv AS io

FROM auxmf AS t1, own_basic_selected AS t2, prices_historical AS t3, own_mktcap AS t4
WHERE t1.cusip=t3.cusip
   AND t1.month=t3.month
  AND t1.cusip=t2.cusip
  AND t2.factset_entity_id=t4.factset_entity_id
  AND t1.month=t4.month;

ALTER TABLE v1_holdings13f
  ALTER COLUMN quarter TYPE int;
ALTER TABLE v1_holdingsmf
  ALTER column quarter type int;

/*roll forward missing report*/
-- CREATE function quarter_add(q int,x int)

CREATE function quarter_add(q int,x int)
RETURNS INT
AS
  $$
  DECLARE byear integer;
          bquarter integer;
          ayear integer;
          aquarter integer;
          result integer;
  BEGIN
  byear=floor(q/100);
  bquarter=mod(q,10);
  ayear=floor(x/4);
  aquarter=(x-4*ayear);
  IF aquarter+aquarter>4 THEN
     result =(byear+ayear+1)*100+(bquarter+aquarter-4)::int;
  ELSE
     result =(byear+ayear)*100+bquarter+aquarter;
  END IF;
  RETURN result;
  END;
  $$
  LANGUAGE plpgsql;

CREATE TABLE cusip_range AS
  SELECT cusip, cast(extract(year FROM max(termination_date))*100+extract( quarter FROM max(termination_date)) AS int) AS maxofqtr
 FROM own_basic_selected
GROUP BY cusip;

-- ALTER TABLE cusip_range
-- ALTER COLUMN maxofqtr TYPE int;

/*range of dates based on 13f*/
CREATE TABLE rangeofdates AS
  SELECT DISTINCT quarter
  FROM v1_holdings13f
    WHERE quarter BETWEEN (SELECT sqtr()) AND (SELECT eqtr())
    ORDER BY quarter;

/*creates institution data pairs for rolling forward*/
CREATE TABLE insts_13f AS
  SELECT DISTINCT factset_entity_id
FROM v1_holdings13f  ORDER BY factset_entity_id;

CREATE TABLE insts_13fdates AS
  SELECT DISTINCT factset_entity_id, quarter
FROM insts_13f, rangeofdates
  ORDER BY factset_entity_id, quarter;

ALTER TABLE insts_13fdates
ALTER COLUMN quarter type int;
CREATE TABLE insts_mf AS
  SELECT DISTINCT factset_fund_id FROM v1_holdingsmf
ORDER BY factset_fund_id;

CREATE TABLE insts_mfdates AS
   SELECT DISTINCT factset_fund_id,quarter
FROM insts_mf,rangeofdates
ORDER BY factset_fund_id, quarter;
ALTER TABLE insts_mfdates
ALTER COLUMN quarter TYPE int;

CREATE TABLE pairs_13f AS
  SELECT DISTINCT factset_entity_id, quarter
    FROM v1_holdings13f
  ORDER BY factset_entity_id, quarter;
ALTER TABLE pairs_13f
ALTER COLUMN quarter TYPE int;

CREATE TABLE pairs_mf AS
  SELECT DISTINCT factset_fund_id,quarter
  FROM v1_holdingsmf
  ORDER BY factset_fund_id,quarter;
ALTER TABLE pairs_mf
ALTER COLUMN quarter TYPE int;
/*rolling*/
CREATE TABLE holds13frollfwd AS
  SELECT a.factset_entity_id, a.quarter,  max(b.quarter) AS maxofqtr

  FROM pairs_13f AS a, pairs_13f AS b
  WHERE a.quarter>b.quarter and a.factset_entity_id=b.factset_entity_id
  GROUP BY a.factset_entity_id, a.quarter;



CREATE TABLE holdsmfrollfwd AS
  SELECT a.factset_fund_id,max(b.quarter) AS maxofqtr, a.quarter
  FROM pairs_mf AS a, pairs_mf AS b
  WHERE a.factset_fund_id=b.factset_fund_id
       AND a.quarter>b.quarter
  GROUP BY a.factset_fund_id,a.quarter;

/*find missing reports*/
CREATE TABLE missing13f AS
  SELECT a.factset_entity_id,a.quarter
  FROM insts_13fdates AS a
  LEFT JOIN pairs_13f b
    ON (a.factset_entity_id=b.factset_entity_id and a.quarter=b.quarter)
  WHERE b.factset_entity_id IS NULL;


CREATE TABLE missingmf AS
  SELECT a.factset_fund_id,a.quarter
   FROM insts_mfdates AS a
   LEFT JOIN pairs_mf b ON (a.factset_fund_id=b.factset_fund_id AND a.quarter=b.quarter)
  WHERE b.factset_fund_id IS NULL;

CREATE TABLE fill_13f AS
  SELECT a.factset_entity_id,a.quarter,b.maxofqtr AS lagqtr
  FROM missing13f AS a, holds13frollfwd AS b
  WHERE a.factset_entity_id=b.factset_entity_id
     AND a.quarter>b.maxofqtr AND a.quarter<least(b.quarter,quarter_add(b.maxofqtr,8));
/*if a.quarter>b.quarter, shd use b.quarter report instead of b.maxofqtr*/
/*also if last report is so old that a.quarter>last report +8, do not fill*/
/*temporary previous calculation */
/*Also restriction a.quarter<b.quarter ensures that the fill table contains missing values that are smaller than maximum report date of an entity.*/

CREATE TABLE fill_mf AS
  SELECT a.factset_fund_id,a.quarter,b.maxofqtr AS lagqtr
  FROM missingmf AS a, holdsmfrollfwd AS b
  WHERE a.factset_fund_id=b.factset_fund_id
     AND a.quarter>b.maxofqtr AND a.quarter<least(b.quarter,quarter_add(b.maxofqtr,8));


CREATE FUNCTION maxdate() RETURNS INT
  LANGUAGE plpgsql
  AS
  $$
  DECLARE maxdate int;
  BEGIN
  SELECT cast(max(quarter)AS int) FROM rangeofdates into maxdate ;
  RETURN maxdate;
  END;
  $$
/*fill in last possibly not yet reported entry*/

CREATE TABLE holds13f_rollfwdlast AS
  WITH temp AS (
    SELECT factset_entity_id, max(quarter) AS maxofqtr
    FROM pairs_13f
    GROUP BY factset_entity_id)


    SELECT factset_entity_id, maxofqtr, quarter_add(maxofqtr,1)AS qtr_start, least(quarter_add(maxofqtr,3),maxdate()) AS qtr_end
    FROM temp
    GROUP BY factset_entity_id, maxofqtr
HAVING maxofqtr > quarter_add(maxdate(),-3) AND maxofqtr<maxdate();

/*fill in missing value that is after the maximum report date of an entity*/
/*this applies to entities with annual frequency that have not reported yet by the maximum date in the daterange*/
INSERT INTO fill_13f
  (
SELECT
  a.factset_entity_id,
  a.quarter,
  b.maxofqtr AS lagqtr
FROM missing13f AS a, holds13f_rollfwdlast AS b
WHERE a.factset_entity_id=b.factset_entity_id
     AND a.quarter >= b.qtr_start
     AND a.quarter <= b.qtr_end
  );

CREATE TABLE holdsmf_rollfwdlast AS
  WITH temp AS (
    SELECT factset_fund_id, max(quarter) AS maxofqtr
    FROM pairs_mf
    GROUP BY factset_fund_id)

    SELECT factset_fund_id, maxofqtr, quarter_add(maxofqtr,1)AS qtr_start, least(quarter_add(maxofqtr,3),maxdate()) AS qtr_end
    FROM temp
    GROUP BY factset_fund_id, maxofqtr
HAVING maxofqtr > quarter_add(maxdate(),-3) AND maxofqtr<maxdate();

INSERT INTO fill_mf
  (
SELECT
  a.factset_fund_id,
  a.quarter,
  b.maxofqtr AS lagqtr
FROM missingmf AS a, holdsmf_rollfwdlast AS b
WHERE a.factset_fund_id=b.factset_fund_id
     AND a.quarter >= b.qtr_start
     AND a.quarter <= b.qtr_end
  );

/*create actual insert tables use a to insert b*/
CREATE table inserts_13f AS
  SELECT
    b.factset_entity_id, a.cusip, b.quarter, a.quarter AS ref_quarter,
         a.holding, a.io
  FROM fill_13f AS b, v1_holdings13f AS a, cusip_range AS c
  WHERE a.factset_entity_id=b.factset_entity_id
    AND a.quarter=b.lagqtr
    AND a.cusip=c.cusip
    AND (b.quarter<=c.maxofqtr OR c.maxofqtr IS NULL)
  ORDER BY b.factset_entity_id, b.quarter, a.cusip;

CREATE table inserts_mf AS
  SELECT
    b.factset_fund_id, a.cusip, b.quarter, a.quarter AS ref_quarter,
         a.holding, a.io
  FROM fill_mf AS b, v1_holdingsmf AS a, cusip_range AS c
  WHERE a.factset_fund_id=b.factset_fund_id
    AND a.quarter=b.lagqtr
    AND a.cusip=c.cusip
    AND (b.quarter<=c.maxofqtr OR c.maxofqtr IS NULL)
  ORDER BY b.factset_fund_id, b.quarter, a.cusip;

CREATE TABLE v2_holdings13f AS
SELECT factset_entity_id, cusip, quarter, holding,io
   FROM v1_holdings13f
       UNION ALL
SELECT factset_entity_id, cusip, quarter, holding,io
   FROM inserts_13f;

CREATE TABLE v2_holdingsmf AS  /*sum at fund manager level*/
  WITH merge AS (
    SELECT factset_fund_id, cusip, quarter, holding, io
       FROM v1_holdingsmf
        UNION ALL
       SELECT factset_fund_id, cusip, quarter, holding, io
       FROM inserts_mf
    )
    /*sum funds to entity level*/
SELECT factset_entity_id, cusip, quarter, sum(holding) AS holding, sum(io) AS io
  FROM merge AS t1, own_funds AS t2
  WHERE t1.factset_fund_id=t2.factset_fund_id
    GROUP BY factset_entity_id, cusip,quarter;

/*13F securities include non-US stocks*/
/*example: Deutsche bank ordinary shares CUSIP D18190898*/
/*Now merge 13f with mf tables*/

CREATE TABLE inst_quarter_mf AS
  SELECT DISTINCT factset_entity_id,quarter
  FROM v2_holdingsmf;
CREATE TABLE inst_quarter_13f AS
  SELECT DISTINCT factset_entity_id,quarter
  FROM v2_holdings13f;

CREATE TABLE inst_quarter_13f_only AS
SELECT a.factset_entity_id, a.quarter
FROM inst_quarter_13f AS a
       LEFT JOIN inst_quarter_mf AS b ON (a.factset_entity_id = b.factset_entity_id and a.quarter = b.quarter)
WHERE b.factset_entity_id IS NULL
  AND b.quarter IS NULL;

CREATE TABLE inst_quarter_mf_only AS
  SELECT a.factset_entity_id, a.quarter
    FROM inst_quarter_mf AS a
      LEFT JOIN  inst_quarter_13f AS b ON (a.factset_entity_id=b.factset_entity_id AND a.quarter=b.quarter)
      WHERE b.factset_entity_id IS NULL
       AND b.quarter IS NULL;

CREATE TABLE inst_quarter_both AS
  SELECT a.factset_entity_id, a.quarter
     FROM inst_quarter_mf AS a, inst_quarter_13f b
  WHERE a.factset_entity_id=b.factset_entity_id
     AND a.quarter=b.quarter;


/*so far entity-cusip level io*/
CREATE TABLE v1_holdingsall AS
   WITH unionall AS (
     /*all 13f*/
     SELECT factset_entity_id,cusip,quarter,io
         FROM v2_holdings13f
      UNION ALL
     /*plus mf only*/
     SELECT b.factset_entity_id, b.cusip, b.quarter, b.io
         FROM inst_quarter_mf_only AS a , v2_holdingsmf AS b
            WHERE a.factset_entity_id=b.factset_entity_id
            AND a.quarter=b.quarter
      UNION ALL
     SELECT c.factset_entity_id, c.cusip, c.quarter,c.io
         FROM inst_quarter_both AS a, own_basic_selected AS b,v2_holdingsmf AS c
     WHERE c.cusip=b.cusip
     AND b.iso_country !='US'
     AND a.quarter=c.quarter
     AND a.factset_entity_id=c.factset_entity_id
     )

SELECT factset_entity_id, cusip, quarter,max(io) AS io
FROM unionall
GROUP BY factset_entity_id, cusip, quarter,io;

/*aggregate at company level for each entity*/
CREATE TABLE v2_holdingsall AS
  SELECT a.factset_entity_id, b.factset_entity_id AS company_id, a.quarter,
         c.iso_country AS inst_country, d.iso_country AS sec_country, c.entity_sub_type, sum(a.io) AS io
/*this is summation at entity-company-quarter level*/
  FROM v1_holdingsall AS a, own_basic_selected AS b, edm_standard_entity AS c, edm_standard_entity AS d
  WHERE a.cusip=b.cusip
    AND b.factset_entity_id=d.factset_entity_id
    AND a.factset_entity_id=c.factset_entity_id
    AND b.factset_entity_id IS NOT NULL
    AND a.io IS NOT NULL
  GROUP BY a.factset_entity_id, b.factset_entity_id,a.quarter, c.iso_country, d.iso_country,c.entity_sub_type;

/*sum at company level across all entities*/
/*for io >1, truncate at 1*/


/*simple io metrics to begin with: domestic and foreign io*/
/*the use of adjustment factor covers some potential underlying data error*/
CREATE TABLE adjfactor AS
  SELECT company_id, quarter, sum(io) AS io, greatest(sum(io),1) AS adjf
FROM v2_holdingsall
GROUP BY company_id, quarter;


/*now calculate io metrics: domestic, US, foreign*/
EXPLAIN CREATE TABLE v3_holdingsall AS
  SELECT t1.company_id, t1.quarter, t1.factset_entity_id, t1.sec_country, t1.inst_country,
         (CASE WHEN t1.sec_country=t1.inst_country THEN 1
         ELSE 0 END) AS is_dom,
         (CASE WHEN t1.inst_country='US' THEN 1
           ELSE 0 END) AS is_us_inst,
         t1.io/t2.adjf AS io_adj,t1.io AS io_unadj
  FROM v2_holdingsall t1, adjfactor t2
WHERE t1.company_id=t2.company_id AND  t1.quarter=t2.quarter

ORDER BY t1.company_id, t1.quarter, io_adj DESC;

/*calculate IO*/

CREATE TABLE holdings_by_firm1 AS
  SELECT
  company_id,quarter,sec_country,count(*) AS nbr_firms,
  sum(io_adj) AS io, /*summation across entities*/
  sum(io_adj*is_dom)  AS io_dom,
  sum(io_adj*(1-is_dom)) AS io_for,
  sum(io_adj*(1-is_dom)*is_us_inst) AS io_for_us,
  sum(io_adj*(1-is_dom)*(1-is_us_inst)) AS io_for_nus
FROM v3_holdingsall
GROUP BY company_id,quarter,sec_country;

SELECT max(io) FROM holdings_by_firm1;

/*put market capitalization along with ownership ratio.*/

/*include market cap*/
CREATE TABLE holdings_by_firm2 AS
  SELECT a.*,c.entity_proper_name,b.own_mv
  FROM holdings_by_firm1 AS a, own_mktcap AS b,entity_identifiers AS c
  WHERE a.company_id=b.factset_entity_id
  AND a.company_id=c.factset_entity_id
  AND a.quarter=b.quarter
  AND b.eoq=1;

SELECT count(*) FROM holdings_by_firm2 WHERE io>1;
/*40555 entriees larger than 1*/
SELECT count(*) FROM holdings_by_firm_msci WHERE io>1;
/*8295 entries larger than 1*/

/*exclude  REIT*/
CREATE TABLE  holdings_by_firm_all AS
SELECT *
FROM holdings_by_firm2
  WHERE company_id NOT in (SELECT factset_entity_id FROM edm_standard_entity WHERE primary_sic_code ='6798');

CREATE TABLE holdings_by_firm_final AS
    SELECT*
    FROM ctry AS a, holdings_by_firm_all AS b
    WHERE a.iso=b.sec_country;


SELECT *
FROM holdings_by_firm_final
WHERE company_id='000BDW-E' AND quarter=200301;

select * FROM holdings_by_firm_msci
WHERE factset_entity_id='000BDW-E' AND quarter=200301;

SELECT COUNT(*) FROM holdings_by_firm_msci; /*1410895 entries*/
SELECT COUNT(*) FROM holdings_by_firm_final; /*1625546 entries*/
/*This difference roughly matches the 119742 market cap entries matched with datastream.*/

SELECT * FROM holdings_by_firm_final LIMIT 10;

-- SELECT count(*) FROM holdings_by_firm_final AS a, holdings_by_firm_msci AS b
-- WHERE a.company_id=b.factset_entity_id
-- AND a.quarter=b.quarter
-- AND a.io=b.io; /*4656 records that are exactly the same.*/

/*following are some tests for the consistency between io i calculated and io petro calculates*/
/*conclusion, figures are highly similar although there are come minor inconsistencies due to different market cap.*/
/*I suspect that very larger ownership ratio is a consequence of erroneous market cap data*/
/*so find market cap that is very small from their final table*/
with min AS (SELECT MIN(mktcap) AS mincap  FROM holdings_by_firm_msci )
SELECT * FROM holdings_by_firm_msci AS a, min AS b where a.mktcap=b.mincap; /*13 small mkt cap*/


/*turns out that their marketcap has minimum value of 0*/

SELECT * FROM holdings_by_firm_msci WHERE io>1 LIMIT 10;
/*turns out that the cases in which ownership ratio is larger than 1 is not simplyh due to very small mkt cap*/

/*check one case*/
--SELECT * FROM own_mktcap WHERE factset_entity_id='000BFS-E' AND quarter=200201;
/*this section checks whether ownship ratio I calculate matches that calculated by petro*/
CREATE TABLE checkmatch AS
  SELECT DISTINCT factset_entity_id FROM holdings_by_firm_msci LIMIT 4;
create table match AS
  SELECT factset_Entity_id, quarter
  FROM holdings_by_firm_msci WHERE
  factset_entity_id='0C9CBX-E' LIMIT 10;

SELECT a.factset_entity_id AS company_id,a.quarter,  io,io_dom,io_for,io_for_us,io_for_nus,entity_proper_name,mktcap
FROM holdings_by_firm_msci AS a, checkmatch AS b
WHERE a.factset_entity_id=b.factset_entity_id

UNION ALL

SELECT company_id,c.quarter,io,io_dom,io_for,io_for_us,io_for_nus,entity_proper_name,own_mv FROM holdings_by_firm_final AS c, checkmatch AS d
WHERE c.company_id=d.factset_entity_id;

(SELECT quarter,own_mv from own_mktcap WHERE factset_Entity_id= '000BDW-E' AND eoq=1 LIMIT 10)
union all
(select quarter,mktcap FROM holdings_by_firm_msci WHERE factset_Entity_id='000BDW-E' LIMIT 10);

/*I think they use slightly different market cap data from ownership database.*/

SELECT * FROM own_mktcap_raw WHERE  factset_entity_id= '000BDW-E' AND eoq=1;

/*next step is to create table for each country and winsorize*/
/*annual frequency for plots add end of year indicator*/
ALTER TABLE holdings_by_firm_final
ADD COLUMN year int,
ADD COLUMN eoy int;
UPDATE holdings_by_firm_final
  SET eoy=(CASE WHEN MOD(quarter, 100)=4 THEN 1
    ELSE  0 END
    ),
      year=floor(quarter/100);

/*INCLUDE MKT CAP AND IO PERCENTILE by country by quarter*/

ALTER TABLE holdings_by_firm_final
ADD COLUMN io_pct double precision,
ADD COLUMN mv_pct double precision,
ADD COLUMN io_dom_pct double precision,
ADD COLUMN io_for_pct double precision,
ADD COLUMN io_us_pct double precision,
ADD COLUMN io_nus_pct double precision;


UPDATE holdings_by_firm_final AS a
SET io_pct=iocum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY IO)AS iocum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;


UPDATE holdings_by_firm_final AS a
SET mv_pct=mvcum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY own_mv)AS mvcum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;


UPDATE holdings_by_firm_final AS a
SET io_dom_pct=iodomcum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY io_dom)AS iodomcum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;

UPDATE holdings_by_firm_final AS a
SET io_for_pct=ioforcum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY io_for)AS ioforcum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;

UPDATE holdings_by_firm_final AS a
SET io_us_pct=ioforuscum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY io_for_us)AS ioforuscum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;

UPDATE holdings_by_firm_final AS a
SET io_nus_pct=iofornuscum FROM (SELECT company_id,quarter, cume_dist() OVER( PARTITION BY ctry,quarter ORDER BY io_for_nus)AS iofornuscum
  FROM holdings_by_firm_final) AS b
WHERE a.company_id=b.company_id AND a.quarter=b.quarter;

/*count observation each country each quarter*/
CREATE TABLE obscount AS
  SELECT DISTINCT ctry,quarter FROM holdings_by_firm_final
ORDER BY ctry,quarter;
ALTER TABLE obscount
ADD COLUMN obs int;

UPDATE obscount AS a
SET obs= count FROM (SELECT COUNT(*) AS count,ctry,quarter FROM holdings_by_firm_final
  GROUP BY ctry,quarter) AS b
WHERE a.ctry=b.ctry and a.quarter=b.quarter;

/*output tables at country-quarter level*/
DO $$
  DECLARE entry record;
BEGIN
FOR entry in SELECT DISTINCT ctry,quarter AS qtr FROM obscount
LOOP
EXECUTE format('CREATE TABLE %I AS SELECT
  company_id, quarter,
  io, io_pct,
  io_dom, io_dom_pct,
  io_for,io_for_pct,
  io_for_us,io_us_pct,
  io_for_nus,io_nus_pct,
  own_mv,mv_pct
FROM holdings_by_firm_final
WHERE ctry=%L AND quarter=%L',  concat(trim(entry.ctry),entry.qtr),trim(entry.ctry),entry.qtr);
EXECUTE format('COPY %I  TO ''C:\Users\ylu115\Documents\Institutional preference\factset_io\plot\%s.csv'' DELIMITER '','' CSV HEADER',concat(trim(entry.ctry),entry.qtr),concat(trim(entry.ctry),entry.qtr));
END LOOP;
END
$$;

 DO $$
   DECLARE ctry varchar;
   DECLARE qtr int;
     BEGIN
   ctry='USA';
   qtr=200401;
EXECUTE format('CREATE TABLE %I AS SELECT
  company_id, quarter,
  io, io_pct,
  io_dom, io_dom_pct,
  io_for,io_for_pct,
  io_for_us,io_us_pct,
  io_for_nus,io_nus_pct,
  own_mv,mv_pct
FROM holdings_by_firm_final
WHERE ctry=%L AND quarter=%L',  concat(trim(ctry),qtr),ctry,qtr);
EXECUTE format('COPY %I  TO ''C:\Users\ylu115\Documents\Institutional preference\factset_io\plot\%s.csv'' DELIMITER '','' CSV HEADER',concat(trim(ctry),qtr),concat(trim(ctry),qtr));
END
$$;



COPY obscount  TO 'C:\Users\ylu115\Documents\Institutional preference\factset_io\plot\obscount.csv' DELIMITER ',' CSV HEADER;

/*output tables at country level*/

DO $$
  DECLARE entry record;
BEGIN
FOR entry in SELECT DISTINCT ctry FROM obscount
LOOP
-- EXECUTE format('CREATE TABLE %I AS SELECT
--   company_id, quarter,
--   io, io_pct,
--   io_dom, io_dom_pct,
--   io_for,io_for_pct,
--   io_for_us,io_us_pct,
--   io_for_nus,io_nus_pct,
--   own_mv,mv_pct
-- FROM holdings_by_firm_final
-- WHERE ctry=%L',  trim(entry.ctry),trim(entry.ctry));
EXECUTE format('COPY %I  TO ''F:\Institutional preference\factset_io\plot\%s.csv'' DELIMITER '','' CSV HEADER',trim(entry.ctry),trim(entry.ctry));
END LOOP;
END
$$;

COPY holdings_by_firm_final  TO 'C:\Users\ylu115\Documents\Institutional preference\factset_io\plot\holdings_by_firm_all.csv' csv HEADER;



/*merge factset with  morningstar*/

SELECT * FROM own_funds WHERE factset_fund_id like '0000%';
/*small stock ETF*/


SELECT * FROM etf_all  AS a, own_funds AS b
WHERE a.fundid=format('FS%s',b.factset_fund_id);

SELECT count(DISTINCT factset_fund_id) FROM own_funds;

SELECT count(*) FROM etf_all;

--finally imported without errors. errors due to ',' in numbers
select * from OWN_FUNDS WHERE fund_type='ETF';

--filter ETF for small stocks
CREATE TABLE small_etf AS
  SELECT *
  FROM etf_all AS a
  WHERE "Global_Broad_category"= 'Equity'
  AND( "Global_category" LIKE '%Small%'
  OR "Morningstar_Category" LIKE '%Small%'
  OR "Global_Investment_Fund_Sector" LIKE '%Small%'
  OR "Morningstar_Institutional_Category" LIKE '%Small%'
  OR  "Equity_style_box" LIKE '%Small%'
  OR "name" LIKE '%Small%'
  OR "Primary_Prospectus_Benchmark" LIKE '%Small%'
  OR "Prospectus_Objective" LIKE '%Small%'
  OR "Strategy_Name" LIKE '%Small%'
  OR "FTSE_Russell_Benchmark" like '%Small%');

SELECT count(*)
FROM small_etf;

/*many U.S etfs miss investment areas, fill in according to US categories*/
UPDATE small_etf AS a
SET "Investment Area"='United States of America'
WHERE "US_Category_Group"='U.S. Equity'
AND "Investment Area" IS NULL;


SELECT count(*) FROM small_etf
WHERE "US_Category_Group"='U.S. Equity'
AND "Investment Area" IS NULL;

SELECT COUNT(*) FROM small_etf
WHERE "Investment Area" IS NULL;

SELECT * FROM small_etf
WHERE "Investment Area" IS NULL;

ALTER TABLE small_etf_unique
ADD COLUMN is_index int;

CREATE TABLE small_etf_unique AS
  SELECT name,"Global_category","Morningstar_Category","Global_Investment_Fund_Sector",
         "Morningstar_Institutional_Category","Investment Area","US_Category_Group",
         "domicile", "Equity_style_box","Firm_Name","Branding_Name","Primary_Prospectus_Benchmark",
         "exchange","Exchange_Country","secid","fundid","Strategy_Name",
         "Management_Company","FTSE_Russell_Benchmark","Primary_Share"
  FROM small_etf
  WHERE "Primary_Share"='Yes';

SELECT count(*)
FROM small_etf_unique
WHERE "Investment Area" IS NULL;

SELECT COUNT(DISTINCT sec_country) FROM holdings_by_firm_msci;