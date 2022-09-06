dotmaticQuery = """
select listagg(recep_red_n,'; ') WITHIN GROUP (ORDER BY compound_id) AS recep
FROM 
(select compound_id, receptor||'= '||avg(round(reduction,2))||'(n='||count(reduction)||')' as recep_red_n
from ds3_userdata.EWB_FNHR_INV_AG_3PT_NEW  WHERE  reduction >50 AND conc=10000 --AND compound_id=:compound_id
group by compound_id,receptor)
group by compound_id
"""

databricksQuery = """
select concat_ws('; ', collect_list(recep_red_n)) as recep from (
	select compound_id, receptor||'= '||avg(round(reduction,2))||'(n='||count(reduction)||')' as recep_red_n from (
		select 
			compound_id,
			RECEPTOR,
			max(REDUCTION * 100) as REDUCTION,
			conc
		from (
			select
				cec.ssn,cec.compound_id, cec.batch, cec.EXP_COND_ID,
				max(decode(cec.att_name,'RECEPTOR', cec.att_val)) as RECEPTOR,
				max(decode(cdr.att_name,'REDUCTION', cdr.att_val)) as REDUCTION,
				max(decode(cdr.Independent_Variable_type ,'Concentration', cdr.Independent_Variable_value)) as CONC
			from rnd_rncd_common_trusted.cdb_detailed_results  cdr
			join rnd_rncd_common_trusted.cdb_experimental_conditions cec on (cdr.EXP_COND_ID=cec.EXP_COND_ID)
			where  
				cdr.Assay_Name = 'FNHR PANEL'
			group by  cec.ssn,cec.compound_id, cec.batch, cec.EXP_COND_ID
		) where receptor IN ('RORa', 'RORg')
		group by compound_id, RECEPTOR, conc
	) where reduction >50 
	and conc=10000
	--and compound_id=:compound_id
	group by compound_id, receptor, reduction 
	order by receptor, reduction 
) group by compound_id 
order by compound_id
"""

databricksResult = None
dotmaticsResult = None

import jaydebeapi

with jaydebeapi.connect("com.databricks.client.jdbc.Driver",
                        "jdbc:databricks://sagerx-aws-devtest-rnd.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/endpoints/17e578c497c3dece;",
                        {"user": "token", "password": "<token>"},
                        ["DatabricksJDBC42.jar","ojdbc8.jar"],) as databricksConn:
	with databricksConn.cursor() as databricksCurs:
		databricksCurs.execute(databricksQuery)

		databricksResult = databricksCurs.fetchall()

with open("databricks-result.py", "w") as dbf:
	dbf.write("databricksResult = "+ str([[y for y in  list(x)] for x in databricksResult]))


# print(f"Databricks => \n{databricksResult}")


with jaydebeapi.connect("oracle.jdbc.driver.OracleDriver",
                        "jdbc:oracle:thin:svc_prod_databricks_cider/Mxo_UsQ8Z8yM@SAGE-ORACLE.corp.com:1521:PROD",
                        {},
						["DatabricksJDBC42.jar","ojdbc8.jar"]) as dotmaticsConn:
	with dotmaticsConn.cursor() as dotmaticsCurs:
		dotmaticsCurs.execute(dotmaticQuery)

		dotmaticsResult = dotmaticsCurs.fetchall()

with open("dotmatics-result.py", "w") as dbf:
	dbf.write("dotmaticsResult = "+ str([[y for y in  list(x)] for x in dotmaticsResult]))


# print(f"Dotmatics => \n{dotmaticsResult}")

assert set(databricksResult) == set(dotmaticsResult), "Results doesn't match\n"



