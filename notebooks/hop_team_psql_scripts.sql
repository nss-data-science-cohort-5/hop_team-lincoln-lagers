
	-- NSS DS5 - hop_teaming PSQL IMPORT SCRIPTS --

-- 1) CREATE DATABASE

CREATE DATABASE hop_teaming
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;



-- 2) CREATE TABLE SCRIPTS

CREATE TABLE IF NOT EXISTS docgraph
(
	pk_id     			INT PRIMARY KEY,
	from_npi 			VARCHAR(10), 
	to_npi 				VARCHAR(10), 
	patient_count 		int, 
	transaction_count 	int, 
	average_day_wait  	numeric, 
	std_day_wait 		numeric
);
	

CREATE TABLE IF NOT EXISTS npi_data
(
	pk_id_rownum				SERIAL PRIMARY KEY,
	NPI							VARCHAR(10),
	Entity_Type_Code		   	TEXT,
	Provider_Organization_Name 	TEXT,
	Provider_Last_Name			TEXT,
	Provider_First_Name			TEXT,
	Provider_Middle_Name		TEXT,
	Provider_Name_Prefix		TEXT,
	Provider_Name_Suffix 		TEXT,
	Provider_Credential 		TEXT,	
Provider_First_Line_Business_Practice_Location_Address TEXT,
Provider_Second_Line_Business_Practice_Location_Address TEXT,	
Provider_Business_Practice_Location_Address_City_Name TEXT,
Provider_Business_Practice_Location_Address_State_Name TEXT,
Provider_Business_Practice_Location_Address_Postal_Code TEXT,
	Combined_Taxonomy 			TEXT
);


CREATE TABLE IF NOT EXISTS nucc_taxonomy
(
    code 			TEXT,
    "grouping" 		TEXT,
    classification 	TEXT,
    specialization  TEXT,
    definition      TEXT,
    notes  			TEXT,
    displayname  	TEXT,
    section 		TEXT
);


CREATE TABLE IF NOT EXISTS zip_cbsa
(
	zip 				VARCHAR(10),   
	cbsa 				VARCHAR(10), 
	usps_zip_pref_city 	TEXT,  
	usps_zip_pref_state TEXT,
	res_ratio  			numeric, 
	bus_ratio 			numeric,  
	oth_ratio 			numeric,  
	tot_ratio 			numeric  
);



-- 3) IMPORTING FILES

/*
	REFRESH "Tables" AND RIGHT-CLICK docgraph > Import/Export.  
	
	UNDER THE Options TAB, SELECT Import AND Header > Yes.  Make sure to select ',' FOR Delimiter AND " FOR Escape.
	
	UNDER THE Columns TAB, X OUT (REMOVE) pk_id FROM THE docgraph TABLE WHEN YOU IMPORT IT.  FOR THE OTHER TABLES, YOU CAN LEAVE THE COLUMNS AS THEY ARE. SEE THE GOOGLE DOC AT https://docs.google.com/document/d/1FXnvbX5XDcQ0XMYLK5D7ruW--qlIr47OA6oGjjsubRY/edit FOR SCREEN SHOTS.  REPEAT THIS PROCESS FOR THE OTHER TABLES AFTER YOU HAVE CREATED THEM WITH THE SCRIPTS.
	
	FOR THE npi_data .csv FILE, WE ARE USING CONNOR'S VERSION WITH THE Combined_Taxonomy COLUMN.  TO CREATE THIS FILE, SEE CONNOR'S npi_to_sqlv2.ipynb OR MY COPY OF IT AT https://github.com/nss-data-science-cohort-5/hop_team-lincoln-lagers/. 
	
	FOR THE ZIP_CBSA_122021.xlsx FILE, OPEN IT IN LIBREOFFICE AND SAVE IT AS A .csv FILE WITHOUT CHANGING ANYTHING ELSE.  THEN IMPORT THE .csv FILE.  THIS LETS US USE THE IMPORT TOOL AND ALSO REPLACES THE LEADING ZEROES THAT EXCEL TRUNCATES FROM THE ZIP CODES.  I AM NOT SURE WHETHER THIS WORKS IN MS EXCEL AS WELL, BUT IT IS WORTH A TRY.  IF NOT, LIBREOFFICE IS FREE TO DOWNLOAD.
*/



-- 4) ROW COUNTS TO CHECK DATA 

SELECT COUNT(*)		-- 21,791,1308 ROWS
FROM docgraph;

SELECT COUNT(*)		-- 7,189,078 ROWS
FROM npi_data;

SELECT COUNT(*)		-- 868 ROWS
FROM nucc_taxonomy;

SELECT COUNT(*)		-- 47,484 ROWS
FROM zip_cbsa;




-- 5) ELIMINATE '.0' IN npi_data Entity_Type_Code AND postal_code COLUMNS

UPDATE npi_data
SET entity_type_code = 
REPLACE(entity_type_code, '.0', '');


UPDATE npi_data
SET provider_business_practice_location_address_postal_code = 
REPLACE(provider_business_practice_location_address_postal_code, '.0', '');	




-- 6) CREATE TABLE INDEXES (POSSIBLY MORE TO FOLLOW ON LARGER TABLES)

CREATE UNIQUE INDEX docgraph_pk_ix
ON docgraph (pk_id);

-- SEE devcenter.heroku.com/articles/postgresql-indexes
CREATE INDEX docgraph_avg_day_wait_ix
ON docgraph (average_day_wait)
WHERE average_day_wait <= 100;

CREATE INDEX docgraph_trans_count_ix
ON docgraph (transaction_count)
WHERE transaction_count >= 50;



CREATE UNIQUE INDEX npi_data_pk_ix
ON npi_data (pk_id_rownum);

CREATE INDEX npi_data_npi_ix
ON npi_data (npi);



CREATE INDEX nucc_taxonomy_classification_ix
ON nucc_taxonomy (classification);

CREATE INDEX nucc_taxonomy_code_ix
ON nucc_taxonomy (code);

CREATE INDEX nucc_taxonomy_specialization_ix
ON nucc_taxonomy (specialization);



CREATE INDEX zip_cbsa_zip_ix
ON zip_cbsa (zip);




-- 7) EXAMPLE JOINS FOR EXPLORATORY DATA ANALYSIS

SELECT nd.npi,
	nd.provider_organization_name AS org_name,
	CASE WHEN nd.entity_type_code = '1' THEN 'Provider'
		WHEN nd.entity_type_code = '2' THEN 'Facility'
		END AS entity_type,
	d.from_npi,
	d.to_npi,
	tx.code AS taxonomy_code,
	tx.classification,	-- DO WE CONCATENATE THESE?
	tx.specialization,
	d.patient_count,
	d.transaction_count,
	d.average_day_wait,
nd.Provider_First_Line_Business_Practice_Location_Address || ' ' || nd.Provider_Second_Line_Business_Practice_Location_Address AS Street_Address,
nd.Provider_Business_Practice_Location_Address_City_Name AS City,
nd.Provider_Business_Practice_Location_Address_State_Name AS "State",
	z.zip,
	z.cbsa
FROM npi_data nd
	INNER JOIN docgraph d
		ON CASE WHEN nd.entity_type_code = '1' THEN nd.npi = d.from_npi
		ELSE nd.npi = d.to_npi END
	INNER JOIN nucc_taxonomy tx
		ON nd.Combined_Taxonomy = tx.code
	INNER JOIN zip_cbsa z
		ON LEFT(nd.Provider_Business_Practice_Location_Address_Postal_Code, 5) = z.zip
WHERE d.transaction_count >= 50
	AND d.average_day_wait <= 100
LIMIT 2;



