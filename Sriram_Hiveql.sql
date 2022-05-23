-- Databricks notebook source
drop table if exists clinicaltrail_2021;
drop table if exists pharma_data;
drop table if exists mesh_data;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_path = "/FileStore/tables/clinicaltrial_2021_csv.gz"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def file_exists(path):
-- MAGIC     try:
-- MAGIC         dbutils.fs.ls(path)
-- MAGIC         return True
-- MAGIC     except Exception as e:
-- MAGIC         if 'java.io.FileNotFoundException' in str(e):
-- MAGIC             return False
-- MAGIC         else:
-- MAGIC             raise

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if file_exists(file_path) == True:
-- MAGIC     fileroot = file_path.split("/")[3].split(".")[0]
-- MAGIC     dbutils.fs.cp(file_path,"file:/tmp/")
-- MAGIC else:
-- MAGIC     print("File is not found in dbfs")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC gzip -d /tmp/ /tmp/$fileroot.gz

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mv("file:/tmp/" + fileroot,"/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if fileroot.endswith("_csv"):
-- MAGIC     fileroot_new = fileroot.replace("_csv",".csv")
-- MAGIC dbutils.fs.mv("/FileStore/tables/" + fileroot,"/FileStore/tables/" + fileroot_new)

-- COMMAND ----------

CREATE TABLE if not exists clinicaltrail_2021(
ID STRING,
Sponsor STRING,
Status STRING,
Start STRING,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' '|')
LOCATION '/FileStore/tables/clinicaltrial_2021.csv';

-- COMMAND ----------

CREATE TABLE if not exists pharma(
Company STRING,
Parent_Company STRING,
Penalty_Amount STRING,
Subtraction_From_Penalty STRING,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING,
Penalty_Year STRING,
Penalty_Date STRING,
Offense_Group STRING,
Primary_Offense STRING,
Secondary_Offense STRING,
Description STRING,
Level_of_Government STRING,
Action_Type STRING,
Agency STRING,
Civil_Criminal STRING,
Prosecution_Agreement STRING,
Court STRING,
Case_ID STRING,
Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,
Facility_State STRING,
City STRING,
Address STRING,
Zip STRING,
NAICS_Code STRING,
NAICS_Translation STRING,
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' ',')
LOCATION '/FileStore/tables/pharma.csv';

-- COMMAND ----------

create table if not exists mesh(
term STRING,
tree STRING)
USING csv OPTIONS ( 'multiLine' 'true', 'escape' '"', 'header' 'true', 'delimiter' ',')
LOCATION '/FileStore/tables/mesh.csv';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 1

-- COMMAND ----------

select distinct count(Id) as distinct_count from clinicaltrail_2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 2

-- COMMAND ----------

select Type,count(Type) as frequency from clinicaltrail_2021 group by Type order by frequency desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 3

-- COMMAND ----------

select conditions_split as conditions,count(conditions_split) as frequency from (
      select conditions_split from clinicaltrail_2021 lateral view explode(split(conditions,','))
      conditions AS conditions_split)
      group by conditions_split order by frequency desc limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 4

-- COMMAND ----------

select tree_code,count(tree_code) as frequency from
       (select conditions_split from clinicaltrail_2021 
        lateral view explode(split(conditions,',')) conditions AS conditions_split ) as clinc 
        left outer join 
        (select term,SPLIT(tree,'[\.]')[0] as tree_code from mesh) as mesh
        on clinc.conditions_split=mesh.term
        group by tree_code order by frequency desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 5

-- COMMAND ----------

select sponsor, count(sponsor) as frequency 
      from clinicaltrail_2021 where sponsor not in (select parent_company from pharma) 
      group by sponsor order by frequency desc limit 10;

-- COMMAND ----------

select sponsor, count(sponsor) as frequency 
      from clinicaltrail_2021 left anti join pharma
      on clinicaltrail_2021.sponsor = pharma.parent_company
      group by sponsor order by frequency desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Question 6

-- COMMAND ----------

select split(Completion," ")[0] as completed_month, count(split(Completion," ")[0]) as frequency
       from clinicaltrail_2021
       where Status == "Completed" 
       group by completed_month,split(Completion," ")[1]
       having split(Completion," ")[1] == "2021" 
       order by from_unixtime(to_unix_timestamp(completed_month,'MMM'),'MM')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Further Analysis 1

-- COMMAND ----------

select Status,count(Status) as frequency,round(count(Status)*100/SUM(COUNT(Status)) OVER(),3) AS percentage_of_clinical_trails 
              from clinicaltrail_2021 group by Status order by frequency desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Further Analysis 2

-- COMMAND ----------

select split(Completion," ")[1] as completed_year,split(Start," ")[1] as start_year,
        from_unixtime(to_unix_timestamp(split(Start," ")[0],'MMM'),'MM') as start_month,
        from_unixtime(to_unix_timestamp(split(Completion," ")[0],'MMM'),'MM') as completed_month,
        (split(Completion," ")[1] - split(Start," ")[1])*12- 
        from_unixtime(to_unix_timestamp(split(Start," ")[0],'MMM'),'MM') +
        from_unixtime(to_unix_timestamp(split(Completion," ")[0],'MMM'),'MM') as number_of_months
       from clinicaltrail_2021 where Status = 'Completed'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Further Analysis 3

-- COMMAND ----------

select Type,round(avg((split(Completion," ")[1] - split(Start," ")[1])*12- 
        from_unixtime(to_unix_timestamp(split(Start," ")[0],'MMM'),'MM') +
        from_unixtime(to_unix_timestamp(split(Completion," ")[0],'MMM'),'MM')),3) as completed_month 
       from clinicaltrail_2021 where Status = 'Completed' group by Type

-- COMMAND ----------

/*To validate that Type Expnaded access doesnt have any status Completed*/
Select Type,Count(Type) as frequency from clinicaltrail_2021 where Status = 'Completed' group by Type 
