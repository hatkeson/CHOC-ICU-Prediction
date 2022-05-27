spark.sql("use real_world_data_dec_2021")

spark.sql("show tables").show(1000,0)

spark.sql("select * from encounter").printSchema()

spark.sql("select * from demographics").printSchema()

## Identify all inpatient pediatric-age encounters for hospitals with an ICU

### Identify all inpatient encounters of patients < 18 years

encounter.classification.standard.primaryDisplay = ‘Inpatient’ 

spark.sql("select classification.standard.primaryDisplay from encounter").distinct().show()

# note on changes: 

# 1. added birthdate for the year 

# 2. commented out one of the races 

# 3. utilized enc.personid and the enc.tenant

# 4. only inpatient data
from pyspark.sql import functions as F

enc = spark.sql("""
select enc.personid, encounterid, reasonforvisit.standard.primaryDisplay as reasonforvisit, 
    financialclass.standard.primarydisplay payer, admissionsource.standard.primaryDisplay admissionsource,
    hospitalizationstartdate, dem.birthdate, dischargedate, enc.tenant,
    ---aggregate(array_distinct(races.standard.primaryDisplay), '', (u, w) -> concat_ws(';',u,w)) race,
    status.standard.primaryDisplay status, 
    birthsex.standard.primaryDisplay as sex,
    ---coalesce()
    ---calculate_age
    
    --- patient level data
    aggregate(array_distinct(races.standard.primaryDisplay), '', (u, w) -> concat_ws(';',u,w)) race,
    aggregate(array_distinct(ethnicities.standard.primaryDisplay), '', (u,w) -> concat_ws(';',u,w)) ethnicity,
    aggregate(array_distinct(locations.name), '', (u,w) -> concat_ws(';',u,w)) encounterLocations
from encounter enc
---where to keep only encounters for which age at encounter is < 18 years
inner join demographics dem
on enc.personid = dem.personid
where enc.classification.standard.primaryDisplay = 'Inpatient'
""")
enc.cache()

# Exclusion criteria: LOS should > 1 hour

enc.columns

enc.show(1)

enc.createOrReplaceTempView('encdata')

spark.sql("select e.birthdate from demographics e limit 1").show()

### Getting their most current data


from pyspark.sql.functions import col, max as max_

temp = enc.withColumn("hospitalizationstartdate", col("hospitalizationstartdate").cast("timestamp")).groupBy("personid").agg(max_("hospitalizationstartdate"))
temp.columns

# obtain only the most recent hospitalization date
enc = enc.join(temp, on='personid', how ='inner')

### calculate the age of the patients

## calculating the age. Utilizes the dataframe instead of sql
from pyspark.sql.functions import *

# coneverts the birthdate from 0000-00-00 00:00:00 to 0000-00-00 and place it on birthday
enc = enc.withColumn("birthday",to_date("birthdate"))

# converts the birthday into age, this will have decimals but we only take the floor of the decimal so

# 17.8 = 17 not 18
enc = enc.withColumn('age', floor(datediff(current_date(), to_date(col('birthday'), 'yyyy/M/d'))/365.25))




### Getting only those who are less that 18 years old

enc = enc.where(enc.age < 18)



enc.show(1)

### demographics table summary

# row count
enc.count()

# get the number of males and females

enc.groupBy("sex").count().show()

# getting the number of races
from pyspark.sql.functions import col
enc.filter(col("race").like("%Asian%")).count()

from pyspark.sql.functions import col
print(enc.filter(col("race").like("%White%")).count())

print(enc.filter(col("race").like("%Hispanic%")).count())


print(enc.where(col("race").like("%Black%") | F.col("race").like('African American')).count())

print(enc.filter(col("race").like("%American Indian%")).count())
print(enc.filter(col("race").like("%Native Hawaiian%")).count())

print(enc.filter(col("race").like("%Caucasian%")).count())
print(enc.filter(col("race").like("%Other Race%")).count())
print(enc.filter(col("race").like("%Mixed racial group%")).count())

print(enc.filter(col("race").like("%Unknown racial group%")).count()) 

enc.select(mean ('age')).collect()

enc.select(stddev('age')).collect()

enc.select('race').distinct().show()

enc.groupBy('encounterLocations').count().show(n = 100)

### Identify hospitals (tenant where the encounter class is 'Inpatient') with an ICU
dataframe of one column (tenant) that meets condition. Encounter

d2 = enc.where('encounterLocations rlike "ICU"').select('tenant').distinct()

# d2 = spark.sql("""
# select distinct tenant
# from 
# encdata
# where encounterLocations rlike 'ICU'

# """)

### Identify inpatient pediatrics encounters at hospitals with an ICU
A tenant with at least on pediatric encounter class of 'Inpatient' and a facility location rlike 'ICU' ( like '%ICU%')
d3 
result = d1.join(d2, on='tenant', how ='inner')

result = enc.join(d2, on='tenant', how ='inner')

result.columns

### Getting lab, measurement and etc.

spark.sql("select * from lab").printSchema()

## takes a while to run (use this table for EDA)
per_lab = spark.sql("""select personid, labcode.standard.primaryDisplay as Type, 
interpretation.standard.primaryDisplay as Interpretation,
typedvalue.numericValue.value as value,
issueddate
from lab""")

per_lab.show(n = 20)

spark.sql("select labcode.standard.primaryDisplay as Type from lab").distinct().show()

spark.sql("select labcode.standard.primaryDisplay as Type from lab").distinct().count()

### converting data from long to wide format --> use this for model creation

from pyspark.sql.types import IntegerType
per_lab = per_lab.withColumn("value", per_lab["value"].cast(IntegerType()))

spark.conf.set('spark.sql.pivotMaxValues', u'50000')
temp_data = per_lab.groupBy("personid").pivot("Type").sum("value")






####

from pyspark.sql.functions import col, max as max_
temp_data_date = (per_lab.withColumn("datetime", col("issueddate").cast("timestamp"))
    .groupBy("personid")
    .agg(max_("datetime"))) # this groups by personid and captures only the latest date

join_temp_data = temp_data_date.join(per_lab, how = "inner", on = "personid")

columns_to_drop = ['max(datetime)', 'issueddate']
join_temp_data_dropped = join_temp_data.drop(*columns_to_drop)



temp_data_latest = join_temp_data_dropped.groupBy("personid").pivot("Type").sum("value")

spark.sql("select * from measurement").printSchema()

spark.sql("select ").distinct().show()



spark.sql("select * from demographics").printSchema()

enc = spark.sql("""
select personid, encounterid, reasonforvisit.standard.primaryDisplay as reasonforvisit, 
    financialclass.standard.primarydisplay payer, admissionsource.standard.primaryDisplay admissionsource,
    hospitalizationstartdate, dischargedate, tenant,
    aggregate(array_distinct(races.standard.primaryDisplay), '', (u, w) -> concat_ws(';',u,w)) race,
    status.standard.primaryDisplay status,
    ---coalesce()
    ---calculate_age
    
    --- patient level data
    aggregate(array_distinct(races.standard.primaryDisplay), '', (u, w) -> concat_ws(';',u,w)) race,
    aggregate(array_distinct(ethnicities.standard.primaryDisplay), '', (u,w) -> concat_ws(';',u,w)) ethnicity  
from encounter enc
---where to keep only encounters for which age at encounter is < 18 years
inner join demographics dem
on enc.personid = dem.personid
""")
enc.cache()

# Exclusion criteria: LOS should > 1 hour
# Include only tenants where average is less 18 ---
# start an hour after hospitalization
# admissiontype vs admissionsource

# we may need to explore status ---spark.sql("select distinct status from encounter order by 1").show(n = 100, truncate = 0)

### encounter.locations

spark.sql("select * from encounter").printSchema()

# get a list of encounter classes; confirm "Inpatient" as hospitalizations
spark.sql("""
select classification.standard.primaryDisplay, count(distinct encounterid)
from encounter
group by 1
order 2 desc
""").show(n=100, truncate=0)









spark.sql("""select active, count(distinct personid) from demographics group by 1 """).show(truncate =0)

spark.sql("""select distinct conditioncode.standard.id, conditioncode.standard.primaryDisplay, COUNT(DISTINCT ENCOUNTERID)
from condition
where conditioncode.standard.codingSystemId = '2.16.840.1.113883.6.90' and
 conditioncode.standard.id rlike '^U07.[1-2]|^U49|^U00'
GROUP BY 1, 2
order by 3 DESC
""").show(n = 1000, truncate =0)





cvLabs1 = spark.sql("""
SELECT distinct labcode.standard.primaryDisplay lab, labcode.standard.id, 
    INTERPRETATION.STANDARD.PRIMARYDISPLAY interpretation, TYPEDVALUE.TEXTVALUE.VALUE labvalue
FROM LAB
WHERE labcode.standard.id in ('94509-7','96121-9','94758-0','95823-1','94765-5','94315-9','96122-7','94313-4','94310-0','94502-2','94647-5','94532-9','95942-9','97099-6','95941-1','95380-2','95423-0','95422-2')

""")
cvLabs1.cache()



cvLabs2 = spark.sql("""
SELECT distinct labcode.standard.primaryDisplay lab, labcode.standard.id, 
    INTERPRETATION.STANDARD.PRIMARYDISPLAY interpretation, TYPEDVALUE.TEXTVALUE.VALUE labvalue
FROM LAB
WHERE labcode.standard.id in ('98733-9','99771-8','98846-9','98847-7','99774-2','99773-4','95209-3','94763-0','94661-6','95825-6','98069-8','94762-2','95542-7','94769-7','96118-5','94504-8','94503-0','94558-4','96119-3','97097-0','96094-8','98080-5','96896-6','96764-6','96763-8','94562-6','94768-9','95427-1','94720-0','95125-1','96742-2','94761-4','94563-4','94507-1','95429-7','94505-5','94547-7','95416-4','94564-2','94508-9','95428-9','94506-3','96895-8','100157-7','96957-6','95521-1','96898-2','94510-5','94311-8','94312-6','95522-9','94760-6','96986-5','95409-9','94533-7','94756-4','94757-2','95425-5','96448-6','96958-4','94766-3','94316-7','94307-6','94308-4','99596-9','95411-5','95410-7','97098-8','98132-4','98494-8','96899-0','94644-2','94511-3','94559-2','95824-9','94639-2','97104-4','98131-6','98493-0','94646-7','94645-9','96120-1','94534-5','96091-4','94314-2','96123-5','99314-7','94745-7','94746-5','94819-0','94565-9','94759-8','95406-5','96797-6','95608-6','94500-6','95424-8','94845-5','94822-4','94660-8','94309-2','96829-7','96897-4','94531-1','95826-4','94306-8','96900-6','94642-6','94643-4','94640-0','95609-4','96765-3','94767-1','94641-8','96752-1','96751-3','99597-7','96603-6','98732-1','98734-7','96894-1','95970-0','100156-9','96741-4','96755-4','94764-8','99772-6','95971-8','95974-2','95972-6','95973-4')

""")
cvLabs2.cache()



cvLabs1.count()

cvLabs1.orderBy('id').show(truncate=0,n = 100)

cvLabs2.count()

cvLabs2.orderBy('id').show(truncate=0,n = 1200)

covidlabs = spark.sql("""
SELECT PERSONID, ENCOUNTERID, SERVICEDATE,
    max(case when 
        lower(INTERPRETATION.STANDARD.PRIMARYDISPLAY) rlike 'above|abnormal|pos' or
        lower(TYPEDVALUE.TEXTVALUE.VALUE) rlike 'pos|^detected'
        then 1 else 0
    end) COVID_Positive

FROM LAB
WHERE labcode.standard.id
group by 1, 2, 3
""")
covidlabs.cache()







spark.sql("select count(distinct encounterid) from encounter").show(truncate =0)

spark.sql("select count(distinct personid) from demographics").show(100,0)

ab = spark.sql(""" 
select dem.personid, encounterid, classification.standard.primaryDisplay, 
    case when datediff(servicedate,birthdate)/365 < 18 then 1 else 0 end isPediatric
from demographics dem inner join encounter enc
on dem.personid = enc.personid
""")
ab.cache()

ab.createOrReplaceTempView('abTable')

spark.sql("""
select isPediatric, count(distinct personid) nPat, count(distinct encounterid) nEncs
from abTable
group by 1
order by 1

""").show(truncate =0,n = 1000)


spark.sql("""
select case
    when primaryDisplay rlike '^Outpatient|clinic' then 'Outpatient'
    when primarydisplay rlike '^Inpatient' then 'Inpatient'
    when primaryDisplay rlike '^Emergency' then 'Emergency'
end encounterType, isPediatric, count(distinct personid) nPat, count(distinct encounterid) nEncs
from abTable
group by 1, 2
order by 2,1

""").show(truncate =0,n = 1000)

spark.sql("select count(distinct personid), count(distinct encounterid) from abtable").show(truncate =0)



spark.sql("""
select e.encountertype,
   count(distinct e.tenant) as tenants,
   count(distinct e.personid) as patients,
   count(distinct e.encounterid) as encounters
from covid_2021_q3.encounter e,
(select personid, min(servicedate) as first_qual_dt from covid_2021_q3.encounter 
where age_at_encounter < 18 and (dx_qual_ind = 1 or lab_qual_ind = 1) 
group by personid) peds
where e.personid = peds.personid
and e.servicedate > peds.first_qual_dt
group by e.encountertype
order by encounters DESC
""").show(truncate = 0)


spark.sql("""
select e.encountertype,
   count(distinct e.tenant) as tenants,
   count(distinct e.personid) as patients,
   count(distinct e.encounterid) as encounters
from covid_2021_q3.encounter e

where (dx_qual_ind = 1 or lab_qual_ind = 1) 
group by 1
order by 1
""").show(truncate = 0)


spark.sql("show databases").show(truncate =0, n = 1000)

spark.sql("""use covid_2021_q3           """)

spark.sql("show tables").show(100, 0)

spark.sql("""
select count(distinct personid)
from condition
where conditioncode.standard.id rlike '^D57'

""").show()

covLabs = spark.sql("""
SELECT DISTINCT LABCODE.STANDARD.PRIMARYDISPLAY COVIDLABS 
FROM lab
WHERE LOWER(labcode.standard.primaryDisplay) RLIKE 'sars coronavirus 2|sars-cov-2|sars cov|covid' AND 
 (lower(INTERPRETATION.STANDARD.PRIMARYDISPLAY) rlike 'above|abnormal|pos' or
        lower(TYPEDVALUE.TEXTVALUE.VALUE) rlike 'pos|^detected')
ORDER BY 1
""")
covLabs.cache()

covLabs.show(n = 1000, truncate = 0)

spark.sql("""
select distinct classification.standard.primaryDisplay
from condition
order by 1
""").show(n = 1000, truncate = 0)

spark.sql("""
select type.standard.primaryDisplay, count(distinct encounterid)
from encounter
group by 1
order by 2 desc
""").show(n = 1000, truncate = 0)

spark.sql("""
select col.classification.standard.primaryDisplay encounterClass2, count(distinct encounterid)
from 
(
select encounterid, explode(encountertypes) from encounter 
) sq
group by 1
order by 2 desc
""").show(n = 1000, truncate = 0)

spark.sql("""
select distinct labcode.standard.primarydisplay, labcode.standard.id
from lab
where lower(labcode.standard.primarydisplay) rlike 'adenovirus|coronavirus|metapneumovirus|rhinovirus|enterovirus|influenza|syncytial'
order by 2
""").show(n = 1000, truncate =0)

spark.sql("select * from lab").printSchema()

spark.sql("select distinct route.standard.primaryDisplay route, route.standard.id from medication").show(truncate =0, n=1000)

spark.sql("""
select isPediatric, count(distinct personid)
from
(
select personid, case when year(birthdate) < 2003 then 1 else 0 end isPediatric
from demographics
) sq
group by 1

""").show(truncate = 0)

spark.sql("""select * from demographics""").printSchema()



drugname rlike 'magnesium' and drugname rlike ''

spark.sql("""
select distinct drugcode.standard.id, drugcode.standard.primaryDisplay
from medication where lower(drugcode.standard.primarydisplay) rlike 'magnesium' and 
    lower(drugcode.standard.primarydisplay) not rlike 'oxide|alumin|acetaminophen|lidocaine|bisacodyl|salicylate|esomeprazole|omeprazole|naproxen|arginine'
order by 1, 2
""").show(n = 1000, truncate = 0)

spark.sql("select * from measurement").printSchema()

spark.sql("""
select distinct measurementcode.standard.id, measurementcode.standard.primarydisplay
from measurement 
where lower(measurementcode.standard.primarydisplay) rlike 'weight'
order by 1,2
""").show(truncate = 0, n = 1000)

'29463-7','3141-9','3142-7','75292-3','8335-2'


spark.sql("""select distinct status from medication order by 1""").show(n = 100, truncate = 0)






vri = spark.sql("""
select * 
from lab
where lower(labcode.standard.primarydisplay) rlike 
    'coronavirus|metapneumovirus|rhinovirus|enterovirus|influenza|syncytial'

""")

spark.sql("""select distinct measurementcode.standard.id, measurementcode.standard.primarydisplay
from measurement
where lower(measurementcode.standard.primarydisplay) rlike 'milk' """).show(n =1000, truncate = 0)

spark.sql("show tables").show(n = 100, truncate =0)

spark.sql("""select distinct clinicaleventcode.standard.primaryDisplay, clinicaleventcode.standard.id from clinical_event
where lower(clinicaleventcode.standard.primaryDisplay) rlike 'inspired|fio2'
""").show(n=1000, truncate = 0)

spark.sql("""select distinct measurementcode.standard.primaryDisplay, measurementcode.standard.id from measurement
where lower(measurementcode.standard.primaryDisplay) rlike 'inspired|fio2'
""").show(n=1000, truncate = 0)



spark.sql("""select encounterid, drugcode.standard.primarydisplay drug, stopdate, startdate from medication
where lower(drugcode.standard.primarydisplay) rlike 'propofol' and lower(route.standard.primaryDisplay) rlike 'intravenous|iv|infusion' """).show(truncate = 0, n = 1000)

spark.sql("""select encounterid, drugcode.standard.primarydisplay drug, stopdate, startdate from medication
where lower(drugcode.standard.primarydisplay) rlike 'propofol' and lower(route.standard.primaryDisplay) rlike 'intravenous|iv|infusion' """).show(truncate = 0, n = 1000)

spark.sql("""select * from medication_administration""").printSchema()

spark.sql("""select medications.drugCode.standard.primaryDisplay drugcode,
effectivetime.administeredperiod.startdate, effectivetime.administeredperiod.endDate
from medication_administration adm
inner join procedure proc
on adm.encounterid = proc.encounterid
where (procedurecode.standard.codingSystemId = '2.16.840.1.113883.6.12' and procedurecode.standard.id = '61510')
""").show(n = 1000, truncate = 0)

spark.sql("""select encounterid, drugreference.drugCode.standard.primarydisplay drug,
effectivetime.administeredperiod.startdate, effectivetime.administeredperiod.endDate
from medication_administration adm
inner join procedure proc
on adm.encounterid = proc.encounterid
where  lower(drugreference.drugCode.standard.primarydisplay) rlike 'propofol|fentanyl|sevoflurane|rocoronium|morphine' 
and (procedurecode.standard.codingSystemId = '2.16.840.1.113883.6.12' and procedurecode.standard.id = '61510')

""").show(n = 1000, truncate = 0)





# icd-10-cm: 2.16.840.1.113883.6.90
# icd-9-cm: 2.16.840.1.113883.6.2, 2.16.840.1.113883.6.103

spark.sql("""select * from condition""").printSchema()

spark.sql("""
select count(distinct personid) from 
condition
where (conditioncode.standard.codingSystemId = '2.16.840.1.113883.6.90' and conditioncode.standard.id rlike '^N[0-9]')
    or (conditioncode.standard.codingSystemId in ('2.16.840.1.113883.6.2', '2.16.840.1.113883.6.103') and 
    conditioncode.standard.id rlike '^5[8-9]|^6[0-2]')

""").show()

spark.sql("""
select case when serviceenddate = "" or serviceenddate is null then 1 else 0 end hasEndDate,
    count(distinct encounterid) nEncounters
from procedure
group by 1

""").show(truncate =0)

142047058  /(142047058  +13668122   )

spark.sql("select * from procedure").printSchema()



spark.sql("""
select distinct procedurecode.standard.primaryDisplay procedure, procedurecode.standard.id procedure_id
from procedure
where 
--HCPCS codes; procedurecode.standard.codingSystemId ='2.16.840.1.113883.6.14'
PROCEDURECODE.standard.id RLIKE '^5A05121|^5A0512C|^5A05221|^5A0522C|^5A15223|^5A1522F|^5A1522G|^5A1522H|^5A15A2F|^5A15A2G|^5A15A2H'
OR
--CPT4 codes; procedurecode.standard.codingSystemId = '2.16.840.1.113883.6.12'
PROCEDURECODE.standard.id RLIKE '^33969|^33966|^33948|^33947|^33946|^33986|^33985|^33984|^33965|^33964|^33963|^33962|^33959|^33958|^33954|^33953|^33952|^33951|^33949'
or
--HCPCS codes; procedurecode.standard.codingSystemId ='2.16.840.1.113883.6.14'
procedurecode.standard.id rlike '^5A15223|^5A1522F|^5A1522G|^5A1522H'
"""
).show(n = 1000, truncate =0)



spark.sql("""
select distinct measurementcode.standard.primaryDisplay ecmo, measurementcode.standard.id ce_id
from measurement
where lower(measurementcode.standard.primaryDisplay) rlike 'ecmo|extracorporeal'
order by 1
""").show(truncate = 0, n = 1000)





spark.sql("""
select count(distinct personid), count(distinct encounterid)
from condition
where conditioncode.standard.id rlike '^I63.6|^437.6'

""").show(truncate = 0, n = 1000)



ins = spark.sql("""
select financialclass.standard.primarydisplay financialclass, count(distinct encounterid) nEncounters
from encounter
group by 1
order by 2 desc
""")
ins.cache()

ins.show(n = 1000, truncate =0)



spark.sql("""
select distinct labcode.standard.primaryDisplay, labcode.standard.id
from lab
where labcode.standard.primaryDisplay rlike 'lomerular'
order by 2
""").show(truncate =0, n = 1000)

spark.sql("""select distinct interpretation.standard.primaryDisplay from lab
where labcode.standard.id in ('33914-3','444336003','48642-3','48643-1','50044-7','50210-4','50384-7','62238-1',
'69405-9','70969-1','76633-7','77147-7','88293-6','88294-4')""").show(truncate =0, n = 1000)

spark.sql("""select * from lab """).printSchema()

# spark.sql("""use real_world_data_2021_q3""")

x = spark.sql("""
select distinct referencerange.typedreferencehighvalue.numericvalue.value
from lab
where labcode.standard.id = '3094-0'
""")
x.cache()

x.show(n = 100, truncate = 0)