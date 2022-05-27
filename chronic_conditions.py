spark.sql("use real_world_data_dec_2021")

spark.sql("show tables").show()

spark.sql("select * from condition").printSchema()

spark.sql("select * from procedure").printSchema()

diag = spark.sql("""
SELECT PERSONID, CCODE
FROM
(
SELECT PERSONID,
    CASE
        WHEN CONDITIONCODE RLIKE '^A' THEN 'CCC_INFECTION'
        WHEN CONDITIONCODE RLIKE '^C' THEN 'CCC_NEOPLASM'
        WHEN CONDITIONCODE RLIKE '^D' THEN 'CCC_BLOOD'
        WHEN CONDITIONCODE RLIKE '^E' THEN 'CCC_ENDOCRINE'
        WHEN CONDITIONCODE RLIKE '^F' THEN 'CCC_MENTAL'
        WHEN CONDITIONCODE RLIKE '^G' THEN 'CCC_NERVOUS'
        WHEN CONDITIONCODE RLIKE '^H[0-5][0-9]' THEN 'CCC_EYE'
        WHEN CONDITIONCODE RLIKE '^H[6-9][0-9]' THEN 'CCC_EAR'
        WHEN CONDITIONCODE RLIKE '^I' THEN 'CCC_CIRCULATORY'
        WHEN CONDITIONCODE RLIKE '^J' THEN 'CCC_RESPIRATORY'
        WHEN CONDITIONCODE RLIKE '^K' THEN 'CCC_DIGESTIVE'
        WHEN CONDITIONCODE RLIKE '^L' THEN 'CCC_SKIN'
        WHEN CONDITIONCODE RLIKE '^M' THEN 'CCC_MUSCULOSKELETAL'
        WHEN CONDITIONCODE RLIKE '^N' THEN 'CCC_GENITOURINARY'
        WHEN CONDITIONCODE RLIKE '^P' THEN 'CCC_PERINATAL'
        WHEN CONDITIONCODE RLIKE '^Q' THEN 'CCC_CONGENITAL'
        WHEN CONDITIONCODE RLIKE '^R' THEN 'CCC_UNCLASSIFIED'
        WHEN CONDITIONCODE RLIKE '^S|^T' THEN 'CCC_INJURY'
        WHEN CONDITIONCODE RLIKE '^V|^W|^X|^Y' THEN 'CCC_EXTERNAL'

    END CCODE
FROM 
(
SELECT CON.PERSONID, regexp_replace(CONDITIONCODE.standard.id, '[^a-zA-Z0-9]+',"") CONDITIONCODE
FROM CONDITION CON
)
)
""")
diag.cache()

diag.printSchema()

from pyspark.sql.types import *
from pyspark.sql.functions import *

diag = diag.withColumn('value', lit(1))
# diag = diag.filter('CCODE not like "%error%"')
diag = diag.groupBy('PERSONID').pivot('CCODE').max("value").fillna(0)
diag.cache()

diag.columns

diag.select("personid", "null", "CCC_BLOOD", "CCC_CIRCULATORY", "CCC_RESPIRATORY", "CCC_INJURY", "CCC_PERINATAL").show(10)

diag_sums = diag.agg(*[sum(diag[c_name]) for c_name in diag.columns])

diag_sums

diag_sums.show()

diag_pd = diag_sums.toPandas()

diag_pd

diag_final = diag_pd.T

diag_final

diag_final.rename(index={'sum(PERSONID)':'personid','sum(null)':'null', 'sum(CCC_BLOOD)':'BLOOD', 
                         'sum(CCC_CIRCULATORY)':'CIRCULATORY', 'sum(CCC_CONGENITAL)':'CONGENITAL', 
                        'sum(CCC_DIGESTIVE)':'DIGESTIVE', 'sum(CCC_EAR)':'EAR', 'sum(CCC_ENDOCRINE)':'ENDOCRINE', 
                         'sum(CCC_EXTERNAL)':'EXTERNAL', 'sum(CCC_EYE)':'EYE', 'sum(CCC_GENITOURINARY)':'GENITOURINARY',
                        'sum(CCC_INFECTION)':'INFECTION', 'sum(CCC_INJURY)':'INJURY', 
                         'sum(CCC_MENTAL)':'MENTAL', 'sum(CCC_MUSCULOSKELETAL)':'MUSCULOSKELETAL', 
                         'sum(CCC_NEOPLASM)':'NEOPLASM', 'sum(CCC_NERVOUS)':'NERVOUS', 'sum(CCC_PERINATAL)':'PERINATAL', 
                         'sum(CCC_RESPIRATORY)':'RESPIRATORY', 'sum(CCC_SKIN)':'SKIN', 'sum(CCC_UNCLASSIFIED)':'UNCLASSIFIED'}, 
                  inplace=True)

diag_final

diag_final = diag_final[1:]

diag_final.index.name = 'Body Systems'
diag_final.reset_index(inplace=True)

diag_final

diag_final.rename(columns = {0:'Count'}, inplace = True)

diag_final

def divide(row):  
    return row['Count']/1000000

diag_final['count_red'] = diag_final.apply(lambda row: divide(row), axis=1)

diag_final

import matplotlib.pyplot as plt
import pandas as pd

plt.figure(figsize = (12,8))
plt.rcParams.update({'font.size':15})

plt.barh(diag_final['Body Systems'], diag_final['count_red'])
plt.xlabel('Number of Patients with Chronic Conditions in Millions')
plt.title("Barplot of Patient Counts of Chronic Conditions")

