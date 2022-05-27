# Histogram of Patient Ages

### Get Dataframe like in EDA file

spark.sql("use real_world_data_dec_2021")

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

from pyspark.sql.functions import col, max as max_

temp = enc.withColumn("hospitalizationstartdate", col("hospitalizationstartdate").cast("timestamp")).groupBy("personid").agg(max_("hospitalizationstartdate"))
temp.columns

# obtain only the most recent hospitalization date
enc = enc.join(temp, on='personid', how ='inner')

## calculating the age. Utilizes the dataframe instead of sql
from pyspark.sql.functions import *

# coneverts the birthdate from 0000-00-00 00:00:00 to 0000-00-00 and place it on birthday
enc = enc.withColumn("birthday",to_date("birthdate"))

# converts the birthday into age, this will have decimals but we only take the floor of the decimal so

# 17.8 = 17 not 18
enc = enc.withColumn('age', floor(datediff(current_date(), to_date(col('birthday'), 'yyyy/M/d'))/365.25))

enc = enc.where(enc.age < 18)

d2 = enc.where('encounterLocations rlike "ICU"').select('tenant').distinct()

# d2 = spark.sql("""
# select distinct tenant
# from 
# encdata
# where encounterLocations rlike 'ICU'

# """)

result = enc.join(d2, on='tenant', how ='inner')


import matplotlib.pyplot as plt
import pandas as pd

res_pd = result.select('age').toPandas()

ax1 = res_pd['age'].plot(kind='hist', bins=25, facecolor='lightblue')

res_pd.min()
# there are negative ages

res_pd[res_pd['age'] < 0]
# 155 people recorded with negative ages

res_pd1 = res_pd[res_pd['age'] >= 0]

plt.figure(figsize = (12,8))
plt.rcParams.update({'font.size':15})
plt.hist(res_pd1['age'], bins=18, edgecolor='white', linewidth=1)
plt.xlabel('Age during Treatment')
plt.title("Histogram of Age during Treatment")



