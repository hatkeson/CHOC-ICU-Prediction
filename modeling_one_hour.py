# Read data from file created as part of setup

spark.sql("use real_world_data_dec_2021")

cohort_encounters = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/encounters")
cohort_encounters.cache()

cohort_patient = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/patients")
cohort_patient.cache()

demo = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/demographics")
demo.cache()

# Assumption -- keep only patients with a single unique birth date (less than 18 years as at 2019-01-01)
bday = spark.sql("""select personid, min(to_timestamp(birthdate) ) birthdate
from demographics
where datediff(to_timestamp('2019-01-01 00:00:01'), to_timestamp(birthdate))/365.25 < 18
group by personid
having count(distinct birthdate) = 1

""")
bday.cache()

demo = demo.join(bday, on = 'personid', how='inner')
demo.printSchema()

from pyspark.sql.functions import *

demo = demo.withColumn("birthday",to_date("birthdate"))
demo = demo.withColumn('age', floor(datediff(current_date(), to_date(col('birthday'), 'yyyy/M/d'))/365.25))
demo = demo.drop("birthday")
demo = demo.drop("birthdate")
demo.printSchema()

from pyspark.sql.functions import *

rr = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/respiratory_rate")
rr.cache()

hr = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/heart_rate")
hr.cache()

dbp = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/diastolic_blood_pressure")
dbp.cache()

sbp = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/systolic_blood_pressure")
sbp.cache()

spo2 = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/oxygen_saturation")
spo2.cache()

temp = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/vitals/temperature")
temp = temp.withColumnRenamed("value", "temperature")
temp.cache()

temp.columns

from pyspark.sql.functions import isnan, when, count, col
temp_null = temp.select([count(when(col(c).isNull(), c)).alias(c) for c in temp.columns])

temp_null.show()

temp.show(10)

spo2.show(5)

sbp.show(5)

dbp.show(5)

hr.show(5)

rr.show(5)

loc = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/locations")
loc.cache()

cohort_encounters.createOrReplaceTempView('cohort_encounter') # these are all encounters for our study
cohort_patient.createOrReplaceTempView('cohort_patient')

spo2.show(5)

vitals = rr.join(hr, on=['personid', 'encounterid', 'hour_mark'], how="outer") \
    .join(dbp, on=['personid', 'encounterid', 'hour_mark'], how="outer") \
    .join(sbp, on=['personid', 'encounterid', 'hour_mark'], how="outer") \
    .join(temp, on=['personid', 'encounterid', 'hour_mark'], how="outer") \
    .join(spo2, on=['personid', 'encounterid', 'hour_mark'], how="outer")

# new approach 

vital_coh = temp.select('personid', 'encounterid', 'hour_mark')\
    .union(hr.select('personid', 'encounterid', 'hour_mark'))\
    .union(rr.select('personid', 'encounterid', 'hour_mark'))\
    .union(sbp.select('personid', 'encounterid', 'hour_mark'))\
    .union(dbp.select('personid', 'encounterid', 'hour_mark'))\
    .union(spo2.select('personid', 'encounterid', 'hour_mark')) #pulse ox, oxygen saturation

vitals = vital_coh.join(temp, on= ['personid', 'encounterid', 'hour_mark'], how = 'left_outer')\
        .join(hr, on=['personid', 'encounterid', 'hour_mark'], how="left_outer")\
        .join(dbp, on=['personid', 'encounterid', 'hour_mark'], how="left_outer") \
        .join(sbp, on=['personid', 'encounterid', 'hour_mark'], how="left_outer") \
        .join(rr, on=['personid', 'encounterid', 'hour_mark'], how="left_outer") \
        .join(spo2, on=['personid', 'encounterid', 'hour_mark'], how="left_outer")
        

## to accommodate for person with missing values
vitals2 = vitals1.join(rr, on=['personid', 'encounterid', 'hour_mark'], how="left") \
    .join(dbp, on=['personid', 'encounterid', 'hour_mark'], how="left") \
    .join(sbp, on=['personid', 'encounterid', 'hour_mark'], how="left") \
    .join(temp, on=['personid', 'encounterid', 'hour_mark'], how="left") \
    .join(spo2, on=['personid', 'encounterid', 'hour_mark'], how="left") \
    .join(hr, on=['personid', 'encounterid', 'hour_mark'], how="left")
    
    

# take inner join of all vital signs --> only include patients with all three
vitals = rr.join(hr, on=['personid', 'encounterid', 'hour_mark'], how="inner") \
    .join(dbp, on=['personid', 'encounterid', 'hour_mark'], how="inner") \
    .join(sbp, on=['personid', 'encounterid', 'hour_mark'], how="inner") \
    .join(temp, on=['personid', 'encounterid', 'hour_mark'], how="inner") \
    .join(spo2, on=['personid', 'encounterid', 'hour_mark'], how="inner")

vitals.count()

vitals = vitals.withColumn("respiratory_rate", col("respiratory_rate").cast("int")) \
    .withColumn("heart_rate", col("heart_rate").cast("int")) \
    .withColumn("diastolic_blood_pressure", col("diastolic_blood_pressure").cast("int")) \
    .withColumn("systolic_blood_pressure", col("systolic_blood_pressure").cast("int")) \
    .withColumn("spo2", col("spo2").cast("int"))

vitals = vitals.withColumn("respiratory_rate", col("respiratory_rate").cast("int")) \
    .withColumn("heart_rate", col("heart_rate").cast("int")) \
    .withColumn("diastolic_blood_pressure", col("diastolic_blood_pressure").cast("int")) \
    .withColumn("systolic_blood_pressure", col("systolic_blood_pressure").cast("int")) \
    .withColumn("spo2", col("spo2").cast("int"))

vitals.printSchema()

vitals.count()

vitals.show(50)

# Encounter-immutable variables as single dataset
demographics and conditions

## Condition

cond = spark.read.parquet("s3://rwd-datalab-persistence-s3-data-scientist/CHOC/choccrwd1/HLA/data/chronic_conditions")

cond = cond.drop("null")
cond.printSchema()

## Join Demographics and Conditions on personid

patient_level = demo.join(cond, demo.personid == cond.PERSONID)
sqlContext.sql("set spark.sql.caseSensitive=true")
patient_level = patient_level.drop("PERSONID")
sqlContext.sql("set spark.sql.caseSensitive=false")
#patient_level = patient_level.withColumn('age', floor(datediff(current_date(), to_date(col('birthdate'), 'yyyy/M/d'))/365.25))
patient_level.cache()

patient_level.printSchema()

vitals.cache()

vitals.printSchema()

### ICU windows
start date and end date

loc.createOrReplaceTempView("loc")

icu = spark.sql("""
select distinct personid, encounterid, beginDate icu_start, endDate icu_end
from loc
where name rlike 'ICU'
""")

icu.show(10)

icu.printSchema()

icu = icu.withColumn("icu_start",to_timestamp("icu_start")) 
icu = icu.withColumn("icu_end",to_timestamp("icu_end")) 
icu.printSchema()

icu.show(10)

xx = vitals.join(icu, on=['personid', 'encounterid'], how = 'left_outer')
xx.show(10)

xx.printSchema()

xx.count()

# personid, encounterid, v1, v2, ..., v5, hour_mark,              icu_start,            icu_end
# personid, encounterid, v1, v2, ..., v5, '2022-04-28 11:00:00', '2022-04-24 09:00:00', icu_end1, 
# personid, encounterid, v1, v2, ..., v5, '2022-04-28 11:00:00', '2022-04-24 11:00:00', icu_end2 
# personid, encounterid, v1, v2, ..., v5, '2022-04-28 11:00:00', '2022-04-24 11:00:00', icu_end3

### logic error above: patients discharged from ICU to the general floor and then discharged home will be missed
# if hour_mark is greater than all icu_start, then keep only one record for that hour_mark and set outcome to zero

xx.createOrReplaceTempView('abc')

fin = spark.sql("""
select personid, encounterid, hour_mark, respiratory_rate, heart_rate, diastolic_blood_pressure, systolic_blood_pressure, 
temperature, spo2, icu_start, icu_end,
    case
        when icu_start is NULL and icu_end is NULL then 0
        when (unix_timestamp(icu_start) - unix_timestamp(hour_mark))/3600 <= 1 then 1  --'2022-04-28 11:00:00.0000'
        else 0
    end outcome_tx_ICU
from abc
where (unix_timestamp(icu_start) - unix_timestamp(hour_mark))/3600 > 0
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
""")

fin.groupBy("outcome_tx_ICU").count().show()

fin.show()

fin = fin.drop("icu_start").drop("icu_end")
fin.printSchema()

# join patient_level
fin = fin.join(patient_level, on="personid")

fin.printSchema()

fin.select("personid", "encounterid").show(10)

# use only the first hour that someone has been admitted
# group by person, select only oldest timestamp
# see if there's any people who visit the ICU

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
w2 = Window.partitionBy("personid").orderBy(col("hour_mark"))
fin_one_hour = fin.withColumn("row",row_number().over(w2)) \
  .filter(col("row") == 1).drop("row")

fin_one_hour.count()

fin_one_hour.groupBy("outcome_tx_ICU").count().show()

fin_one_hour_pd = fin_one_hour.toPandas()

fin_one_hour_pd = fin_one_hour_pd.dropna()

import seaborn as sns
from matplotlib import pyplot as plt

temp = fin_one_hour_pd[['respiratory_rate', 'heart_rate', 'diastolic_blood_pressure', 'systolic_blood_pressure', 'temperature', 'spo2','outcome_tx_ICU']]






temp[['respiratory_rate', 'heart_rate', 'diastolic_blood_pressure', 
      'systolic_blood_pressure', 'temperature', 'spo2']] = temp[['respiratory_rate', 'heart_rate', 
                                                                 'diastolic_blood_pressure', 
                                                                 'systolic_blood_pressure', 
                                                                 'temperature', 
                                                                 'spo2']].astype(float)

# removing exteremely unlikely cases of data since it skews most of the data 
q_low = temp["heart_rate"].quantile(0.01) 
q_hi  = temp["heart_rate"].quantile(0.99)

temp = temp[(temp["heart_rate"] < q_hi) & (temp["heart_rate"] > q_low)]


q_low = temp["respiratory_rate"].quantile(0.01) 
q_hi  = temp["respiratory_rate"].quantile(0.99)

temp = temp[(temp["respiratory_rate"] < q_hi) & (temp["respiratory_rate"] > q_low)]


q_low = temp["diastolic_blood_pressure"].quantile(0.01) 
q_hi  = temp["diastolic_blood_pressure"].quantile(0.99)

temp = temp[(temp["diastolic_blood_pressure"] < q_hi) & (temp["diastolic_blood_pressure"] > q_low)]

q_low = temp["systolic_blood_pressure"].quantile(0.01) 
q_hi  = temp["systolic_blood_pressure"].quantile(0.99)

temp = temp[(temp["systolic_blood_pressure"] < q_hi) & (temp["systolic_blood_pressure"] > q_low)]

q_low = temp["temperature"].quantile(0.01) 
q_hi  = temp["temperature"].quantile(0.99)

temp = temp[(temp["temperature"] < q_hi) & (temp["temperature"] > q_low)]


q_low = temp["spo2"].quantile(0.01) 
q_hi  = temp["spo2"].quantile(0.99)

temp = temp[(temp["spo2"] < q_hi) & (temp["spo2"] > q_low)]


temp_melt = temp.melt(id_vars=['outcome_tx_ICU'], value_vars=['respiratory_rate', 'heart_rate', 'diastolic_blood_pressure', 
      'systolic_blood_pressure', 'temperature', 'spo2'],
        var_name='Vitals', value_name='Vitals Values')

sns.set(font_scale = 1.1)
plt.rcParams["figure.figsize"] = [7.00, 7.00]
plt.xlabel('Vitals Values', fontsize = 20)
plt.ylabel('Vitals', fontsize = 20)
sns.boxplot(y= 'Vitals', x='Vitals Values', data = temp_melt, hue = 'outcome_tx_ICU')


## cleaning the original data set

fin_one_hour_pd[['respiratory_rate', 'heart_rate', 'diastolic_blood_pressure', 
      'systolic_blood_pressure', 'temperature', 'spo2']] = fin_one_hour_pd[['respiratory_rate', 'heart_rate', 
                                                                 'diastolic_blood_pressure', 
                                                                 'systolic_blood_pressure', 
                                                                 'temperature', 
                                                                 'spo2']].astype(float)


q_low = fin_one_hour_pd["heart_rate"].quantile(0.01) 
q_hi  = fin_one_hour_pd["heart_rate"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["heart_rate"] < q_hi) & (fin_one_hour_pd["heart_rate"] > q_low)]


q_low = fin_one_hour_pd["respiratory_rate"].quantile(0.01) 
q_hi  = fin_one_hour_pd["respiratory_rate"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["respiratory_rate"] < q_hi) & (fin_one_hour_pd["respiratory_rate"] > q_low)]


q_low = fin_one_hour_pd["diastolic_blood_pressure"].quantile(0.01) 
q_hi  = fin_one_hour_pd["diastolic_blood_pressure"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["diastolic_blood_pressure"] < q_hi) & (fin_one_hour_pd["diastolic_blood_pressure"] > q_low)]

q_low = fin_one_hour_pd["systolic_blood_pressure"].quantile(0.01) 
q_hi  = fin_one_hour_pd["systolic_blood_pressure"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["systolic_blood_pressure"] < q_hi) & (fin_one_hour_pd["systolic_blood_pressure"] > q_low)]

q_low = fin_one_hour_pd["temperature"].quantile(0.01) 
q_hi  = fin_one_hour_pd["temperature"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["temperature"] < q_hi) & (fin_one_hour_pd["temperature"] > q_low)]


q_low = fin_one_hour_pd["spo2"].quantile(0.01) 
q_hi  = fin_one_hour_pd["spo2"].quantile(0.99)

fin_one_hour_pd = fin_one_hour_pd[(fin_one_hour_pd["spo2"] < q_hi) & (fin_one_hour_pd["spo2"] > q_low)]

fin_one_hour.groupBy("outcome_tx_ICU").count().show()





fin_one_hour_pd = fin_one_hour_pd.drop(['temperature'], axis=1)

fin_one_hour_pd['gender'] = fin_one_hour_pd.gender.astype('category')
fin_one_hour_pd['race'] = fin_one_hour_pd.race.astype('category')
fin_one_hour_pd['ethnicity'] = fin_one_hour_pd.ethnicity.astype('category')
fin_one_hour_pd['outcome_tx_ICU'] = fin_one_hour_pd.outcome_tx_ICU.astype('category')

fin_one_hour_pd

fin_one_hour_pd.dtypes

fin_one_hour_pd = fin_one_hour_pd.dropna()

fin_one_hour_pd.isnull().sum(axis = 0)

fin_one_hour_pd['outcome_tx_ICU'].value_counts()

cols = ['age',
        'respiratory_rate','heart_rate','diastolic_blood_pressure',
        'systolic_blood_pressure','temperature','spo2','CCC_BLOOD',
       'CCC_CIRCULATORY', 'CCC_CONGENITAL', 'CCC_DIGESTIVE', 'CCC_EAR',
       'CCC_ENDOCRINE', 'CCC_EXTERNAL', 'CCC_EYE', 'CCC_GENITOURINARY',
       'CCC_INFECTION', 'CCC_INJURY', 'CCC_MENTAL', 'CCC_MUSCULOSKELETAL',
       'CCC_NEOPLASM', 'CCC_NERVOUS', 'CCC_PERINATAL', 'CCC_RESPIRATORY',
       'CCC_SKIN', 'CCC_UNCLASSIFIED']

import pandas as pd
import numpy as np
from sklearn import preprocessing
import matplotlib.pyplot as plt 
plt.rc("font", size=14)
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import seaborn as sns
sns.set(style="white")
sns.set(style="whitegrid", color_codes=True)

X = fin_one_hour_pd[cols]
y = fin_one_hour_pd['outcome_tx_ICU']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

clf = LogisticRegression(random_state=0).fit(X_train, y_train)



clf.score(X_test, y_test)

# roc curve and auc
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from matplotlib import pyplot
# generate 2 class dataset

# generate a no skill prediction (majority class)
ns_probs = [0 for _ in range(len(y_test))]
# fit a model
model = LogisticRegression(solver='lbfgs')
model.fit(X_train, y_train)
# predict probabilities
lr_probs = model.predict_proba(X_test)
# keep probabilities for the positive outcome only
lr_probs = lr_probs[:, 1]
# calculate scores
ns_auc = roc_auc_score(y_test, ns_probs)
lr_auc = roc_auc_score(y_test, lr_probs)
# summarize scores
print('No Skill: ROC AUC=%.3f' % (ns_auc))
print('Logistic: ROC AUC=%.3f' % (lr_auc))
# calculate roc curves
ns_fpr, ns_tpr, _ = roc_curve(y_test, ns_probs)
lr_fpr, lr_tpr, _ = roc_curve(y_test, lr_probs)
# plot the roc curve for the model
pyplot.plot(ns_fpr, ns_tpr, linestyle='--', label='No Skill')
pyplot.plot(lr_fpr, lr_tpr, marker='.', label='Logistic')
# axis labels
pyplot.xlabel('False Positive Rate')
pyplot.ylabel('True Positive Rate')
# show the legend
pyplot.legend()
# show the plot
pyplot.show()

# Gradient boosting method
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
import seaborn as sns
from matplotlib import pyplot
from sklearn.datasets import make_classification
from sklearn.metrics import classification_report, confusion_matrix, roc_curve, auc


cols = ['age',
        'respiratory_rate','heart_rate','diastolic_blood_pressure',
        'systolic_blood_pressure','spo2','CCC_BLOOD',
       'CCC_CIRCULATORY', 'CCC_CONGENITAL', 'CCC_DIGESTIVE', 'CCC_EAR',
       'CCC_ENDOCRINE', 'CCC_EXTERNAL', 'CCC_EYE', 'CCC_GENITOURINARY',
       'CCC_INFECTION', 'CCC_INJURY', 'CCC_MENTAL', 'CCC_MUSCULOSKELETAL',
       'CCC_NEOPLASM', 'CCC_NERVOUS', 'CCC_PERINATAL', 'CCC_RESPIRATORY',
       'CCC_SKIN', 'CCC_UNCLASSIFIED']

X = fin_one_hour_pd[cols]
y = fin_one_hour_pd['outcome_tx_ICU']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

clf = GradientBoostingClassifier(n_estimators = 1000, learning_rate= .01, 
                                 max_depth = 6, random_state=0).fit(X_train, y_train)

clf.score(X_test, y_test)

learning_rates = [0.05, 0.1, 0.25, 0.5, 0.75, 1]
for learning_rate in learning_rates:
    clf = GradientBoostingClassifier(n_estimators = 1000, learning_rate = learning_rate, max_features = 2, max_depth = 3, random_state = 0)
    clf.fit(X_train, y_train)
    print("Learning rate: ", learning_rate)
    print("Accuracy score (training): {0:.3f}".format(clf.score(X_train, y_train)))
    print("Accuracy score (validation): {0:.3f}".format(clf.score(X_test, y_test)))
    print()
    
    
    
    

from sklearn.model_selection import GridSearchCV
# hyper paramter tuning for gradient boosting
parameters = {
 "loss":["deviance"],
 "learning_rate": [.001, 0.01, .05, .1, 1, ],
 "max_depth":[2, 3, 5, 8, 10],
 "max_features":["log2","sqrt"],
 "criterion": ["friedman_mse"],
 "subsample":[1],
 "n_estimators":[ 100, 200, 300, 500, 1000, 2000, 3000]
 }
clf1 = GridSearchCV(GradientBoostingClassifier(), parameters, n_jobs = -1)
clf1.fit(X_train, y_train)
print(clf1.score(X_train, y_train))
print(clf1.best_params_) 


predictions = clf1.predict(X_test)
print("Confusion Matrix:")
print(confusion_matrix(y_test, predictions))
print()
print("Classification Report")
print(classification_report(y_test, predictions))

from sklearn import metrics
from matplotlib import pyplot as plt
y_pred_proba = clf1.predict_proba(X_test)[::,1] # get the probability of prediction

print(y_pred_proba)
fpr, tpr, _ = metrics.roc_curve(y_test, y_pred_proba) # find the false positive, true positive rates
auc = metrics.roc_auc_score(y_test, y_pred_proba)
plt.plot(fpr,tpr,label="AUC="+str(auc))
plt.title("Auc Curve of Gradient boosting method")
plt.ylabel('True Positive Rate')
plt.xlabel('False Positive Rate')
plt.legend(loc=4)
plt.show()

# metrics

clf = GradientBoostingClassifier(n_estimators=20, learning_rate = 0.5, max_features = 2, max_depth = 2, random_state = 0)
predictions = clf.predict(X_test)

print("Confusion Matrix:")
print(confusion_matrix(y_test, predictions))
print()
print("Classification Report")
print(classification_report(y_test, predictions))



import statsmodels.api as sm
log_reg = sm.Logit(y, X).fit()

#pyspark.ml.classification.logisticregression for Prof Minin's model

log_reg.summary()

log_reg.pred_table(0.3)
# precision = 0.0379
# recall = 