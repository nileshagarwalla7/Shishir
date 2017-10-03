from pyspark import SparkConf,SparkContext
import time
import sys
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
import datetime
from pyspark.sql import functions as F
import pandas as pd
from time import gmtime, strftime
import logging
import argparse
from pytz import timezone
import pytz
from datetime import datetime

### creating spark context
conf = SparkConf()
conf.setAppName('alpha-code')
sc = SparkContext(conf=conf)


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

def get_ist_date(utc_dt):
    import pytz
    from datetime import datetime
    local_tz = pytz.timezone('Asia/Kolkata')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)

def get_pst_date():
	from datetime import datetime
	dt = datetime.utcnow()
	#dt=datetime.now(tz=pytz.utc)
	###(dt, micro) =  datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific')).strftime('%Y-%m-%d %H:%M:%S.%f').split('.')
	###dt = "%s.%03d" % (dt, int(micro) / 1000)
	return dt
	

def lower_locale(locale):
	return locale.lower()

lower_locale_udf = udf(lower_locale,StringType())

AlphaStartDate = get_pst_date()
print("AlphaStartDate = ",AlphaStartDate)

### sql server info for writing log table 
properties = {"user" : "occuser" , "password":"Exp3dia22" , "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
url = "jdbc:sqlserver://10.23.18.135"


### defining log id initiation
rep = 720

### function to append logs to central log table

def log_df_update(spark,IsComplete,Status,EndDate,ErrorMessage,RowCounts,StartDate,FilePath,tablename):
	import pandas as pd
	l = [(process_name_log.value,process_id_log.value,IsComplete,Status,StartDate,EndDate,ErrorMessage,int(RowCounts),FilePath)]
	schema = (StructType([StructField("SourceName", StringType(), True),StructField("SourceID", IntegerType(), True),StructField("IsComplete", IntegerType(), True),StructField("Status", StringType(), True),StructField("StartDate", TimestampType(), True),StructField("EndDate", TimestampType(), True),StructField("ErrorMessage", StringType(), True),StructField("RowCounts", IntegerType(), True),StructField("FilePath", StringType(), True)]))
	rdd_l = sc.parallelize(l)
	log_df = spark.createDataFrame(rdd_l,schema)
	#  "Orchestration.dbo.AlphaProcessDetailsLog"
	log_df.withColumn("StartDate",from_utc_timestamp(log_df.StartDate,"PST")).withColumn("EndDate",from_utc_timestamp(log_df.EndDate,"PST")).write.jdbc(url=url, table=tablename,mode="append", properties=properties)



	

### defining parameters for a campaign 
StartDate = get_pst_date()

try :
	
	
	parser = argparse.ArgumentParser()
	parser.add_argument("--launch_date", help="Write launch_date like Y-M-D")
	parser.add_argument("--locale_name", help="Write locale_name like en_nz")
	parser.add_argument("--sqlTraveler", help="Write True or False")
	parser.add_argument("--data_environ", help="Write dev or prod or test")
	parser.add_argument("--file_name_s3", help="Write file_name_se like s3://")

	args = parser.parse_args()
	LaunchDate = args.launch_date
	locale_name = args.locale_name
	sqlTraveler = args.sqlTraveler
	data_environ = args.data_environ
	file_name_s3 = args.file_name_s3
	pos = locale_name.split('_')[1].upper()
	current_date =  time.strftime("%Y/%m/%d")
	
	
	
	#LaunchDate = '2017-09-17'
	#locale_name = 'en_nz'
	#sqlTraveler = 'False'
	#data_environ = 'prod'
	#file_name_s3 = 'en_US_testing_20170829'
	#pos = locale_name.split('_')[1].upper()
	#current_date =  '2017/09/14'
	
	status_table = (sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://10.23.18.135").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable", "Orchestration.dbo.AlphaConfig").option("user", "occuser").option("password", "Exp3dia22").load())
	pos1 = locale_name.split('_')[1].upper() #Converting the country to uppercase
	pos0 = locale_name.split('_')[0]  #Obtaining the language code
	search_string = pos0+"_"+pos1 #storing locale in 'en_US' format
	required_row = status_table.filter(status_table.Locale == search_string).filter("brand like 'Brand Expedia'").collect() #finding out the rows that contain the brand Expedia only
	global process_id
	global process_name 
	process_id = required_row[0]['id']  #ID of the particular row
	process_name = required_row[0]['ProcessName'] #Process name for the filtered row

	process_id_log = sc.broadcast(process_id)
	process_name_log = sc.broadcast(process_name)
	
	
	log_df_update(sqlContext,1,'parameters are correct',get_pst_date(),' ','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	
except:
	log_df_update(sqlContext,0,'failed',get_pst_date(),'parameters are improper','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("parameters not present!!!")

print("LaunchDate = " + str(LaunchDate))



StartDate = get_pst_date() #pacific standard time

tableName = ['campaign_definition','template_definition','segment_module','module_variable_definition','segment_definition'
					,'module_definition']



#importing files from server
def importFiles(x):
	temp = (sqlContext.read.format("jdbc")
								 .option("url", "jdbc:sqlserver://10.23.18.135")
								 .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
								 .option("dbtable", "AlphaMVP.dbo."+x).option("user", "occuser")
								.option("password", "Exp3dia22").load())
	return temp
#writing the different tables in 'df'+tableName format
try :
	for name in tableName:
		data_framename = 'df'+(''.join([i.title() for i in name.split('.')[0].split('_')]))
		globals()[data_framename] = (importFiles("{}_{}".format(name,data_environ))).drop("id") 

	log_df_update(sqlContext,1,'config files are imported',get_pst_date(),' ','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	
except:
	log_df_update(sqlContext,0,'failed',get_pst_date(),'config files not present','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')  
	raise Exception("campaign meta data not present!!!")



### import traveler profile data
StartDate = get_pst_date()

'''
try :
	dfTraveler = (sqlContext.read.parquet("s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/email_campaign/merged_shop_phase_date_format/{}/{}/{}".format(pos,locale_name,current_date)).filter("mer_status = 1") .cache())
	log_df_update(1,'traveler data imported',get_pst_date(),'No Error',str(dfTraveler.count()),StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	
except:
	log_df_update(0,'failed',get_pst_date(),'current traveler data was not present','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	raise Exception("traveler data not present!!!")
'''

	#reading the tables
	
	
#gives information about the completion status, launch and end dates
central_log_table = (sqlContext.read.format("jdbc")
						 .option("url", "jdbc:sqlserver://10.23.18.135")
						 .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
						 .option("dbtable", "Orchestration.dbo.CentralLog").option("user", "occuser")
						.option("password", "Exp3dia22").load())

#gives details on cluster and job
etl_config_table = (sqlContext.read.format("jdbc")
						 .option("url", "jdbc:sqlserver://10.23.18.135")
						 .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
						 .option("dbtable", "Orchestration.dbo.ETLconfig").option("user", "occuser")
						.option("password", "Exp3dia22").load())


etl_config_table = etl_config_table.withColumnRenamed("ScheduleTimes ","ScheduleTimes")
etl_config_table = etl_config_table.withColumnRenamed("Job_Name ","Job_Name")
etl_config_table = etl_config_table.withColumnRenamed("ID ","ID")
filter_condition_etl_config = "Locale = " + "'" + locale_name + "'"

etl_config_table.filter(filter_condition_etl_config).filter(etl_config_table.IsFinal == 1).filter("Job_Name not like ' '").orderBy(desc('ScheduleTimes')).show(10,False)
traveler_source_lookup_row = etl_config_table.filter(filter_condition_etl_config).filter(etl_config_table.IsFinal == 1).filter("Job_Name not like ' '").orderBy(desc('ScheduleTimes')).collect()
print(traveler_source_lookup_row)
central_log_status_id = traveler_source_lookup_row[0]["ID"]
clt1 = central_log_table.filter(central_log_table.SourceID == central_log_status_id).filter(central_log_table.IsComplete == 1).filter(central_log_table.SourceName == "ETL").orderBy(desc('StartDate'))
#  clt1.show(2,False)
detailed_log_list = clt1.collect()[0] 
print (detailed_log_list)
log_id = detailed_log_list['LogID']
File_path = detailed_log_list['FilePath']
dfTraveler = (sqlContext.read.parquet(File_path).filter("mer_status = 1").withColumn("locale",lower_locale_udf("locale")).cache())

print(File_path)

print("------------imported Traveler Data")



### extracting pos,tpid and eapid information for filtering out MIS and config files accordingly
pos_info = (dfTraveler.select("tpid","eapid","locale")
					.filter("locale is not null and tpid is not null and eapid is not null").limit(1).rdd.first())

pos_filter_cond = "locale = '" + pos_info["locale"] + "' and tpid = " + str(pos_info["tpid"]) + " and eapid = " + str(pos_info["eapid"])

#print(pos_filter_cond)


StartDate = get_pst_date()
### populating values in all rows for var source column
module_var_def = (dfModuleVariableDefinition)

df2 = (module_var_def.select("module_id","source_connection","var_source")
			.filter("var_source != ''").filter("var_source is not null")
			.withColumnRenamed("var_source","var_source1"))   #shouldn't it be distinct?

df3 = module_var_def.join(df2,["module_id","source_connection"],'left')

dfModuleVariableDefinition = (df3.drop("var_source")
													.withColumnRenamed("var_source1","var_source").withColumn("locale",lower_locale_udf("locale"))
													 .filter(pos_filter_cond))


if (dfModuleVariableDefinition.count()) <= 0 :
	log_df_update(sqlContext,0,'populating var source in module var definition',get_pst_date(),'check filters data is not present','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')  
	raise Exception("check filters data is not present in module variable definition!!!")
else :
	log_df_update(sqlContext,1,'Populated var source in module var definition',get_pst_date()," ",str(dfModuleVariableDefinition.count()),StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')


StartDate = get_pst_date()
### creating meta campaign data by combining all the raw files 
dfMetaCampaignData1 = (dfCampaignDefinition
									 .filter("locale = '{}' and LaunchDate = '{}'".format(locale_name,LaunchDate)).withColumn("locale",lower_locale_udf("locale"))
									.filter(pos_filter_cond).filter("campaign_deleted_flag = 0"))

dfMetaCampaignData2 = (dfMetaCampaignData1.join(dfTemplateDefinition.filter("template_deleted_flag = 0"),'template_id','inner')
																		 .join(dfSegmentModule.filter("seg_mod_deleted_flag = 0"),'segment_module_map_id','inner')
																		 .join(dfModuleDefinition,['locale','module_type_id','tpid','eapid','placement_type','channel'],'inner')                                     
																		 .join(dfSegmentDefinition.filter("segment_deleted_flag = 0"),["tpid","eapid","segment_type_id"],'inner')
																		 .filter("status = 'active published'"))

#contains all the data that is required for meta campaign which will be joined with travelers later on
dfMetaCampaignData_31 = (dfMetaCampaignData2.select([c for c in dfMetaCampaignData2.columns if c not in 
																			 {"campaign_type_id","dayofweek","program_type",
																			"derived_module_id","context","language","lob_intent","status"}])
										.withColumn("locale",lower_locale_udf("locale"))
										.filter(pos_filter_cond))


if (dfMetaCampaignData_31.count()) <= 0 :
	condition_str = 'check campaign is present for locale {} and launch date {}'.format(locale_name,LaunchDate)
	log_df_update(sqlContext,0,'creating meta campaign data',get_pst_date(),condition_str,'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception(condition_str)
else :
	log_df_update(sqlContext,1,'meta campaign data creation',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')


print("------------created Meta_campaign_31")



dfTravelers = (dfTraveler.repartition(rep,"email_address").cache())

### Populating segment type id in travelers data:
dfMetaCampaign = (dfMetaCampaignData_31
							 .select("campaign_segment_type_id","campaign_id","priority","tpid","eapid","locale")
							 .distinct()
							)


print("------------Dftraveler done")
### importing traveler-segment mapped data
StartDate = get_pst_date()


seg_clt1 = central_log_table.filter(central_log_table.SourceID == process_id).filter(central_log_table.IsComplete == 1).filter(central_log_table.SourceName == "Segment_mapping").orderBy(desc('EndDate'))
seg_detailed_log_list = seg_clt1.collect()[0] 
print (seg_detailed_log_list)
seg_log_id = seg_detailed_log_list['LogID']
File_path_seg = seg_detailed_log_list['FilePath']


try :
	traveler = (sqlContext.read.
					parquet(File_path_seg)
					.repartition(rep))
	log_df_update(sqlContext,1,'traveler-segment data imported',get_pst_date(),' ',str(traveler.count()),StartDate,File_path_seg,'Orchestration.dbo.AlphaProcessDetailsLog')
	
except :
	log_df_update(sqlContext,0,'failed',get_pst_date(),'traveler-segment data not present','0',StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,File_path,'Orchestration.dbo.CentralLog')
	raise Exception("traveler-segment data not present!!!")


	

StartDate = get_pst_date()
### Joining traveler segment table with meta campaign to assign corresponding campaigns
dfTraveler_MetaCampaign = (traveler.withColumnRenamed("segment_type_id","campaign_segment_type_id")
											.join(dfMetaCampaign,["campaign_segment_type_id","tpid","eapid","locale"] ,'inner')
											.drop("segment_type_id")
											.withColumnRenamed("segment_type_id_concat","segment_type_id")
											.drop("")
											 .distinct())
#loggerGen.info("dfTraveler_MetaCampaign Read Successfully")

final_df_for_alpha = (dfTraveler_MetaCampaign
												.join(dfTravelers,['email_address',"test_keys","tpid","eapid","locale"],'inner')
												.repartition(rep)
												.distinct()
												.cache()
											)

if  (final_df_for_alpha.count())<=0 :
	check_str = 'check values of join keys namely segment_type_id, tpid,eapid and locale '
	log_df_update(sqlContext,0,'Joining traveler segment and traveler data',get_pst_date(),check_str,'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("Error in creating final_df_for_alpha")
else :
	log_df_update(sqlContext,1,'Joining traveler segment and traveler data',get_pst_date()," ",str(final_df_for_alpha.count()),StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')

	

### Creating module version mapping for week and day wise circulation of module ids
StartDate = get_pst_date()
df_module = (dfMetaCampaignData_31
					 .groupBy('module_type_id','placement_type')
					 .agg(countDistinct('version'))
					)

### For a combination of module_type_id and placement_type the version changes with change in module_id

col_grp = ['campaign_id','slot_position','module_type_id','placement_type'] #columns that are required to be selected in dfMetaCampaignData_4

#table containing number of versions available for module_type_id, placement_type and campaign_id
dfMetaCampaignData_4 = (dfMetaCampaignData_31
									.select(col_grp)
									.join(df_module,['module_type_id','placement_type'],'inner')
									.distinct()
									.withColumnRenamed('count(DISTINCT version)','ttl_versions')
									)

### dfMetaCampaignData_4  is created to find out the total number of versions available for a combination of module_type_id and placement_type

if  (dfMetaCampaignData_4.count())<=0 :
	log_df_update(sqlContext,0,'Dict_map function for module version allocation',get_pst_date(),'error in dict map function','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("Error in dict_map function")
else :
	log_df_update(sqlContext,1,'Dict_map function for module version allocation',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')

	

### Joining with dfModuleVariableDefinition to populate Module content

window_version = Window.partitionBy(col_grp).orderBy("version")
dfMetaCampaignData_VarDef = (dfMetaCampaignData_31
										.join(dfModuleVariableDefinition,["module_id","tpid","eapid","locale"],'left')
										.join(dfMetaCampaignData_4,col_grp,'inner')
										.withColumn("rank",dense_rank().over(window_version))
										.drop("version").withColumnRenamed("rank","version")
										.drop("locale"))

### dfMetaCampaignData_VarDef is the data on the campaigns expanded completely where the granular level data is the var related data

### creating data frame having info about MIS tables
mis_data_df = (dfMetaCampaignData_VarDef
						.filter("var_source is not null")
						.select("module_id","var_position","var_source","var_structure").distinct())


### function to extract joining key of MIS table
def extract_keys(var_source):
	temp1 = var_source.split("|")
	if len(temp1) > 1:
			 keys = "##".join([val.split(".")[1] for val in temp1[1].split(";") if val!=None])
	else : keys = ""
	return keys  #columns that are used to join table

extract_keys_udf = udf(extract_keys,StringType())

### function to extract required cols from MIS table
def extract_cols(var_struct):
	import re
	content_cols = []
	if (var_struct.find('%%')>=0):
			 z = [m.start() for m in re.finditer('%%', var_struct)]
			 j = 0 
			 while (j <= len(z)-2):
					temp = var_struct[z[j]+2:z[j+1]]  #storing the data that lies between '%%'
					temp_ls = temp.split('.')
					if temp_ls[0] != 'traveler':
						 content_cols.append(temp_ls[1]) 

					j+=2

	return "##".join(list(set(content_cols))) #columns that are required to extract data from (image,links,text etc.)

extract_cols_udf = udf(extract_cols,StringType())


### function to extract MIS table name
def file_name(var_source):
	join_key = var_source
	if( len(join_key.split('|')) > 1):
			 return join_key.split('|')[1].split(';')[0].split('.')[0]
	
file_name_udf = udf(file_name,StringType())


### populating MIS table name , joining keys and cols required using above functions. Contains data that include the keys and columns required at a module id and var position level for each file
mis_data_intermediate = (mis_data_df.withColumn("file_name",file_name_udf("var_source"))
								 .withColumn("keys",extract_keys_udf("var_source"))
								 .withColumn("mis_cols",extract_cols_udf("var_structure"))
								 .filter("file_name is not null and mis_cols!='' and keys!='' ")
								 .select("module_id","var_position","file_name","keys","mis_cols").distinct())
								 

### mis_data_intermediate is a table which contains file_name, keys, mis_cols
### Using the key in keys column the file_name will be joined to traveler data and the data from columns belonging to mis_cols will be fetched

mis_data_file_names = mis_data_intermediate.select("file_name").distinct().rdd.map(lambda x: str(x[0])).collect() #finding out the file names
mis_data_rdd =mis_data_intermediate.rdd.collect() #converting mis_data_intermediate to rdd


print("------------creating MIS RDD")

### function to read MIS table from sql server
mis_content_dict = {}
mis_data_rdd_dic = {}
def readAllMisFiles(file_names):
	for file_name in file_names:
			 file = (sqlContext.read.format("jdbc").option("url", "jdbc:sqlserver://10.23.18.135")
																.option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
																 .option("dbtable", "AlphaMVP.dbo."+file_name)
																 .option("user", "occuser")
																 .option("password", "Exp3dia22")
																 .load())
			 file.cache()
			 mis_data_rdd_dic[file_name]=file #prints the schema, behaves as a pointer. doesn't store it in memory until this dictionary is used somewhere


### mis_data_rdd_dic is a dictionary with key as the filename and value as the schema. It acts as a pointer to that file
### reading MIS data

StartDate = get_pst_date()

try:
	readAllMisFiles(mis_data_file_names)
	log_df_update(sqlContext,1,'MIS data imported',get_pst_date(),' ','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')

except:
	log_df_update(sqlContext,0,'failed',get_pst_date(),'MIS data is not available','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("MIS data not present!!!")

	

print("------------reading MIS data")
### creating dictionary of MIS data 
StartDate = get_pst_date()

mis_content_dict = {}
mis_not_present = []

for i in mis_data_rdd:          #goes along each row of mis_data_intermediate table(rdd version which is mis_data_rdd)
	try :
			 module_id = i["module_id"]
			 file_name = i["file_name"]
			 join_keys = [val for val in i["keys"].split("##") if val!='']
			 mis_cols = i["mis_cols"].split("##")
			 cols_req = ["key"] + mis_cols              #format of [key, col1, col2....]
			 content_df = ( mis_data_rdd_dic[file_name].withColumn("locale",lower_locale_udf("locale"))
									.filter(pos_filter_cond)
									.withColumn("key",concat(*join_keys))
								 .select(cols_req).toPandas())
			 content_df.index = content_df.key 
			 content_dict = content_df[mis_cols].to_dict() #content_df- {col1: {value of join key: value of col1}, col2: {value of join key: value of col2}}
			 mis_content_dict[str(module_id)+file_name+i["keys"].replace('##','')+str(i["var_position"])] = content_dict
			 #mis_content_dict- module_idfile_namekeysvar_position: { content_dict }
			 
	except :
			 file_name = i["file_name"]
			 module_id = i["module_id"]
			 mis_not_present.append(file_name)


if len(mis_not_present) >0 :
	file_name_str = "#".join(mis_not_present) + " MIS files are not present"
	log_df_update(sqlContext,0,'Required MIS data not present',get_pst_date(),file_name_str,'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("MIS data not present!!!")
else :
	log_df_update(sqlContext,1,'Required MIS Data is present',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')

	

### selecting the required columns from meta campaign data and creating dictionary
col_list = ['campaign_id','slot_position','var_structure','var_source','var_position','module_id','module_type_id'
				 ,'module_priority_in_slot','segment_type_id','version','ttl_versions']


dict_cpgn = sc.broadcast(dfMetaCampaignData_VarDef[dfMetaCampaignData_VarDef.var_position.isNotNull()]
										.withColumn('var_position',dfMetaCampaignData_VarDef.var_position.cast(StringType()))
									.select(col_list)
										.toPandas())

#dict_cpgn has a table with dfMetaCampaignData_VarDef with the var_position cast as string
#broadcast is done to avoid shuffling in small tables. Shuffling is expensive in spark

### creating dictionary mapping for number of variable position in each slot           
slot_position_map = (dfMetaCampaignData_VarDef
								 .groupBy('slot_position')
								 .agg({'var_position':'max'})
								 .withColumnRenamed('max(var_position)','var_position')
								 .rdd
								 .collectAsMap())


### getting number of slots and list of slots
slots = len(list(dict_cpgn.value.slot_position.unique()))
slot_list = (list(dict_cpgn.value.slot_position.unique()))
slot_list.sort()


StartDate = get_pst_date()

### creating data frame with unique combination of campaign id, test keys and segment type 
df_slot =(final_df_for_alpha
							.select('campaign_id','test_keys','segment_type_id')
							.distinct()
							.withColumn('number_slots',lit(slots))
							.repartition(rep)
							.cache())


if df_slot.count() <=0 :
	log_df_update(sqlContext,0,'df_slot creation',get_pst_date(),'check final_df_for_alpha ','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')

	raise Exception("Error in df_slot creation!!!")
else :
	log_df_update(sqlContext,1,'df_slot creation',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')

	

StartDate = get_pst_date()
### function to assign module ids to each unique combination created above
def module_allocation(campaign_id,test_keys,segment_type_id,number_slots):

	import pandas as pd
	import datetime
	num_slots = number_slots
	test_keys =  int(test_keys)
	map_dict1 = {}
	segment_type_id_ls = [int(i) for i in segment_type_id.split("#")] #Listing out all the segment types

	for i in slot_list: 
			 slot_position = i
			 cpgn_sub = dict_cpgn.value[(dict_cpgn.value.campaign_id == str(campaign_id)) &
													(dict_cpgn.value.slot_position == slot_position) ]

			 cpgn_sub_final = cpgn_sub[(cpgn_sub['segment_type_id'].isin(segment_type_id_ls))]  #finding out whether segment type id is present in the list of segment types

			 test = pd.DataFrame()

			 module_types = list(cpgn_sub_final.module_type_id.unique()) #unique module types

			 for module in module_types:

					ttl_versions = int(cpgn_sub_final[(cpgn_sub_final.module_type_id == module)].iloc[0].ttl_versions) #one of the version
					seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday() #used to rotate these versions on a daily basis
					version_num = int((int(test_keys)+ seed%(ttl_versions))%(ttl_versions)) + 1
					test = test.append(cpgn_sub_final[(cpgn_sub_final.version==version_num) & (cpgn_sub_final.module_type_id == module)])  #appended to the test dataframe


			 col_req = ['module_type_id','module_priority_in_slot']

			 if len(test.index) == 0:
					final_dict = {"dummy#dummy":"dummy"}   #create a dummy dictionary if the particular slot has nothing to be filled with

			 else:  
					test.index = (test[col_req]
										 .astype(str)
										 .apply(lambda x : '#'.join(x), axis=1))

					test['module_id'] = test['module_id'].astype(str)

					final_dict = test[['module_id']].to_dict()['module_id'] 
			 map_dict1[str(i)] = final_dict
	
	return map_dict1  #map_dict1-{slot_position:{module_type_id##module_priority_in_slot:module_id}}

module_allocation_udf = udf(module_allocation,MapType(StringType(),MapType(StringType(),StringType())))

df_slot_fun = (df_slot
						.withColumn('map_dict1',module_allocation_udf('campaign_id','test_keys','segment_type_id','number_slots')).cache()) #adds map_dict1

print("------------Finished fun function")

if df_slot_fun.count() <=0 :
	log_df_update(sqlContext,0,'module allocation function',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("Error in module allocation function!!!")
else :
	log_df_update(sqlContext,1,'module allocation function',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')



StartDate = get_pst_date()
### function to assign module content 
def module_info(map_dict1):
	map_dict = {}
	for slot in map_dict1:
			 for key in map_dict1[slot]:
					module_id = map_dict1[slot][key]
					
					if module_id == "dummy":
						 test_dict = {"dummy#dummy":"dummy"}
					
					else:
						 test = dict_cpgn.value[dict_cpgn.value.module_id == int(module_id)][['var_source','var_position','var_structure']]
						 col_req = ['var_source','var_position']
						 test.index = (test[col_req]
												.astype(str)
												.apply(lambda x : '#'.join(x), axis=1))
						 test_dict = test[['var_structure']].to_dict()['var_structure']
					map_dict[str(module_id)] = test_dict #map_dict- {module_id:{var_source##var_position:var_structure}}

	return map_dict
	
module_info_udf = udf(module_info,MapType(StringType(),MapType(StringType(),StringType())))

df_slot_fun_module = (df_slot_fun.withColumn('map_dict',module_info_udf('map_dict1'))
									.repartition(rep,'campaign_id','test_keys','segment_type_id')
								 .cache())
#df_slot_fun_module adds a column map_dict

print("------------Finished module info function")

if df_slot_fun_module.count() <=0 :
	log_df_update(sqlContext,0,'module info function',get_pst_date(),'check df_slot and dict_cpgn tables','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')
	raise Exception("Error in module info function!!!")
else :
	log_df_update(sqlContext,1,'module info function',get_pst_date()," ",'0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')



### extracting col names that are required from travelers data
var_source_list = dfMetaCampaignData_VarDef.select('var_source').distinct().rdd.flatMap(lambda x : x).collect()

var_structure_list = dfMetaCampaignData_VarDef.select('var_structure').filter("var_structure is not null").distinct().rdd.flatMap(lambda x : x).collect()

joining_cols = [val.split('|')[0].split(';') for val in var_source_list if val != None]

joining_cols_final = list(set([col.split('.')[1] for ls in joining_cols for col in ls if len(col.split('.'))>1]))

import re
content_cols = []

#finding table and columns that are required to populate the data
for var_struct in var_structure_list: 
	z = [m.start() for m in re.finditer('%%', var_struct)]
	j = 0 
	while (j <= len(z)-2):
			 temp = var_struct[z[j]+2:z[j+1]]
			 temp_ls = temp.split('.')
			 if temp_ls[0] == 'traveler':   #table 
					content_cols.append(temp_ls[1])
					
			 j+=2
			 
	
content_cols_final = list(set(content_cols)) #list of columns from which data needs to be extracted


### subsetting columns required from traveler data and creating key by concatenating the required columns

global final_travel_cols
final_travel_cols = list(set(joining_cols_final+content_cols_final+['campaign_id','test_keys','segment_type_id']))

final_df_for_alpha_key = final_df_for_alpha.withColumn('key',concat_ws('_',*final_travel_cols))

traveler_data_content = (final_df_for_alpha_key.select(final_travel_cols+['key']).distinct()
										.repartition(rep,'campaign_id','test_keys','segment_type_id')
										.cache()
									)


### meta data creation for campaign suppression, dictionary with default flag and campaign priority
cols_suppression = ['module_id','campaign_id','priority','placement_type','module_type','default']

df_suppre = (dfMetaCampaignData_VarDef.withColumn("default", dfMetaCampaignData_VarDef["default"].cast(IntegerType()))
					 .select(cols_suppression).distinct().toPandas())  #Business understanding. Default column with binary values
df_suppre.index = df_suppre["module_id"]

suppress_dict = df_suppre.to_dict()

cpgn_prio_pd = dfMetaCampaignData_VarDef.select('campaign_id','priority').distinct().toPandas()
cpgn_prio_pd.index = cpgn_prio_pd['campaign_id']

cpgn_prio_dict = cpgn_prio_pd.to_dict()['priority']

StartDate = get_pst_date()
### populating the value in each slot position
##Populating None instead of default content

print("------------started content map function")

def content_map(row):

	import re
	import datetime
	seed = datetime.date.today().isocalendar()[1] + datetime.datetime.today().weekday()
#  import sys
#  reload(sys)
#  sys.setdefaultencoding('UTF-8')


	dict_req = {'key':row['key']}
	cpgn_id = row['campaign_id']
	number_slots = row['number_slots']
	OmniExtension = ""
	campaign_priority_ls = []
	module_id_ls = []

	for slot_position in slot_list:

			 map_dict1 = row['map_dict1'][str(slot_position)]  #moduletypeid##priority->moduleid
			 slot_position = str(slot_position)
			 df = pd.DataFrame()
			 map_dict1 = {key_mod:map_dict1[key_mod] for key_mod in map_dict1 if map_dict1[key_mod] not in module_id_ls} #trying to map for each unique module id
			 flag_dummy = 0

			 if (len(map_dict1.keys())>0) and (list(map_dict1.keys())[0].find("dummy")>=0)  :flag_dummy = 1


			 if flag_dummy == 0:
					for key1 in  map_dict1:
						 map_dict = row['map_dict'][map_dict1[key1]] #var

						 var_positions = slot_position_map[int(slot_position)]

						 x_dict = {('S'+slot_position+'_P'+str(int(v))):'' for v in range(1,var_positions+1)}  #{S1P1:'',S1P2:''...}

						 x_dict['S'+slot_position+'_module_priority'] = int(key1.split('#')[1]) #Gets the module priority as the value
						 x_dict['S'+slot_position+'_module_id'] = int(map_dict1[key1])  #Gets the module id as the value. This is where if there are two values(module_type_id eg.), they are mapped randomly
						 x_dict['S'+slot_position+'_att_option'] = ''

						 for key in map_dict:

								join_key =  key.split('#')[0]
								var_pos =str(key.split('#')[1])

								slot_var_pos = 'S'+slot_position+'_P'+(var_pos) #Assigning the Slot_Pos Name

								if( len(join_key.split('|')) > 1):
									file_name =  join_key.split('|')[1].split(';')[0].split('.')[0]
									left_keys = [col.split('.')[1] for col in join_key.split('|')[0].split(';')]
									right_keys = [col.split('.')[1] for col in join_key.split('|')[1].split(';')]


								else :
									file_name =  'NA'
									left_keys = 'NA'
									right_keys = 'NA'

								if map_dict[key] == None :
									var_value = ""

								elif (map_dict[key] != None) and (len(map_dict[key]) == 0) :
									var_value = ""

								elif(len(map_dict[key].split('%%')) == 1):
										var_value = map_dict[key]

								else :  
									z = [m.start() for m in re.finditer('%%', map_dict[key])]  #gives the index where '%%' is present in the form of a list
									var_value = map_dict[key] #varsource##varposition->varstructure

									j = 0
									while (j <= len(z)-2):
											 temp = map_dict[key][z[j]+2:z[j+1]] # eg. traveler.origincity
											 df_name = temp.split('.')[0] #table


											 if (df_name == 'traveler'):    #when the table name is traveler

													trav_attri = str(row[temp.split('.')[1]])   #getting the column name

													if trav_attri == '':
														 trav_attri = "value_not_found"

													var_value = var_value.replace(temp,trav_attri)      #replacing the temp string in var_value with trav_attri which is the column name


											 else :

													travel_data = "".join([str(row[i]) for i in left_keys ])  #Gets the data from the columns mentioned in left_keys
													mod_id_map = x_dict['S'+slot_position+'_module_id']  #module id
													try : 
														 value_re = mis_content_dict[str(mod_id_map)+df_name+''.join(right_keys)+var_pos][temp.split('.')[1]][travel_data]
														 #value_re contains the data/value. It first accesses the dictionary, finds the column and obtains the data for the column
													except : value_re = ''

													if value_re == '':
																value_re = "value_not_found"


													var_value = var_value.replace(temp,str(value_re))

											 j+=2

								x_total = var_value.replace('%%','')
				
								ttl_options = len(x_total.split("|"))  #no. of options 

								if (ttl_options > 2):
									att = int((int(row['test_keys'])+ seed%(ttl_options-1))%(ttl_options-1))  #changes on a daily basis. 
								else:
									att = 0

								x_dict[slot_var_pos] = x_total.split('|')[0]
								

								if (ttl_options<=1): 
									if ( slot_var_pos == "S1_P1"):
										x_dict[slot_var_pos] = x_total.split('|')[0]       #for att 'default' it has been changed to 'None'
									else:
										x_dict[slot_var_pos] = "None"       #for att 'default' it has been changed to 'None'
									att = 'default'
								elif ((ttl_options==2)):
									x_dict[slot_var_pos] = x_total.split('|')[1]
								else :
									list_value = x_total.split('|')[1:]
									x_dict[slot_var_pos] = list_value[att] 

								x = x_dict[slot_var_pos]
								placement_type_mp = suppress_dict['placement_type'][int(x_dict['S'+slot_position+'_module_id'])]
								default_flag_mp = suppress_dict['default'][int(x_dict['S'+slot_position+'_module_id'])]

								if ((x.lower().find('value_not_found') >= 0) or (x.lower().find('none') >= 0) or (x.find('NaN') >= 0) or (x.find('nan') >=0 )
					or (x.lower().find('null') >= 0) ):
									if ( slot_var_pos == "S1_P1"):
										x_dict[slot_var_pos] = x_total.split('|')[0]       #for att 'default' it has been changed to 'None'
									else:
										x_dict[slot_var_pos] = "None"       #for att 'default' it has been changed to 'None'
									att = 'default'
									if ((placement_type_mp in ('hero','banner')) and (default_flag_mp == 0)) :
											 x_dict['S'+slot_position+'_module_priority'] = 9999


								x_dict['S'+slot_position+'_module_type_id'] = str(key1.split('#')[0])

								x_dict['S'+slot_position+'_att_option'] = x_dict['S'+slot_position+'_att_option'] +'#'+slot_var_pos+'.'+str(att)


						 temp = pd.DataFrame(x_dict,index=[x_dict['S'+slot_position+'_module_priority']])

						 df = df.append(temp) 
                    
                    #here module_type_id can be randomly allocated as it doesn't contain the logic of choosing the module_type_id based on its priority as can be found in the else function below
					if len(df) == 0:
						 var_positions = slot_position_map[int(slot_position)]
						 x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}  #slot var pos has been changed to none
						 x_dict['S'+slot_position+'_module_priority'] = "pixel_module"
						 mod_id = '999'
						 x_dict['S'+slot_position+'_module_id'] = int(mod_id)   #haven't changed pixel_module to null. only slot var position has to be changed
						 x_dict['S'+slot_position+'_att_option'] = "pixel_module"
						 x_dict['S'+slot_position+'_module_type_id'] = "pixel_module"
						 campaign_priority = int(cpgn_prio_dict[cpgn_id])  #campaign id is mapped to campaign priority
						 campaign_priority_ls += [campaign_priority] #List of campaign priority is made
						 dict_req.update(x_dict)

					else :
						 final_df = df.loc[[df['S'+slot_position+'_module_priority'].idxmin()]]    
						 col_name = list(final_df['S'+slot_position+'_module_priority'])[0]

						 mod_id = final_df['S'+slot_position+'_module_id'].iloc[0]
						 final_dict = final_df.transpose().to_dict()[col_name]

						 module_id = str(final_dict['S{}_module_id'.format(slot_position)])
						 placement_type = suppress_dict['placement_type'][int(module_id)]
						 module_type = suppress_dict['module_type'][int(module_id)]
						 default_flag = suppress_dict['default'][int(module_id)]
						 campaign_priority = int(cpgn_prio_dict[cpgn_id])

						 if final_df['S'+slot_position+'_module_priority'].iloc[0] == 9999 :

								if ((default_flag == 0) and (placement_type == 'hero')) : 
									campaign_priority = 9999
								elif ((default_flag == 0) and (placement_type == 'banner')):
									mod_id = '999'
									final_dict['S'+slot_position+'_module_id'] = int(mod_id)
									for i in range(1,var_positions+1):
											 final_dict['S'+slot_position+'_P'+str(int(i))] = "None"    #has been changed to 'None' for default flag =0 and placement_type = 'banner'

						 campaign_priority_ls += [campaign_priority]

						 dict_req.update(final_dict)

			 else :     #filling the slots where flag_dummy=1
					var_positions = slot_position_map[int(slot_position)]
					x_dict = {('S'+slot_position+'_P'+str(int(v))):"None" for v in range(1,var_positions+1)}    #when flag_dummy =1 then slot var pos has been changed to 'None'
					x_dict['S'+slot_position+'_module_priority'] = "dummy"
					mod_id = "999"          #dummy still exists for module priority, att option and module type id
					x_dict['S'+slot_position+'_module_id'] = int(mod_id)
					x_dict['S'+slot_position+'_att_option'] = "dummy"
					x_dict['S'+slot_position+'_module_type_id'] = "dummy"
					campaign_priority = int(cpgn_prio_dict[cpgn_id])
					campaign_priority_ls += [campaign_priority]
					dict_req.update(x_dict)

			 if (int(slot_position) < number_slots): OmniExtension += (slot_position+'.'+str(mod_id)+'_')
			 else : OmniExtension += (slot_position+'.'+str(mod_id))
			 module_id_ls.append(str(mod_id))  

	dict_req['OmniExtension'] = OmniExtension
	dict_req['OmniExtNewFormat'] = OmniExtension.replace('.','-')

	campaign_priority_ls.sort(reverse=True)

	dict_req['campaign_priority'] = campaign_priority_ls[0]
	

	return Row(**dict_req)


content_map_data = (traveler_data_content
								.join(df_slot_fun_module,['campaign_id','test_keys','segment_type_id'],'inner')
								.repartition(rep)
								.cache()
							)


### defining final col list required in alpha output
cols_occ = ['tpid','eapid','email_address','expuserid','first_name','mer_status','lang_id','last_name','locale','paid',
					'test_keys']

cols_occ_final = cols_occ + ['campaign_id','key']
final_df_for_alpha_key_occ = final_df_for_alpha_key.select(cols_occ_final)



### Applying campaign suppression filter and assigning campaign according to the priority
window_rank = Window.partitionBy("email_address").orderBy("campaign_priority")

print("------------calling content map function")
try :
	#grab only campaign_id, locale, LaunchDateTimeUTC
	#dfMetaCampaignData1 is already filtered for campaigns going that day

	dfCampaginLaunchData = (dfMetaCampaignData1
			.select("campaign_id", "locale", "LaunchDateTimeUTC")
		)

	final_result = (content_map_data.withColumn('test_keys',content_map_data['test_keys'].cast(IntegerType()))
									.rdd.map(lambda x : content_map(x)).toDF()
								.join(final_df_for_alpha_key_occ,'key','inner').drop('key')
								 .withColumn('ModuleCount',lit(slots))          #adds number of slots
								 .join(dfCampaginLaunchData, ["campaign_id","locale"],"inner")
								 .withColumn("cpgn_rank",rank().over(window_rank))   #rank
							 .filter("cpgn_rank = 1").drop("cpgn_rank")
							 .filter("campaign_priority != 9999")
							 .filter("S1_module_id!=999") ### removing users having no module mapping for subjectline 07/06/2017
							 .filter("S2_module_id!=999") ### removing users having no module mapping for hero 07/06/2017
							 .cache())
							 
	
	#function converts campaign_id from string back to int
	def int_maker(campaign_id):
			id = int(campaign_id)
			return(id)
    
	int_maker_udf = udf(int_maker,IntegerType()) 

	final_result = final_result.withColumn("campaign_id",int_maker_udf("campaign_id"))	 
	
	
	log_df_update(sqlContext,1,'content map function',get_pst_date(),' ',str(final_result.count()),StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')                
except :
	log_df_update(sqlContext,0,'content map function',get_pst_date(),'error in content map function','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')

	raise Exception("error in content map!!!")

	

### Getting slot count or module count for each campaign in case of multiple campaigns

print("------------calling module count map")
module_count_map = (dfMetaCampaignData_VarDef
								.groupBy("campaign_id","AudienceTableName").agg(countDistinct(dfMetaCampaignData_VarDef.slot_position).alias("ModuleCount")))



### User Token Changes Starts Here ###

StartDate = get_pst_date()

### functions needed
def importSQLTable(dbname, tablename):
	temp = (sqlContext.read.format("jdbc")
	.option("url", url)
	.option("driver",properties["driver"])
	.option("dbtable", dbname+".dbo."+tablename)
	.option("user", properties["user"])
	.option("password", properties["password"]).load())
	return temp

def add_userToken(final_df,UT_table_name):
	df_Tokens = importSQLTable("CampaignHistory",UT_table_name)
	df_sub_tokens = importSQLTable("CampaignHistory","Subscriber_UserTokens")
	df_Tokens2 = (df_Tokens.select("ProtoAccountID","UserToken")
				.withColumnRenamed("ProtoAccountID","PAID")
				.withColumn("UserToken",concat(lit("emailclick/"),df_Tokens.UserToken, lit("/") )))
	df_sub_tokens2 = (df_sub_tokens.select("SubscriberID","UserToken")
			.withColumnRenamed("SubscriberID","PAID")
			.withColumn("UserToken",concat(lit("emailclick/"),df_sub_tokens.UserToken, lit("/") ))
			.withColumnRenamed("UserToken","UserToken2"))
	final_df_withUT = final_df.join(df_Tokens2,["PAID"],'left').join(df_sub_tokens2,["PAID"],'left')
	final_df = (final_df_withUT.withColumn("UserToken",coalesce(final_df_withUT.UserToken,final_df_withUT.UserToken2,lit("")))
		.drop("UserToken2"))

	return final_df

### get user Token Configurations from SQL
config = importSQLTable("CampaignHistory","UserToken_Locale_Config")
UT_table_df = config.filter(pos_filter_cond).select("UserTokenTableName").collect()
UserTokenTableName =  UT_table_df[0]["UserTokenTableName"]


### Filter by PAID is NOT NULL (null paids do not get sent)
data = final_result.filter("paid is not null")

try:
	### Add user Token proc
	final_result = add_userToken(data,UserTokenTableName)
	log_df_update(sqlContext,1,'UserToken added and data filtered by PAID IS NOT NULL',get_pst_date(),' ',str(final_result.count()),StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')

except:
	log_df_update(sqlContext,0,'UserToken added and data filtered by PAID IS NOT NULL',get_pst_date(),'UserToken added and data filtered by PAID IS NOT NULL','0',StartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression  process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.AlphaProcessDetailsLog')
	log_df_update(sqlContext,0,'Campaign Suppression process failed',get_pst_date(),'Error','0',AlphaStartDate,' ','Orchestration.dbo.CentralLog')

	raise Exception("error in content map!!!")

### User Token Changes Ends Here ###


### Adding Seed Emails Starts Here ###
StartDate = get_pst_date()

seeds = (importSQLTable("AlphaStaging","AlphaSeedEmails")).filter("CampaignCategory = 'Merch' and IsActive = 1")
SeedWindow = Window.orderBy("SeedEmail")
AlphaOutputWindow = Window.partitionBy("campaign_id").orderBy("test_keys")

seedEmails = seeds.filter(pos_filter_cond).select("tpid","eapid","locale","SeedEmail").distinct().withColumn("row_id",row_number().over(SeedWindow))

seedCounts = seedEmails.count()

sampleForSeed = (final_result.withColumn("row_id",row_number().over(AlphaOutputWindow))
		.filter("row_id <= "+str(seedCounts))
	)

sampleAfterSeed = (sampleForSeed.join(seedEmails, ["tpid","eapid","locale","row_id"], "inner")
	    .drop("row_id").drop("email_address")
	    .withColumnRenamed("SeedEmail","email_address")
	    .withColumn("PAID",lit(999999999))
    )

ListOfColumns = [col for col in final_result.columns]
	
final_result = final_result.select(ListOfColumns).unionAll(sampleAfterSeed.select(ListOfColumns))

log_rowcount = final_result.count()

log_df_update(sqlContext,1,'Adding Seed Emails',get_pst_date(),' ',str(final_result.count()),StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')

### Adding Seed Emails Ends Here ###

StartDate = get_pst_date()

final_result_moduleCount1 = final_result.drop("ModuleCount").join(module_count_map,"campaign_id","inner").dropDuplicates(["email_address", "paid"])

log_rowcount = final_result_moduleCount1.count()

log_df_update(sqlContext,1,'Duplicates have been dropped',get_pst_date(),' ',str(log_rowcount),StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')

StartDate = get_pst_date()

##Converting None to null type
from pyspark.sql.functions import col, when

#function to replace None with null
def blank_as_null(x):
    return when(col(x) == "None", None).otherwise(col(x))

#taking the distinct columns from final_result_moduleCount1  
final_result_moduleCount1_distinct_columns = set(final_result_moduleCount1.columns) # Some set of columns

exprs = [
    blank_as_null(x).alias(x) if x in final_result_moduleCount1_distinct_columns else x for x in final_result_moduleCount1.columns]

final_result_moduleCount2 = final_result_moduleCount1.select(*exprs)

final_result_moduleCount = (final_result_moduleCount2)

log_df_update(sqlContext,1,'Default Value nulled',get_pst_date(),' ','0',StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')


##Addition of loyalty fields for Omni Process


StartDate = get_pst_date()

df_loyalty = importSQLTable("AlphaStaging","SterlingAndEliteMembers").drop('LANG_ID').drop("LRMPendingPoints")


alphaOmni = (final_result_moduleCount2
	.withColumnRenamed('tpid','TPID')
	.withColumnRenamed('eapid','EAPID')
	.withColumnRenamed('first_name','FIRST_NAME')
	.withColumnRenamed('last_name','LAST_NAME')
	.withColumnRenamed('lang_id','LANG_ID')
	.withColumnRenamed('mer_status','IsMER')
	.withColumnRenamed('paid','PAID')
	.withColumnRenamed('email_address','EmailAddress')
	.withColumn('SubjectLine',final_result_moduleCount2["S1_P1"])
)

alphaOmni1 = (alphaOmni
	.withColumn('PM_OK_IND', alphaOmni["IsMER"])
	.join(df_loyalty,['TPID','PAID'],'left'))

final_result_moduleCount = (alphaOmni1
	.withColumnRenamed('MemberID','LoyaltyMemberID')
	.withColumnRenamed('LRMTierName','LoyaltyMemberTierName')
	.withColumnRenamed('MonetaryValue','MonetaryValue')
	.withColumnRenamed('LRMAvailablePoints','LoyaltyAvailablePoints')
)


def loyalty(value):
   if   value == "Active": return 1
   else : return 0
	   
udfloyalty = udf(loyalty, StringType())

final_result_moduleCount = final_result_moduleCount.withColumn("LoyaltyMemberStatus", udfloyalty("LRMStatus"))

## Dropping unwanted columns
final_result_moduleCount = (final_result_moduleCount
	.drop("AudienceTableName")
	.drop("LRMStatus")
	.drop("campaign_priority")
)

log_df_update(sqlContext,1,'Loyalty Fields added',get_pst_date(),' ',str(log_rowcount),StartDate,File_path,'Orchestration.dbo.AlphaProcessDetailsLog')

### removing cols not required in output
col_final_ls = [col for col in final_result_moduleCount.columns if col.find("authrealm")<0]


print("------------Writing data into Bucket")

StartDate = get_pst_date()
### writing output in S3 bucket
path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/"+file_name_s3
(final_result_moduleCount.select(col_final_ls).coalesce(10).write.mode("overwrite").parquet(path))

log_df_update(sqlContext,1,'Writing parquet file to s3 is Done',get_pst_date(),' ',str(log_rowcount),StartDate,path,'Orchestration.dbo.AlphaProcessDetailsLog')


StartDate = get_pst_date()
### writing path of the alpha output in S3 to be picked for S3toSQL writer function in scala
output_rdd_path = "s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/alphaToSqlTest/rdd_path/{}/{}/{}".format(locale_name,LaunchDate,file_name_s3)
ls = [path+"/*"]
rdd_path = sqlContext.createDataFrame(pd.DataFrame(ls)).withColumnRenamed("0","path")
rdd_path.coalesce(1).write.mode("overwrite").parquet(output_rdd_path)

log_df_update(sqlContext,1,'writing meta campaign data as source tables keep on changing in SQL sever',get_pst_date(),' ','0',StartDate,path,'Orchestration.dbo.AlphaProcessDetailsLog')

print("------------writing meta campaign data as source tables keep on changing in SQL sever")
### writing meta campaign data as source tables keep on changing in SQL sever , this data is required for campaign analysis
dfMetaCampaignData_VarDef.write.mode("overwrite").parquet("s3n://big-data-analytics-scratch-prod/project_traveler_profile/affine/config_file/{}/{}/{}".format(locale_name,current_date,file_name_s3))

log_df_update(sqlContext,1,'Campaign Suppression  process completed',get_pst_date(),' ',str(log_rowcount),AlphaStartDate,path,'Orchestration.dbo.AlphaProcessDetailsLog')

log_df_update(sqlContext,1,'Campaign Suppression completed',get_pst_date(),' ',str(log_rowcount),AlphaStartDate,path,'Orchestration.dbo.CentralLog')
