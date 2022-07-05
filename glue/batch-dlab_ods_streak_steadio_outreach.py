import sys
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, BooleanType
from utils import parse_json


args = getResolvedOptions(sys.argv, ["JOB_NAME", "user", "database", "password", "url", "arn", "jdbc-conn", "temp-path"])
glue_context = GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)
logger = glue_context.get_logger()

df_dlab_ods_streak_steadio_outreach = glue_context.create_dynamic_frame.from_options(
    connection_type = "redshift"
    ,connection_options = {
                            "url": args["url"]
                            ,"database" : args["database"]
                            ,"user" : args["user"]
                            ,"password" : args["password"]
                            ,"query":"""
                                    SELECT assignedtosharingentries AS email
                                        , fields
                                        , stagekey AS stage_type_id
                                        , createddate AS created_at
                                    FROM tumblbug_dw.streak.boxes
                                    WHERE pipelineid = 'agxzfm1haWxmb29nYWVyMwsSDE9yZ2FuaXphdGlvbiIMdHVtYmxidWcuY29tDAsSCFdvcmtmbG93GICAxoW9psQLDA'
                                    """
                            ,"redshiftTmpDir" : args["temp-path"]
                            ,"connectionName": args["jdbc-conn"]
                            ,"aws_iam_role": args["arn"]
                        }
    ).toDF()

#Define user defined functions & apply to the targeted columns
udf_email = udf(lambda x: parse_json(json_val=x, key="email", separator='@', idx=0),StringType())
udf_cat = udf(lambda x: parse_json(json_val=x, key="1003"),IntegerType())
udf_tumblbug = udf(lambda x: parse_json(json_val=x, key="1014"),BooleanType())
udf_lead = udf(lambda x: parse_json(json_val=x, key="1002"),IntegerType())

parsed_dlab_ods_streak_steadio_outreach = df_dlab_ods_streak_steadio_outreach.\
                                            withColumn("rep_name", udf_email(df_dlab_ods_streak_steadio_outreach["email"])).\
                                            withColumn("category_id", udf_cat(df_dlab_ods_streak_steadio_outreach["fields"])).\
                                            withColumn("is_tumblbug", udf_tumblbug(df_dlab_ods_streak_steadio_outreach["fields"])).\
                                            withColumn("leadsource_id", udf_lead(df_dlab_ods_streak_steadio_outreach["fields"]))
pre_dlab_ods_streak_steadio_outreach = parsed_dlab_ods_streak_steadio_outreach.\
                                        select("rep_name", "category_id", "stage_type_id", "is_tumblbug", "leadsource_id", "created_at")
pre_dlab_ods_streak_steadio_outreach = DynamicFrame.fromDF(pre_dlab_ods_streak_steadio_outreach
                                                           ,glue_context
                                                           ,"pre_dlab_ods_streak_steadio_outreach")

#Turncate table & refresh table with the dynamic frame
pre_action = "TRUNCATE TABLE dlab.temp_streak_job"
pre_dlab_temp_streak_job = glue_context.write_dynamic_frame.from_jdbc_conf(
    frame=pre_dlab_ods_streak_steadio_outreach
    ,catalog_connection=args["jdbc-conn"]
    ,connection_options={
                        "database": "dev"
                        ,"dbtable": "dlab.temp_streak_job"
                        ,"preactions": pre_action
                    }
    ,redshift_tmp_dir=args["temp-path"]
)

job.commit()
