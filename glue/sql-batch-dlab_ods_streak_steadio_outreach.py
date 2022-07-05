import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


####################################################################################################
# variable setting
####################################################################################################
args = getResolvedOptions(sys.argv, ["JOB_NAME", "user", "database", "password", "url"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

####################################################################################################
# source data
####################################################################################################
source_dlab_ods_streak_steadio_outreach = glueContext.create_dynamic_frame.from_options(
    connection_type = "redshift",
    connection_options = {
                            "url": args["url"],
                            "database" : args["database"],
                            "user" : args["user"],
                            "password" : args["password"],
                            "query":"""
                                    select
                                        b.id as id
                                    ,	p.id as pipeline_id
                                    ,	p.name as pipeline_name
                                    ,	split_part(
                                        json_extract_path_text(
                                            json_extract_array_element_text(assignedtosharingentries, 0), 'email'
                                        ), '@', 1) as rep_name
                                    ,	case when json_extract_path_text(b.fields,'1003') != '' 
                                        then cast(json_extract_path_text(b.fields,'1003') as int) end as category_id
                                    ,   cast(b.stagekey as int) as stage_type_id
                                    ,	decode(json_extract_path_text(b.fields,'1014'), 'true', true, 'false', false) as is_tumblbug
                                    ,	case when json_extract_path_text(b.fields,'1002') != '' 
                                        then cast(json_extract_path_text(b.fields,'1002') as int) end as leadsource_id
                                    ,	b.createddate as created_at
                                    ,   cast(current_timestamp + interval '9 hour' as timestamp) as batched_at
                                    from tumblbug_dw.streak.boxes as b
                                    inner join tumblbug_dw.streak.pipelines as p
                                    on b.pipelineid = p.id
                                    where p.id = 'agxzfm1haWxmb29nYWVyMwsSDE9yZ2FuaXphdGlvbiIMdHVtYmxidWcuY29tDAsSCFdvcmtmbG93GICAxoW9psQLDA'
                                    """,
                            "redshiftTmpDir" : "s3://steadio-glue-info/temporary/",
                            "connectionName":"steadio-segment-con-jdbc",
                            "aws_iam_role": "arn:aws:iam::458226961977:role/redshift-exec-role"
                            }
                        )

####################################################################################################
# Truncate & refresh the target table
####################################################################################################
pre_action = "TRUNCATE TABLE dlab.ods_streak_steadio_outreach"
pre_dlab_temp_streak_job = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=source_dlab_ods_streak_steadio_outreach
    ,catalog_connection="steadio-segment-con"
    ,connection_options={
        "database": "dev",
        "dbtable": "dlab.ods_streak_steadio_outreach",
        "preactions": pre_action
    }
    ,redshift_tmp_dir="s3://steadio-glue-info/temporary/"
)

job.commit()
