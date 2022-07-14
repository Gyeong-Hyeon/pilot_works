import sys
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

####################################################################################################
# job argument setting
####################################################################################################
config = {
            "DefaultArguments": {
                                "--job-bookmark-option": "job-bookmark-disable",
                                "--enable-job-insights": "true",
                                "--enable-metrics": "true",
                                "--enable-continuous-cloudwatch-log": "true",
                                "--enable-spark-ui": "true",
                                "--spark-event-logs-path": "s3://tumblbug-glue-info/spark/",
                                "--TempDir": "s3://tumblbug-glue-info/temporary/",
                                "--enable-glue-datacatalog": "true",
                                "--user": "richard",
                                "--database": "dev",
                                "--password": "Hg6nNbUe24ud6WwM",
                                "--url": "jdbc:redshift://tumblbug-segment.cjsdmq3uescx.ap-northeast-1.redshift.amazonaws.com:5439/dev",
                                "--start_at" : "''",
                                "--end_at" : "''",
                                "--is_full_refresh" : "false"
                            },
            "Connections": {
                            "Connections": [
                                "tumblbug-segment-con"
                            ]
                        },
            "MaxRetries": 0,
            "Timeout": 60,
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X"
        }
args = getResolvedOptions(sys.argv, ["JOB_NAME", "user", "database", "password", "url", "start_at", "end_at", "is_full_refresh"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

####################################################################################################
# param setting
####################################################################################################

start_at = "date_trunc('day', current_timestamp + interval '9 hours') + interval '-1 days'"
end_at = "date_trunc('day', current_timestamp + interval '9 hours')"
pre_query = "delete from dlab.dm_user_visit_date where (active_at >= {} and active_at < {})".format(start_at, end_at)

if args["start_at"] != "''":
    start_at = "cast(" + "'" + args["start_at"] + "'" + " as timestamp)"

if args["end_at"] != "''":
    end_at = "cast(" + "'" + args["end_at"] + "'" + " as timestamp)"

if args["is_full_refresh"] == "true":
    start_at = "cast('1999-01-01' as timestamp)"
    pre_query = "truncate table dlab.ods_streak_steadio_outreach"

####################################################################################################
# source data
####################################################################################################
    source_web_track = glueContext.create_dynamic_frame.from_options(
        connection_type = "redshift",
        connection_options = {
            "url": args["url"],
            "database" : args["database"],
            "user" : args["user"],
            "password" : args["password"],
            "query":f"""
                    select
                        anonymous_id
                        , event_at
                        , sum(web_event) as web_event
                    from(
                        select
                            anonymous_id
                            , date_trunc('day', timestamp + interval '9 hour') as event_at
                            , count(*) as web_event
                        from tumblbug_web.tracks
                        where (anonymous_id is not null
                        and timestamp + interval '9 hour' >= {start_at}
                        and timestamp + interval '9 hour' < {end_at})
                        group by 1,2
                        
                        union all
                        
                        select
                            anonymous_id
                            , date_trunc('day', received_at + interval '9 hour') as event_at
                            , count(*) as web_event
                        from tum_clickstream.track
                        where (anonymous_id is not null
                        and received_at + interval '9 hour' >= {start_at}
                        and received_at + interval '9 hour' < {end_at}) 
                        group by 1,2
                    )
                    group by 1,2
                    """,
            "redshiftTmpDir" : args["TempDir"],
            "connectionName":"tumblbug-segment-con-jdbc",
            "aws_iam_role": "arn:aws:iam::458226961977:role/redshift-exec-role"
        }
    )


source_aos_track = glueContext.create_dynamic_frame.from_options(
    connection_type = "redshift",
    connection_options = {
        "url": args["url"],
        "database" : args["database"],
        "user" : args["user"],
        "password" : args["password"],
        "query":f"""
                select
                    anonymous_id
                    , date_trunc('day', timestamp + interval '9 hour') as event_at
                    , count(*) as aos_event
                from tumblbug_aos.tracks
                where (anonymous_id is not null
                and timestamp + interval '9 hour' >= {start_at}
                and timestamp + interval '9 hour' < {end_at})
                group by 1,2
                """,
        "redshiftTmpDir" : args["TempDir"],
        "connectionName":"tumblbug-segment-con-jdbc",
        "aws_iam_role": "arn:aws:iam::458226961977:role/redshift-exec-role"
    }
)

source_ios_track = glueContext.create_dynamic_frame.from_options(
    connection_type = "redshift",
    connection_options = {
        "url": args["url"],
        "database" : args["database"],
        "user" : args["user"],
        "password" : args["password"],
        "query":f"""
                select
                    anonymous_id
                    , date_trunc('day', "timestamp" + interval '9 hour') as event_at
                    , count(*) as ios_event
                from tumblbug_ios.tracks
                where (anonymous_id is not null
                and timestamp + interval '9 hour' >= {start_at}
                and timestamp + interval '9 hour' < {end_at})
                group by 1,2
                """,
        "redshiftTmpDir" : args["TempDir"],
        "connectionName":"tumblbug-segment-con-jdbc",
        "aws_iam_role": "arn:aws:iam::458226961977:role/redshift-exec-role"
    }
)

####################################################################################################
# logic(SQL Transform)
####################################################################################################

sql_query = """
    select anonymous_id
        , event_at
        , decode(max(web_event), 0, 0, 1) as web_visit
        , max(web_event) as web_event
        , decode(max(aos_event), 0, 0, 1) as aos_visit
        , max(aos_event) as aos_event
        , decode(max(ios_event), 0, 0, 1) as ios_visit
        , max(ios_event) as ios_event
        , decode((web_visit + aos_visit + ios_visit), 0, 0, 1) as total_visit
        , (max(web_event) + max(aos_event) + max(ios_event)) as total_event
    from (
        select anonymous_id, event_at, web_event, 0 as aos_event, 0 as ios_event
        from web_track
        union all
        select anonymous_id, event_at, 0 as web_event, aos_event, 0 as ios_event
        from aos_track
        union all
        select anonymous_id, event_at, 0 as web_event, 0 as aos_event, ios_event
        from ios_track
    )
    group by 1,2;
"""

sql_transform = sparkSqlQuery(
    glueContext,
    query = sql_query,
    mapping = {
        "web_track": source_web_track,
        "aos_track": source_aos_track,
        "ios_track": source_ios_track
    },
    transformation_ctx="sql_transform"
)

####################################################################################################
# target data
####################################################################################################

target_dlab_dm_user_visit_date = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = sql_transform,
    catalog_connection = "tumblbug-segment-con",
    connection_options = {
        "database": args["database"],
        "dbtable": "dlab.dm_user_visit_date",
        "preactions": pre_query
    },
    redshift_tmp_dir = args["TempDir"],
    transformation_ctx="target_dlab_dm_user_visit_date"
)

job.commit()
