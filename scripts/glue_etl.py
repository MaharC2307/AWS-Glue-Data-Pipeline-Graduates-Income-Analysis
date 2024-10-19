import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from recipe_transforms import *
from awsglue.dynamicframe import DynamicFrame

# Generated recipe steps for DataPreparationRecipe_node1726844545098
def applyRecipe_node1726844545098(inputFrame, glueContext, transformation_ctx):
    frame = inputFrame.toDF()
    gc = glueContext
    df1 = DataQuality.RemoveValues.apply(
        data_frame=frame,
        glue_context=gc,
        transformation_ctx="DataPreparationRecipe_node1726844545098-df1",
        condition_expressions=[{
          "Condition": "CONTAINS",
          "Value": "NA",
          "TargetColumn": "International Median income 5 years after graduation"
        }]
    )
    df2 = DataQuality.RemoveValues.apply(
        data_frame=df1,
        glue_context=gc,
        transformation_ctx="DataPreparationRecipe_node1726844545098-df2",
        condition_expressions=[{
          "Condition": "IS_NOT",
          "Value": "[\"31,500\",\"28,300\",\"31,700\",\"42,400\",\"45,400\",\"53,900\",\"50,500\",\"40,000\",\"41,800\"]",
          "TargetColumn": "Canadin Median income 5 years after graduation"
        }]
    )
    return DynamicFrame.fromDF(df2, gc, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Canadian Students
CanadianStudents_node1726843388187 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://de-employement-project/staging/CANADIAN_STUDENTS.csv"], "recurse": True}, transformation_ctx="CanadianStudents_node1726843388187")

# Script generated for node International Students
InternationalStudents_node1726843393697 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://de-employement-project/staging/INTERNATIONAL STUDENTS.csv"], "recurse": True}, transformation_ctx="InternationalStudents_node1726843393697")

# Script generated for node Join
Join_node1726844389753 = Join.apply(frame1=CanadianStudents_node1726843388187, frame2=InternationalStudents_node1726843393697, keys1=["educational qualification", "field of study"], keys2=["educational qualification", "field of study"], transformation_ctx="Join_node1726844389753")

# Script generated for node Change Schema
ChangeSchema_node1726844750293 = ApplyMapping.apply(frame=Join_node1726844389753, mappings=[("educational qualification", "string", "educational qualification", "string"), ("field of study", "string", "field of study", "string"), ("canadian graduates", "string", "canadian graduates", "string"), ("canadian median  income 2 years after graduation", "string", "canadian median  income 2 years after graduation", "string"), ("canadin median income 5 years after graduation", "string", "canadin median income 5 years after graduation", "string"), ("`.educational qualification`", "string", "`.educational qualification`", "string"), ("`.field of study`", "string", "`.field of study`", "string"), ("international graduates", "string", "international graduates", "string"), ("international median income 2 years after graduation", "string", "international median income 2 years after graduation", "string"), ("international median income 5 years after graduation", "string", "international median income 5 years after graduation", "string")], transformation_ctx="ChangeSchema_node1726844750293")

# Script generated for node Data Preparation Recipe
# Adding configuration for certain Data Preparation recipe steps to run properly
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Recipe name: DataPreparationRecipe_node1726844545098
DataPreparationRecipe_node1726844545098 = applyRecipe_node1726844545098(
    inputFrame=ChangeSchema_node1726844750293,
    glueContext=glueContext,
    transformation_ctx="DataPreparationRecipe_node1726844545098")

# Script generated for node Amazon S3
AmazonS3_node1726845486777 = glueContext.write_dynamic_frame.from_options(frame=DataPreparationRecipe_node1726844545098, connection_type="s3", format="glueparquet", connection_options={"path": "s3://de-employement-project/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1726845486777")

job.commit()
