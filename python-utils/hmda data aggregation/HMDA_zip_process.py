import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
glueContext._jvm.System.setProperty("spark.sql.ansi.enabled", "false")
database_name = 'user_info_db'
spark.conf.set('spark.sql.caseSensitive', True)
schema_2 = StructType([
    StructField("activity_year", IntegerType(), True),
    StructField("lei", StringType(), True),
    StructField("loan_type", IntegerType(), True),
    StructField("loan_purpose", IntegerType(), True),
    StructField("preapproval", IntegerType(), True),
    StructField("construction_method", IntegerType(), True),
    StructField("occupany_type", IntegerType(), True),
    StructField("loan_amount", DecimalType(38, 2), True),
    StructField("action_taken", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("county", StringType(), True),
    StructField("census_tract", StringType(), True),
    StructField("ethn_borr_1", IntegerType(), True),
    StructField("ethn_borr_2", IntegerType(), True),
    StructField("ethn_borr_3", IntegerType(), True),
    StructField("ethn_borr_4", IntegerType(), True),
    StructField("ethn_borr_5", IntegerType(), True),
    StructField("ethn_co_borr_1", IntegerType(), True),
    StructField("ethn_co_borr_2", IntegerType(), True),
    StructField("ethn_co_borr_3", IntegerType(), True),
    StructField("ethn_co_borr_4", IntegerType(), True),
    StructField("ethn_co_borr_5", IntegerType(), True),
    StructField("ethn_borr_vis", IntegerType(), True),
    StructField("ethn_co_borr_vis", IntegerType(), True),
    StructField("race_borr_1", IntegerType(), True),
    StructField("race_borr_2", IntegerType(), True),
    StructField("race_borr_3", IntegerType(), True),
    StructField("race_borr_4", IntegerType(), True),
    StructField("race_borr_5", IntegerType(), True),
    StructField("race_co_borr_1", IntegerType(), True),
    StructField("race_co_borr_2", IntegerType(), True),
    StructField("race_co_borr_3", IntegerType(), True),
    StructField("race_co_borr_4", IntegerType(), True),
    StructField("race_co_borr_5", IntegerType(), True),
    StructField("race_borr_vis", IntegerType(), True),
    StructField("race_co_borr_vis", IntegerType(), True),
    StructField("sex_borr", IntegerType(), True),
    StructField("sec_co_borr", IntegerType(), True),
    StructField("sex_borr_vis", IntegerType(), True),
    StructField("sec_co_borr_vis", IntegerType(), True),
    StructField("age_borr", StringType(), True),
    StructField("age_borr_g_62", StringType(), True),
    StructField("age_co_borr", StringType(), True),
    StructField("age_co_borr_g_62", StringType(), True),
    StructField("income", StringType(), True),
    StructField("type_of_purchaser", IntegerType(), True),
    StructField("rate_spread", StringType(), True),
    StructField("hoepa_status", IntegerType(), True),
    StructField("line_status", IntegerType(), True),
    StructField("credit_model_borr", IntegerType(), True),
    StructField("credit_model_co_borr", IntegerType(), True),
    StructField("denial_reason_1", IntegerType(), True),
    StructField("denial_reason_2", IntegerType(), True),
    StructField("denial_reason_3", IntegerType(), True),
    StructField("denial_reason_4", IntegerType(), True),
    StructField("total_loan_cost", StringType(), True),
    StructField("points_and_fees", StringType(), True),
    StructField("origination_charges", StringType(), True),
    StructField("discount_points", StringType(), True),
    StructField("lender_credits", StringType(), True),
    StructField("interest_rate", StringType(), True),
    StructField("ppp_term", StringType(), True),
    StructField("dti", StringType(), True),
    StructField("cltv", StringType(), True),
    StructField("loan_term", StringType(), True),
    StructField("intro_rate_period", StringType(), True),
    StructField("balloon_payment", IntegerType(), True),
    StructField("io_payments", IntegerType(), True),
    StructField("neg_ammortization", IntegerType(), True),
    StructField("oth_neg_ammortization", IntegerType(), True),
    StructField("property_value", StringType(), True),
    StructField("manu_home_sec_prop_type", IntegerType(), True),
    StructField("manu_home_land_prop_interest", IntegerType(), True),
    StructField("total_units", StringType(), True),
    StructField("mf_affordable_units", StringType(), True),
    StructField("submission_of_app", IntegerType(), True),
    StructField("payble_to_inst", IntegerType(), True),
    StructField("aus1", IntegerType(), True),
    StructField("aus2", IntegerType(), True),
    StructField("aus3", IntegerType(), True),
    StructField("aus4", IntegerType(), True),
    StructField("aus5", IntegerType(), True),
    StructField("reverse_mort", IntegerType(), True),
    StructField("open_end_loc", IntegerType(), True),
    StructField("bus_comm", IntegerType(), True)
])

schema_1 = StructType([
    StructField("activity_year", IntegerType(), True),
    StructField("lei", StringType(), True),
    StructField("derived_msa_md", StringType(), True),
    StructField("state_code", StringType(), True),
    StructField("county_code", StringType(), True),
    StructField("census_tract", StringType(), True),
    StructField("conforming_loan_limit", StringType(), True),
    StructField("derived_loan_product_type", StringType(), True),
    StructField("derived_dwelling_category", StringType(), True),
    StructField("derived_ethnicity", StringType(), True),
    StructField("derived_race", StringType(), True),
    StructField("derived_sex", StringType(), True),
    StructField("action_taken", IntegerType(), True),
    StructField("purchaser_type", IntegerType(), True),
    StructField("preapproval", IntegerType(), True),
    StructField("loan_type", IntegerType(), True),
    StructField("loan_purpose", IntegerType(), True),
    StructField("lien_status", IntegerType(), True),
    StructField("reverse_mortgage", IntegerType(), True),
    StructField("open_end_line_of_credit", IntegerType(), True),
    StructField("business_or_commercial_purpose", IntegerType(), True),
    StructField("loan_amount", DecimalType(38, 2), True),
    StructField("combined_loan_to_value_ratio", DecimalType(38, 2), True),
    StructField("interest_rate", DecimalType(38, 2), True),
    StructField("rate_spread", DecimalType(38, 2), True),
    StructField("hoepa_status", IntegerType(), True),
    StructField("total_loan_costs", DecimalType(38, 2), True),
    StructField("total_points_and_fees", DecimalType(38, 2), True),
    StructField("origination_charges", DecimalType(38, 2), True),
    StructField("discount_points", DecimalType(38, 2), True),
    StructField("lender_credits", DecimalType(38, 2), True),
    StructField("loan_term", StringType(), True),
    StructField("prepayment_penalty_term", StringType(), True),
    StructField("intro_rate_period", StringType(), True),
    StructField("negative_amortization", IntegerType(), True),
    StructField("interest_only_payment", IntegerType(), True),
    StructField("balloon_payment", IntegerType(), True),
    StructField("other_nonamortizing_features", IntegerType(), True),
    StructField("property_value", DecimalType(38, 2), True),
    StructField("construction_method", IntegerType(), True),
    StructField("occupancy_type", IntegerType(), True),
    StructField("manufactured_home_secured_property_type", IntegerType(), True),
    StructField("manufactured_home_land_property_interest", IntegerType(), True),
    StructField("total_units", IntegerType(), True),
    StructField("multifamily_affordable_units", IntegerType(), True),
    StructField("income", DecimalType(38, 2), True),
    StructField("debt_to_income_ratio", StringType(), True),
    StructField("applicant_credit_score_type", IntegerType(), True),
    StructField("co_applicant_credit_score_type", IntegerType(), True),
    StructField("applicant_ethnicity_1", StringType(), True),
    StructField("applicant_ethnicity_2", StringType(), True),
    StructField("applicant_ethnicity_3", StringType(), True),
    StructField("applicant_ethnicity_4", StringType(), True),
    StructField("applicant_ethnicity_5", StringType(), True),
    StructField("co_applicant_ethnicity_1", StringType(), True),
    StructField("co_applicant_ethnicity_2", StringType(), True),
    StructField("co_applicant_ethnicity_3", StringType(), True),
    StructField("co_applicant_ethnicity_4", StringType(), True),
    StructField("co_applicant_ethnicity_5", StringType(), True),
    StructField("applicant_ethnicity_observed", StringType(), True),
    StructField("co_applicant_ethnicity_observed", StringType(), True),
    StructField("applicant_race_1", StringType(), True),
    StructField("applicant_race_2", StringType(), True),
    StructField("applicant_race_3", StringType(), True),
    StructField("applicant_race_4", StringType(), True),
    StructField("applicant_race_5", StringType(), True),
    StructField("co_applicant_race_1", StringType(), True),
    StructField("co_applicant_race_2", StringType(), True),
    StructField("co_applicant_race_3", StringType(), True),
    StructField("co_applicant_race_4", StringType(), True),
    StructField("co_applicant_race_5", StringType(), True),
    StructField("applicant_race_observed", IntegerType(), True),
    StructField("co_applicant_race_observed", IntegerType(), True),
    StructField("applicant_sex", IntegerType(), True),
    StructField("co_applicant_sex", IntegerType(), True),
    StructField("applicant_sex_observed", IntegerType(), True),
    StructField("co_applicant_sex_observed", IntegerType(), True),
    StructField("applicant_age", StringType(), True),
    StructField("co_applicant_age", StringType(), True),
    StructField("applicant_age_above_62", StringType(), True),
    StructField("co_applicant_age_above_62", StringType(), True),
    StructField("submission_of_application", StringType(), True),
    StructField("initially_payable_to_institution", StringType(), True),
    StructField("aus_1", IntegerType(), True),
    StructField("aus_2", StringType(), True),
    StructField("aus_3", StringType(), True),
    StructField("aus_4", StringType(), True),
    StructField("aus_5", StringType(), True),
    StructField("denial_reason_1", StringType(), True),
    StructField("denial_reason_2", StringType(), True),
    StructField("denial_reason_3", StringType(), True),
    StructField("denial_reason_4", StringType(), True),
    StructField("tract_population", IntegerType(), True),
    StructField("tract_minority_population_percent", IntegerType(), True),
    StructField("ffiec_msa_md_median_family_income", IntegerType(), True),
    StructField("tract_to_msa_income_percentage", IntegerType(), True),
    StructField("tract_owner_occupied_units", IntegerType(), True),
    StructField("tract_one_to_four_family_homes", IntegerType(), True),
    StructField("tract_median_age_of_housing_units", IntegerType(), True)
])


def store_to_athena(y, folder_name, table_name, file_name, schema):
    
    s3_output_location = f's3://synergi-google-ads/lar_data_processed/{folder_name}_parq/'
    
    s3_path = f's3://synergi-google-ads/lar_data_processed/{folder_name}/{y}_{file_name}_processed.txt'
    
    # Read the text file from S3 into a DataFrame
    df = spark.read.option("delimiter", "|").option("header", "true").schema(schema).csv(s3_path)
    
    df.write.mode("append").format("parquet").saveAsTable(
        name=f"{database_name}.{table_name}_hb",
        path=s3_output_location
    )
    print(f"{folder_name}/{y} : DONE")


year_1 = ['2019', '2020', '2021', '2022']
for y in year_1:
    store_to_athena(y, 'first_link', 'HMDA_LAR', 'lar', schema_1)
# year_2 = ['2022', '2023']
# for y_2 in year_2:
#     store_to_athena(y_2, 'second_link', 'HMDA_MOD_LAR', 'combined_mlar',schema_2)

job.commit()