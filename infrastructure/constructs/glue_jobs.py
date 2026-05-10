from aws_cdk import Duration, aws_glue as glue, aws_iam as iam, aws_s3 as s3
from constructs import Construct


class MedallionGlueJobs(Construct):
    """
    Three PySpark Glue jobs implementing the medallion pipeline.

    Bronze → Silver: schema validation, type casting, deduplication
    Silver → Gold:   business aggregations, Iceberg MERGE INTO for CDC

    Each job uses a dedicated least-privilege IAM role and G.1X workers
    (4 vCPU / 16 GB) with auto-scaling enabled — no over-provisioned G.2X.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        bronze_bucket: s3.Bucket,
        silver_bucket: s3.Bucket,
        gold_bucket: s3.Bucket,
        glue_scripts_bucket: s3.Bucket,
        database_name: str,
    ) -> None:
        super().__init__(scope, construct_id)

        glue_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        for bucket in [bronze_bucket, silver_bucket, gold_bucket, glue_scripts_bucket]:
            bucket.grant_read_write(glue_role)

        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:*", "lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        common_args = {
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-job-insights": "true",
            "--enable-auto-scaling": "true",
            "--job-language": "python",
            "--BRONZE_BUCKET": bronze_bucket.bucket_name,
            "--SILVER_BUCKET": silver_bucket.bucket_name,
            "--GOLD_BUCKET": gold_bucket.bucket_name,
            "--DATABASE": database_name,
            "--ENV": env_name,
        }

        # Bronze ingestion job
        glue.CfnJob(
            self,
            "BronzeIngestion",
            name=f"lakehouse-bronze-ingestion-{env_name}",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{glue_scripts_bucket.bucket_name}/jobs/bronze_ingestion.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=5,
            timeout=60,
            default_arguments={
                **common_args,
                "--job-bookmark-option": "job-bookmark-enable",
                "--datalake-formats": "iceberg",
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
            description="Ingest raw S3 data into Bronze Iceberg tables",
        )

        # Silver transform job
        glue.CfnJob(
            self,
            "SilverTransform",
            name=f"lakehouse-silver-transform-{env_name}",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{glue_scripts_bucket.bucket_name}/jobs/silver_transform.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=5,
            timeout=90,
            default_arguments={
                **common_args,
                "--datalake-formats": "iceberg",
            },
            description="Validate, cast, and deduplicate Bronze → Silver Iceberg tables",
        )

        # Gold aggregation job
        glue.CfnJob(
            self,
            "GoldAggregation",
            name=f"lakehouse-gold-aggregation-{env_name}",
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{glue_scripts_bucket.bucket_name}/jobs/gold_aggregation.py",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=3,
            timeout=60,
            default_arguments={
                **common_args,
                "--datalake-formats": "iceberg",
            },
            description="Aggregate Silver → Gold business marts via Iceberg MERGE INTO",
        )
