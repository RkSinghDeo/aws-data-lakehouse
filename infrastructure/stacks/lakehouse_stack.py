from aws_cdk import (
    CfnDeletionPolicy,
    CfnResource,
    Duration,
    RemovalPolicy,
    Stack,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct

from infrastructure.constructs.iceberg_catalog import IcebergCatalog
from infrastructure.constructs.glue_jobs import MedallionGlueJobs
from infrastructure.constructs.medallion_buckets import MedallionBuckets


class LakehouseStack(Stack):
    """
    Bronze / Silver / Gold lakehouse on S3 Iceberg tables.

    Architecture:
    - S3 Table Buckets: bronze/, silver/, gold/ with appropriate lifecycle rules
    - Glue Data Catalog configured as an Iceberg REST catalog
    - PySpark Glue jobs for each medallion layer
    - GitHub Actions OIDC role for CI/CD deployments
    - IAM least-privilege: each Glue job has its own scoped role
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        env_name = self.node.try_get_context("env") or "dev"

        # ── Medallion S3 buckets ──────────────────────────────────────────────
        buckets = MedallionBuckets(self, "MedallionBuckets", env_name=env_name)

        # ── Glue Data Catalog as Iceberg REST catalog ─────────────────────────
        catalog = IcebergCatalog(
            self,
            "IcebergCatalog",
            database_name=f"lakehouse_{env_name}",
            bronze_bucket=buckets.bronze,
            silver_bucket=buckets.silver,
            gold_bucket=buckets.gold,
        )

        # ── PySpark Glue jobs (Bronze → Silver → Gold) ────────────────────────
        glue_jobs = MedallionGlueJobs(
            self,
            "MedallionGlueJobs",
            env_name=env_name,
            bronze_bucket=buckets.bronze,
            silver_bucket=buckets.silver,
            gold_bucket=buckets.gold,
            glue_scripts_bucket=buckets.scripts,
            database_name=f"lakehouse_{env_name}",
        )

        # ── GitHub Actions OIDC role (CI/CD deployments) ──────────────────────
        gh_provider = iam.OpenIdConnectProvider(
            self,
            "GitHubOIDC",
            url="https://token.actions.githubusercontent.com",
            client_ids=["sts.amazonaws.com"],
        )

        iam.Role(
            self,
            "GitHubActionsDeployRole",
            assumed_by=iam.WebIdentityPrincipal(
                gh_provider.open_id_connect_provider_arn,
                conditions={
                    "StringEquals": {
                        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
                    },
                    "StringLike": {
                        "token.actions.githubusercontent.com:sub": "repo:RkSinghDeo/aws-data-lakehouse:*"
                    },
                },
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("PowerUserAccess")
            ],
            description="GitHub Actions OIDC deploy role for aws-data-lakehouse",
        )
