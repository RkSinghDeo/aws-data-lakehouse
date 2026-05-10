from aws_cdk import Duration, RemovalPolicy, aws_s3 as s3
from constructs import Construct


class MedallionBuckets(Construct):
    """Three-layer medallion S3 buckets with appropriate lifecycle policies."""

    bronze: s3.Bucket
    silver: s3.Bucket
    gold: s3.Bucket
    scripts: s3.Bucket

    def __init__(self, scope: Construct, construct_id: str, *, env_name: str) -> None:
        super().__init__(scope, construct_id)

        def _bucket(name: str, retain: bool = True, versioned: bool = False) -> s3.Bucket:
            return s3.Bucket(
                self,
                name,
                bucket_name=f"lakehouse-{name.lower()}-{env_name}",
                versioned=versioned,
                removal_policy=RemovalPolicy.RETAIN if retain else RemovalPolicy.DESTROY,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                encryption=s3.BucketEncryption.S3_MANAGED,
                enforce_ssl=True,
            )

        # Bronze: raw ingestion — keep indefinitely, move to IA after 30d
        self.bronze = _bucket("bronze", retain=True)
        self.bronze.add_lifecycle_rule(
            id="bronze-ia-transition",
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=Duration.days(30),
                )
            ],
        )

        # Silver: cleaned + validated — keep indefinitely
        self.silver = _bucket("silver", retain=True)

        # Gold: business aggregates — versioned for auditability
        self.gold = _bucket("gold", retain=True, versioned=True)

        # Scripts bucket for Glue job assets
        self.scripts = _bucket("scripts", retain=False)
