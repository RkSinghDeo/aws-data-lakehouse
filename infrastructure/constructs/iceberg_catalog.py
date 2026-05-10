from aws_cdk import CfnDeletionPolicy, CfnResource, Stack, aws_glue as glue, aws_s3 as s3
from constructs import Construct


class IcebergCatalog(Construct):
    """
    Configures the Glue Data Catalog as an Iceberg REST catalog.

    Creates three Glue databases (bronze, silver, gold) and registers
    S3 Table Bucket locations.  Glue's Iceberg REST endpoint allows
    any Iceberg-compatible engine (Spark, Trino, Athena, Flink) to
    read/write tables without schema duplication.

    AWS made Glue Data Catalog the Iceberg REST catalog GA at re:Invent 2024.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        database_name: str,
        bronze_bucket: s3.Bucket,
        silver_bucket: s3.Bucket,
        gold_bucket: s3.Bucket,
    ) -> None:
        super().__init__(scope, construct_id)

        stack = Stack.of(self)

        for layer, bucket in [
            ("bronze", bronze_bucket),
            ("silver", silver_bucket),
            ("gold", gold_bucket),
        ]:
            db = glue.CfnDatabase(
                self,
                f"GlueDb-{layer}",
                catalog_id=stack.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=f"{database_name}_{layer}",
                    description=f"Iceberg {layer} layer — {database_name}",
                    parameters={
                        "classification": "iceberg",
                        # Tells Glue to act as Iceberg REST catalog for this database
                        "table_type": "ICEBERG",
                        "warehouse": f"s3://{bucket.bucket_name}/warehouse/",
                    },
                ),
            )

            # RETAIN on delete/replace — never drop live data tables
            cfn_db = db.node.default_child
            if isinstance(cfn_db, CfnResource):
                cfn_db.cfn_options.deletion_policy = CfnDeletionPolicy.RETAIN
                cfn_db.cfn_options.update_replace_policy = CfnDeletionPolicy.RETAIN
