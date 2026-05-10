#!/usr/bin/env python3
import aws_cdk as cdk
from infrastructure.stacks.lakehouse_stack import LakehouseStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

LakehouseStack(app, "AwsDataLakehouseStack", env=env)

app.synth()
