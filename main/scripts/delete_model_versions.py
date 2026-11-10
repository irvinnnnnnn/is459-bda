"""Delete every version in a SageMaker model package group.

Called by Terraform at destroy time (main/infra/q1_training.tf): AWS refuses to delete a
group that still has versions. Kept as a script, not `python -c`, because Windows cmd.exe
mangles the nested quotes of an inline command.

Usage: python delete_model_versions.py <group> <region>
"""
import sys

import boto3


def main(group: str, region: str) -> None:
    sm = boto3.client("sagemaker", region_name=region)
    pages = sm.get_paginator("list_model_packages").paginate(ModelPackageGroupName=group)
    for page in pages:
        for pkg in page["ModelPackageSummaryList"]:
            arn = pkg["ModelPackageArn"]
            print(f"deleting {arn}")
            sm.delete_model_package(ModelPackageName=arn)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
