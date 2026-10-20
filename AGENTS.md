# Repository Guidelines

## Project Structure & Module Organization

`main/` holds the current implementation: shared Python utilities in `main/common/`, Q1 streaming and training code in `main/Q1_files/`, Q2 batch and app code in `main/Q2_files/`, producer tools in `main/producer/`, and lifecycle/build scripts in `main/scripts/`. Terraform for the AWS stack is in `main/infra/`; all `.tf` files there form one Terraform root. `main/tests/` contains the pytest suite. `zArchive/` contains earlier versions for reference. `PIPELINE_SPEC.md` is the design source of truth; `README.md` documents setup and demos.

## Build, Test, and Development Commands

Run commands from the repository root unless noted:

```bash
python main/scripts/build.py                 # package code for Terraform
cd main/infra && terraform init              # initialize providers
terraform plan                               # preview AWS changes
terraform apply                              # create or update the stack
terraform destroy                            # remove Terraform-managed resources
cd ../..
python main/scripts/pipeline.py status       # inspect the running pipeline
python -m pytest main/tests                  # run tests
```

Use `python main/scripts/pipeline.py start|stop` to start or stop Flink without destroying the stack. Check the README before setup, replay, reset, or destroy; these operations have different effects on AWS resources and stored data.

## Coding Style & Naming Conventions

Use four-space indentation and descriptive `snake_case` names in Python. Keep Q1 and Q2 logic in their respective directories, and shared behavior in `main/common/`. Keep Terraform resources in the existing `main/infra/` root so a single state manages the stack. Update `PIPELINE_SPEC.md` when changing agreed architecture or pipeline behavior.

## Testing Guidelines

Tests use pytest and live in `main/tests/test_*.py`. Run `python -m pytest main/tests`; add or update focused tests alongside changes to shared logic, producer behavior, or Q1 scoring. AWS integration checks require configured credentials and deployed resources, so do not treat local tests as proof of a successful AWS deployment.

## Commits, Pull Requests, and Configuration

Recent history has no consistent subject convention. Use a concise, specific imperative subject, such as `Add Q2 score validation`. Pull requests should summarize behavior changes, list manual checks, and include screenshots for app changes. Never commit AWS credentials, Terraform state, generated build artifacts, or private datasets. Use `terraform.tfvars` for local overrides and keep reusable defaults in `terraform.tfvars.example`.
