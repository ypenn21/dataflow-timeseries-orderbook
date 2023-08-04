# Terraform for High Frequency Market Data Resources
This folder contains Terraform to create all resources used by the Beam/Dataflow components in this repository.
It will create the following:
* A BigQuery Dataset
* A BigQuery Table
* A BigQuery Data Transfer Job to populate the table from data in a GCS bucket that you will specify
* A PubSub Topic
* A BigTable Instance
* A BigTable Table

In order to use it, you will need to configure 2 additional files:
1) `backend.conf`: This should contain two variables:


* `bucket` - a GCS bucket that Terraform can use to store state
* `impersonate_service_account` - the service account identifier with permissions to create the IaC specified herein (in format `<service account name>`@`<project>`.iam.gserviceaccount.com)

2) `terraform.tfvars`: This needs to instantiate your particular parameters that are defined in [`variable.tf`](./variable.tf)