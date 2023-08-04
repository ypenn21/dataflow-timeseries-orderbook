/*
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

variable "sa" {
    type = string
    description = "Terraform service account to impersonate"
}

variable "project" {
    type = string
    description = "GCP Project in which to create resources"
}

variable "region" {
    type = string
    default = "us-central1"
    description = "GCP Region to use for regional resources"
}

variable "bq_dataset_name" {
    type = string
    description = "BigQuery Dataset to create"
}

variable "bq_table_name" {
    type = string
    description = "BigQuery Table to create"
}

variable "bq_schema_file" {
    type = string
    description = "The schema file to use for the BigQuery table"
}

variable "sample_data_gcs_path" {
    type = string
    description = "location of GCS data to load into bq_table"
}

variable "sample_data_format" {
    type = string
    default = "PARQUET"
    description = "The file format of the sample data in GCS"

    validation {
        condition = contains(["CSV","JSON","AVRO","PARQUET","ORC","THRIFT"],var.sample_data_format)
        error_message = "Allowed values are CSV, JSON, AVRO, PARQUET, ORC, THRIFT"
    }
}

variable "pubsub_topic_name" {
    type = string
    description = "PubSub topic on which to put the BigQuery data"
}

variable "bt_instance_name" {
    type = string
    description = "BigTable instance name to write to"
}

variable "bt_cluster_name" {
    type = string
    description = "BigTable cluster name"
}

variable "bt_num_nodes" {
    type = number
    description = "number of nodes in the BigTable instance"
}

variable "bt_table_name" {
    type = string
    description = "BigTable table name to write to"
}