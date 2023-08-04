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

terraform {
	required_providers{
		google = {
			source="hashicorp/google"
			version="4.51.0"
		}
  	}

	backend "gcs" {}
}

provider "google" {
	alias = "impersonation"
	scopes = [
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/userinfo.email",
	]
}

data "google_service_account_access_token" "default" {
	provider               	= google.impersonation
	target_service_account 	= var.sa
	scopes                 	= ["userinfo-email", "cloud-platform"]
	lifetime               	= "1200s"
}

provider "google" {
	project 				= var.project
	region					= var.region
	access_token			= data.google_service_account_access_token.default.access_token
	request_timeout 		= "60s"
}

resource "google_pubsub_topic" "pubsub_topic" {
	name					= var.pubsub_topic_name
}

resource "google_pubsub_subscription" "pubsub_subscription" {
	name					= format("%s-%s",var.pubsub_topic_name,"sub")
	topic					= google_pubsub_topic.pubsub_topic.name
	message_retention_duration = "1200s"
	retain_acked_messages 	= false
	ack_deadline_seconds 	= 20
}

resource "google_bigquery_dataset" "bq_dataset" {
	dataset_id				= var.bq_dataset_name
	location 				= var.region
}

resource "google_bigquery_table" "bq_table" {
	dataset_id 				= google_bigquery_dataset.bq_dataset.dataset_id
	table_id 				= var.bq_table_name
	schema 					= file(var.bq_schema_file)
	deletion_protection 	= false
}

resource "google_bigquery_data_transfer_config" "bq_data_xfer" {
	depends_on 				= [google_bigquery_table.bq_table]
	display_name 			= "Populate sample data"
	location 				= var.region
	data_source_id 			= "google_cloud_storage"
	destination_dataset_id 	= google_bigquery_dataset.bq_dataset.dataset_id
	params 					= {
		destination_table_name_template	= google_bigquery_table.bq_table.table_id
		data_path_template	= var.sample_data_gcs_path
		write_disposition	= "APPEND"
		file_format			= "PARQUET"
	} 
}

resource "google_bigtable_instance" "bt_instance" {
	name 					= var.bt_instance_name
	deletion_protection 	= false

	cluster {
		cluster_id			= var.bt_cluster_name
		num_nodes 			= var.bt_num_nodes
		storage_type 		= "SSD"
		zone 				= format("%s-%s",var.region,"a")
	}
}

resource "google_bigtable_table" "bt_table" {
	name					= var.bt_table_name
	instance_name 			= google_bigtable_instance.bt_instance.name
	column_family {
		family 				= "cf1"
	}
}