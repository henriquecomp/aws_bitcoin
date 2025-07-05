# Bloco de configuração do Terraform e do provedor AWS
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configura o provedor AWS para a região desejada
provider "aws" {
  region = "us-east-1"
}

# --- 1. Bucket S3 de Destino ---
resource "aws_s3_bucket" "bucket_destino" {
  bucket        = "henrique-bitcoin-2"
  force_destroy = true
}

# --- 2. IAM Role e Policy para o Firehose ---
resource "aws_iam_role" "firehose_role" {
  name               = "firehose-bitcoin-s3-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_policy" "firehose_policy" {
  name   = "firehose-bitcoin-s3-policy"
  policy = data.aws_iam_policy_document.firehose_permissions.json
}

data "aws_iam_policy_document" "firehose_permissions" {
  statement {
    sid    = "AllowS3Access"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject"
    ]
    resources = [
      aws_s3_bucket.bucket_destino.arn,
      "${aws_s3_bucket.bucket_destino.arn}/*"
    ]
  }
}

resource "aws_iam_role_policy_attachment" "firehose_attach" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.firehose_policy.arn
}

# --- 4. Recursos do Catálogo de Dados do AWS Glue (para o Athena) ---
resource "aws_glue_catalog_database" "btc_database" {
  name = "btc"
}

resource "aws_glue_catalog_table" "price_table" {
  name          = "price"
  database_name = aws_glue_catalog_database.btc_database.name
  table_type    = "EXTERNAL_TABLE"

  # partition_keys {
  #   name = "year"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "month"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "day"
  #   type = "string"
  # }
  # partition_keys {
  #   name = "hour"
  #   type = "string"
  # }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.bucket_destino.id}/data/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      name                  = "json"
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "date"
      type = "timestamp"
    }
  }

  parameters = {
    "EXTERNAL"       = "TRUE",
    "classification" = "json"
  }

  depends_on = [aws_glue_catalog_database.btc_database]
}

# --- 3. Kinesis Firehose Delivery Stream ---
resource "aws_kinesis_firehose_delivery_stream" "bitcoin_stream" {
  name        = "bitcoin_firehose"
  destination = "extended_s3"

  extended_s3_configuration {
    bucket_arn         = aws_s3_bucket.bucket_destino.arn
    role_arn           = aws_iam_role.firehose_role.arn
    buffering_size     = 64
    buffering_interval = 60
    #prefix             = "data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    #error_output_prefix = "erros/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:HH}/!{firehose:error-output-type}"
    prefix = "data/"
    error_output_prefix = "erros/"

    # Usa o processamento nativo do Firehose para adicionar o delimitador.
    processing_configuration {
      enabled = true
      processors {
        # --- CORREÇÃO FINAL APLICADA AQUI ---
        type = "AppendDelimiterToRecord" # 'Record' no singular
        parameters {
          parameter_name  = "Delimiter"
          parameter_value = "\\n" # O valor '\\n' representa o caractere de nova linha.
        }
      }
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.firehose_attach,
    aws_glue_catalog_table.price_table
  ]
}