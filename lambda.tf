provider "aws" {
  region  = var.aws_region
}

#----------------------------------------------------------------------------
# Data
#----------------------------------------------------------------------------
# Get Logic Tier Subnets by CIDR block
data "aws_subnet" "logic_az1" {
  cidr_block = var.logic_az1_cidr_block
}

data "aws_subnet" "logic_az2" {
  cidr_block = var.logic_az2_cidr_block
}

data "aws_subnet" "logic_az3" {
  cidr_block = var.logic_az3_cidr_block
}

data "aws_iam_role" "iam_lambda" {
  name = var.role_name
}

data "aws_security_group" "lambda_sg" {
  name = var.sg_name
}

#----------------------------------------------------------------------------
# Lambda
#----------------------------------------------------------------------------
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_function
# https://docs.aws.amazon.com/lambda/latest/dg/lambda-java.html
resource "aws_lambda_function" "lambda" {
  filename      = var.filename
  function_name = var.function_name
  source_code_hash = filebase64sha512(var.filename)
  handler = var.handler
  role = data.aws_iam_role.iam_lambda.arn
  runtime = var.runtime

  environment {
    variables = {
      db_name = var.db_name
      password = var.password
      rds_endpoint = var.rds_endpoint
      username = var.username
    }
  }
  
  vpc_config {
    subnet_ids = [data.aws_subnet.logic_az1.id, data.aws_subnet.logic_az2.id, data.aws_subnet.logic_az3.id]
    security_group_ids = [data.aws_security_group.lambda_sg.id]
  }
}