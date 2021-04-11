terraform {
  backend "s3" {
    bucket = "nus-iss-equeue-terraform"
    key    = "lambda/smartSearch/tfstate"
    region = "us-east-1"
  }
}
