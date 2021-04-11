terraform {
  backend "s3" {
    bucket = "nus-iss-equeue-terraform"
    key    = "lambda/activate-pending-staff/tfstate"
    region = "us-east-1"
  }
}