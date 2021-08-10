variable "base_bucket_name" {
    default = "datalake-igti-tf"

}

variable "ambiente" {
    default = "producao"

}

variable "numero_conta" {
    default = "289405200928"

}

variable "aws_region" {
    default = "sa-east-1"
}

variable "lambda_function_name" {
    default = "IGTIexecutaEMR"
}

variable "key_pair_name" {
    default = "jeferson=igti-teste"
}

variable "airflow_subnet_id" {
    default = "subnet-0c2d4f69"
}

variable "vpc_id"{
    default = "vpc-f60e6393"
}





