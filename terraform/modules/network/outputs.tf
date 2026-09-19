output "vpc_id" {
  value = aws_vpc.data_vpc.id
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "datalake_bucket_name" {
  value = aws_s3_bucket.datalake.bucket
}

output "datalake_bucket_arn" {
  value = aws_s3_bucket.datalake.arn
}
