output "flink_execution_role_arn" {
  value = aws_iam_role.flink_execution_role.arn
}

output "flink_execution_role_name" {
  value = aws_iam_role.flink_execution_role.name
}
