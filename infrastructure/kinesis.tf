 resource "aws_kinesis_firehose_delivery_stream" "extended_s3_stream" {
   name        = "igti-kinesis-firehose-s3-stream"
   destination = "extended_s3"

   extended_s3_configuration {
     role_arn   = aws_iam_role.firehose_role.arn
     bucket_arn = aws_s3_bucket.stream.arn
     prefix = "firehose/"
     buffer_size = 5
     buffer_interval = 60
    
     cloudwatch_logging_options {
         enabled = true
         log_group_name = aws_cloudwatch_log_group.firehose.name
         log_stream_name = aws_cloudwatch_log_stream.firehose.name
     }
    
   }
 }


 