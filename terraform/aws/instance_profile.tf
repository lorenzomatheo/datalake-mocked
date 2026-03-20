resource "aws_iam_role" "maggulake" {
  name = "maggu_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    billing_group = "datalake"
  }
}

resource "aws_iam_role_policy" "maggu_policy" {
  name = "maggu_policy"
  role = aws_iam_role.maggulake.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "maggulake" {
  name = "maggulake"
  role = aws_iam_role.maggulake.name
}