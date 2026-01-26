resource "aws_iam_policy" "emr_instance_profile_policy" {
  name        = "ExplaneEmrInstanceProfilePolicy"
  description = "Grants permissions to S3 buckets for the EMR cluster."

  policy = file("policies/emr-instance-profile-policy.json")
}

resource "aws_iam_role" "emr_instance_profile_role" {
  name = "ExplaneEmrInstanceProfile"

  assume_role_policy = file("policies/emr-instance-profile-assume-role-policy.json")
}

resource "aws_iam_instance_profile" "emr_instance_profile" {
  name = "ExplaneEmrInstanceProfile"
  role = aws_iam_role.emr_instance_profile_role.name
}

resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.emr_instance_profile_role.name
  policy_arn = aws_iam_policy.emr_instance_profile_policy.arn
}

resource "aws_iam_policy" "emr_service_role_policy" {
  name        = "ExplaneEmrServiceRolePolicy"
  description = "Grants permissions for AWS to manage EMR resources."

  policy = templatefile("policies/emr-service-role-policy.json", {
    account_id = data.aws_caller_identity.current.account_id,
    sg_id      = aws_default_security_group.default.id
    vpc_id     = aws_vpc.main.id
  })
}

resource "aws_iam_role" "emr_service_role" {
  name = "ExplaneEmrServiceRole"

  assume_role_policy = file("policies/emr-service-role-assume-role-policy.json")
}

resource "aws_iam_role_policy_attachment" "attach_custom" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = aws_iam_policy.emr_service_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "attach_default" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}



resource "aws_emr_cluster" "main" {
  name          = "ExplaneEmr"
  release_label = "emr-7.12.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id                         = aws_subnet.public.id
    emr_managed_master_security_group = aws_default_security_group.default.id
    emr_managed_slave_security_group  = aws_default_security_group.default.id
    instance_profile                  = aws_iam_instance_profile.emr_instance_profile.arn
  }

  master_instance_group {
    instance_type = "r6g.xlarge"
  }

  core_instance_group {
    instance_type  = "r6g.xlarge"
    instance_count = 1
  }

  tags = {
    for-use-with-amazon-emr-managed-policies = true
  }

  service_role = aws_iam_role.emr_service_role.arn
}
