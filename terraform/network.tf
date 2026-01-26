resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/24"

  tags = {
    Name = "explane-vpc"
  }
}

resource "aws_subnet" "private" {
  for_each = {
    a = {
      block = "10.0.0.0/28",
      name  = "explane-private-a",
      az    = "us-east-1a"
    },
    f = {
      block = "10.0.0.160/28",
      name  = "explane-private-f",
      az    = "us-east-1f"
    }
  }

  vpc_id            = aws_vpc.main.id
  cidr_block        = each.value.block
  availability_zone = each.value.az

  tags = {
    Name = each.value.name
  }
}


resource "aws_subnet" "public" {

  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.0.16/28"
  availability_zone       = "us-east-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "explane-public-a"
  }
}

resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.main.id

  ingress {
    protocol  = -1
    self      = true
    from_port = 0
    to_port   = 0
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "explane-igw"
  }
}

resource "aws_default_route_table" "rtb" {
  default_route_table_id = aws_vpc.main.default_route_table_id
}

resource "aws_route" "default_internet" {
  route_table_id         = aws_default_route_table.rtb.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}
