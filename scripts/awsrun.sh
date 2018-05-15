#!/bin/bash

aws ec2 run-instances \
	--image-id "ami-41695a24" \
	--instance-type "r4.2xlarge" \
	--key-name "tp" \
	--monitoring '{"Enabled":false}' \
	--security-group-ids "sg-2131064a" \
	--instance-initiated-shutdown-behavior "stop" \
	--subnet-id "subnet-fec85696" \
	--count 5
