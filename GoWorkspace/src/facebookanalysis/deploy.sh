#!/bin/bash

go get ./
go build wc.go
aws s3 cp wc s3://facebook-analysis/bin/wc
