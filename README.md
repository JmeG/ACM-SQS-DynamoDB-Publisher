# AWS Application Cost Monitoring - DynamoDB Publisher
[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)
[![Build Status](https://travis-ci.org/ServerlessOpsIO/ACM-DynamoDB-Publisher.svg?branch=master)](https://travis-ci.org/ServerlessOpsIO/ACM-DynamoDB-Publisher)
[![License](https://img.shields.io/badge/License-BSD%202--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)

Application Cost Monitoring provides granular AWS spend tracking. This will write items from [AWS Application Cost Monitoring](https://github.com/ServerlessOpsIO/ApplicationCostMonitoring/) to DynamoDB.

**This requires [AWS Application Cost Monitoring](https://github.com/ServerlessOpsIO/ApplicationCostMonitoring/) to be deployed first.** This will subscribe to the SNS topic of that service and publish messages produced by it to S3.

![System Architecture](/diagram.png?raw=true "System Architecture")

## Deployment
This can be deployed using Serverless Framework or AWS Serverless Application Repository (AppRepo).  Due to limitations in AppRepo, you will need to setup the AWS Glue Crawler for Athena manually.

You must also deploy the [AWS Application Cost Monitoring](https://github.com/ServerlessOpsIO/ApplicationCostMonitoring/) service first.  That provides the billing report and ingestion pipeline.  This repository will subscribe to the message topic provided by that service.

### Serverless Framework
Clone of this repository by using [Serverless Framework](https://serverless.com/).

```
$ npm install -g serverless
$ npm install
$ serverless deploy -v
```

### AWS Serverless Application Repository
See the README and instructions at:

* 

## Usage
This service provides AWS cost and usage data in a DynamoDB table.  Either query the data manually or, better, deploy a front end to search the data.
