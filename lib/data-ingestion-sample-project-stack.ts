import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3_deployment from 'aws-cdk-lib/aws-s3-deployment';  // Import the correct module
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as path from 'path';

export class DataIngestionSampleProjectStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Define S3 bucket for your data ingestion project
    const bucket = new s3.Bucket(this, 'DataIngestionSampleBucket', {
      bucketName: 'data-ingestion-sample-project',
      versioned: true,
    });

    // Upload the Python script to a specific S3 folder (e.g., /script/)
    const glueScriptUpload = new s3_deployment.BucketDeployment(this, 'DeployGlueScript', {
      destinationBucket: bucket,
      destinationKeyPrefix: 'scripts/',  // Folder in S3
      sources: [s3_deployment.Source.asset(path.join(__dirname, '../glue-scripts'))],  // Upload the entire folder
    });

    // Glue Job IAM Role
    const glueRole = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });

    // Attach required policies to Glue role
    glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));
    glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'));

    // Define the Glue job and reference the correct script location in S3
    new glue.CfnJob(this, 'DataIngestionSampleETLJob', {
      name: 'DataIngestionETLJob',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',  // Use 'glueetl' for Spark jobs, 'pythonshell' for Python shell jobs
        scriptLocation: `s3://${bucket.bucketName}/scripts/etl.py`, // Custom S3 path
        pythonVersion: '3',
      },
      glueVersion: '3.0',
      maxCapacity: 2,  // Number of DPUs (Data Processing Units)
    });
  }
}
