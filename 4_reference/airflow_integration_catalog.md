# The Airflow Integration Catalog

**What Apache Airflow can actually talk to, and which of it you will really use.**

Every class name in the AWS chapters was read out of the installed package, not
recalled from documentation. Verified against:

| component | version |
|---|---|
| Apache Airflow | 3.3.1 |
| `apache-airflow-providers-amazon` | 9.34.0 — **268** operator/sensor/transfer classes across 87 modules |
| `apache-airflow-providers-standard` | 1.17.0 |
| `apache-airflow-providers-common-sql` | 2.1.0 |

Companion to atlas 8 (what Airflow is and is not) and atlas 9 (the 30 building
blocks). Where atlas 9 teaches the thirty things you use daily, this catalogue
is the map of everything else, so that when a new requirement lands you know
whether an operator already exists.

---

## 1 · The four kinds of integration

Everything Airflow can do to an external system arrives as one of four things.
Knowing which you need cuts the search enormously.

| kind | question it answers | naming | example |
|---|---|---|---|
| **Operator** | "do a thing and tell me when it is finished" | `*Operator` | `EmrServerlessStartJobOperator` |
| **Sensor** | "wait until a condition is true" | `*Sensor` | `S3KeySensor` |
| **Transfer** | "move data from A to B" | `XToYOperator` | `RedshiftToS3Operator` |
| **Hook** | "give me an authenticated client and get out of my way" | `*Hook` | `AthenaHook`, `S3Hook` |

**A hook is the escape hatch and it is not a fallback.** Every operator is a
thin wrapper over a hook. When no operator fits, a five-line `@task` using the
hook is idiomatic Airflow, not a workaround. Chapter 15 covers this.

Two cross-cutting properties matter more than any individual class:

- **`deferrable=True`** — the wait moves to the triggerer process and the task
  holds no worker slot. Available on most AWS operators that wait. On a busy
  deployment this is the difference between 16 concurrent jobs and thousands.
- **`.partial() / .expand()`** — dynamic task mapping works on *any* operator,
  not just `@task`. One definition, N instances decided at run time.

---

## 2 · The decision table

Start here. The mistake is not picking the wrong operator, it is reaching for
Airflow at all when a simpler trigger would do.

| you need to… | reach for | not |
|---|---|---|
| run one job on a schedule, nothing else | EventBridge Scheduler | a whole Airflow deployment |
| run Spark on a managed cluster you own | `EmrCreateJobFlowOperator` + steps | Glue, if you need bootstrap actions |
| run Spark with no cluster lifecycle | `EmrServerlessStartJobOperator` | EMR on EC2 |
| run Spark on the platform's shared k8s | `EmrContainerOperator` (EMR on EKS) | a second EMR estate |
| run a small managed ETL script | `GlueJobOperator` | EMR, which starts slower |
| run any container | `EcsRunTaskOperator`, `BatchOperator`, `KubernetesPodOperator` | wrapping it in a Lambda |
| run a query and use the answer | `AthenaOperator` / `RedshiftDataOperator` + a task that reads it | the operator alone — see chapter 6 |
| assert a data condition | `SQLCheckOperator` family, Glue Data Quality | a bespoke Python assertion |
| wait for a file | `S3KeySensor` (deferrable) | a `time.sleep` loop |
| wait for another DAG | an **Asset** | `ExternalTaskSensor`, usually |
| move bytes between two stores | a transfer operator | reading them into the worker |
| move a lot of bytes | a Spark/Glue job | any transfer operator |
| call a REST API | `HttpOperator` or `@task` + `requests` | a shell `curl` |
| ask a human to approve | `ApprovalOperator` (HITL, chapter 14) | an email and a paused DAG |
| do something with no operator | the **Hook** | inventing a plugin |

---

## 3 · Core building blocks — `apache-airflow-providers-standard`

Installed with Airflow. No external system required. These are the operators
that show up in every DAG regardless of what the DAG actually does.

| class | module | what it does |
|---|---|---|
| `BashOperator` | `operators.bash` | run a shell command — the universal escape hatch |
| `PythonOperator` | `operators.python` | run a callable (pre-TaskFlow style) |
| `PythonVirtualenvOperator` | `operators.python` | run a callable in a throwaway venv with its own deps |
| `ExternalPythonOperator` | `operators.python` | run a callable in a **pre-built** interpreter — the fast version of the above |
| `BranchPythonOperator` | `operators.python` | choose a downstream path (`@task.branch` is the modern form) |
| `BranchExternalPythonOperator` | `operators.python` | branch, decided in another interpreter |
| `BranchPythonVirtualenvOperator` | `operators.python` | branch, decided in a temporary venv |
| `ShortCircuitOperator` | `operators.python` | falsy return skips everything downstream |
| `EmptyOperator` | `operators.empty` | a no-op node: join points, named markers |
| `LatestOnlyOperator` | `operators.latest_only` | skip everything downstream on a backfill run |
| `TriggerDagRunOperator` | `operators.trigger_dagrun` | start another DAG by id, optionally waiting |
| `BranchDateTimeOperator` | `operators.datetime` | branch on a time-of-day window |
| `BranchDayOfWeekOperator` | `operators.weekday` | branch on the day of the week |
| `ApprovalOperator` | `operators.hitl` | **human-in-the-loop**: pause until a person approves |
| `HITLOperator` / `HITLEntryOperator` | `operators.hitl` | ask a human for a value or a decision |
| `HITLBranchOperator` | `operators.hitl` | let a human pick the branch |
| `SmoothOperator` | `operators.smooth` | an easter egg. It plays Santana. Genuinely in the provider. |

**Sensors in the same provider:**

| class | waits for |
|---|---|
| `FileSensor` | a path on a filesystem (needs an `fs`-type Connection — Airflow 3 seeds none) |
| `ExternalTaskSensor` | a task in another DAG at the same logical date |
| `DateTimeSensor` / `DateTimeSensorAsync` | a wall-clock moment |
| `TimeSensor` / `TimeSensorAsync` | a time of day |
| `TimeDeltaSensor` / `TimeDeltaSensorAsync` / `WaitSensor` | a duration after the data interval |
| `DayOfWeekSensor` | a given weekday |
| `BashSensor` | a shell command to exit 0 |
| `PythonSensor` | a callable to return truthy |

> The `*Async` suffix on the standard sensors and `deferrable=True` on the AWS
> ones are the same idea: the triggerer waits, not a worker.

---

## 4 · SQL against anything — `apache-airflow-providers-common-sql`

These work against **any** database that has a DBAPI hook — Postgres, MySQL,
Snowflake, Redshift, Trino, Athena, Databricks, Oracle, MSSQL. You change
`conn_id`, not the operator. This is the most under-used provider in the
ecosystem.

| class | what it does |
|---|---|
| `SQLExecuteQueryOperator` | run SQL. Replaces the per-database `PostgresOperator`, `SnowflakeOperator`, etc., all of which are deprecated |
| `SQLCheckOperator` | run a query; fail the task if any value in the first row is falsy |
| `SQLValueCheckOperator` | compare a single result against an expected value with tolerance |
| `SQLIntervalCheckOperator` | compare today's metrics against a previous interval — drift detection in one operator |
| `SQLThresholdCheckOperator` | assert a value lies between a min and max, either of which may itself be a query |
| `SQLColumnCheckOperator` | declarative per-column assertions: null_check, distinct_check, unique_check, min, max |
| `SQLTableCheckOperator` | declarative table-level assertions written as boolean SQL expressions |
| `BranchSQLOperator` | branch on the result of a query |
| `SQLBulkLoadOperator` | bulk-load a file into a table |
| `SQLInsertRowsOperator` | insert rows from a Python iterable |
| `SqlSensor` | wait until a query returns a truthy result |
| `AnalyticsOperator` | run analytics-shaped SQL with result handling |

```python
# Declarative data quality with no custom code, on any SQL engine.
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator

SQLColumnCheckOperator(
    task_id="check_cdr_quality",
    conn_id="redshift_default",
    table="silver.cdr_events",
    column_mapping={
        "cdr_id":        {"null_check": {"equal_to": 0}, "unique_check": {"equal_to": 0}},
        "duration_sec":  {"min": {"geq_to": 0}, "max": {"leq_to": 86400}},
        "subscriber_id": {"null_check": {"equal_to": 0}},
    },
)
```

---

## 5 · AWS compute — the three EMR models and everything beside them

### The EMR family in full

| deployment model | operators | sensors |
|---|---|---|
| **EMR on EC2** | `EmrCreateJobFlowOperator`, `EmrAddStepsOperator`, `EmrModifyClusterOperator`, `EmrTerminateJobFlowOperator` | `EmrJobFlowSensor`, `EmrStepSensor` |
| **EMR Serverless** | `EmrServerlessCreateApplicationOperator`, `EmrServerlessStartJobOperator`, `EmrServerlessStopApplicationOperator`, `EmrServerlessDeleteApplicationOperator` | `EmrServerlessJobSensor`, `EmrServerlessApplicationSensor` |
| **EMR on EKS** | `EmrContainerOperator`, `EmrEksCreateClusterOperator` | `EmrContainerSensor` |
| **EMR Studio notebooks** | `EmrStartNotebookExecutionOperator`, `EmrStopNotebookExecutionOperator` | `EmrNotebookExecutionSensor` |

### Choosing between them

| your workload | model | why |
|---|---|---|
| 40 minutes, 60 nodes, custom native library, heavy shuffle | **EC2** | bootstrap actions and custom AMIs exist nowhere else |
| 8 minutes, bursty, every 15 minutes | **Serverless** | a transient cluster would spend 5 of those minutes booting |
| small, frequent, must share the platform's k8s IAM and network | **EKS** | reuses a cluster and a security posture you already run |
| someone wants to run a notebook on a schedule | **Studio notebooks** | it is a real, supported thing |

> **The terminate trap.** `EmrTerminateJobFlowOperator` must carry
> `trigger_rule="all_done"`. With the default `all_success` a failed step
> leaves the cluster running until a human notices. This is the most expensive
> single mistake in the whole catalogue.

### The rest of AWS compute

| service | operators | sensors |
|---|---|---|
| **AWS Glue** | `GlueJobOperator`, `GlueDataQualityOperator`, `GlueDataQualityRuleSetEvaluationRunOperator`, `GlueDataQualityRuleRecommendationRunOperator` | `GlueJobSensor`, `GlueDataQualityRuleSetEvaluationRunSensor`, `GlueDataQualityRuleRecommendationRunSensor` |
| **Glue crawlers** | `GlueCrawlerOperator`, `GlueCrawlerCreateOperator`, `GlueCrawlerRunOperator`, `GlueCrawlerUpdateOperator`, `GlueCrawlerDeleteOperator` | `GlueCrawlerSensor` |
| **Glue Data Catalog** | `GlueCatalogCreateDatabaseOperator`, `GlueCatalogDeleteDatabaseOperator`, `GlueCatalogCreateTableOperator`, `GlueCatalogDeleteTableOperator`, `GlueCatalogCreatePartitionOperator`, `GlueCatalogBatchDeletePartitionOperator` | `GlueCatalogPartitionSensor` |
| **Glue DataBrew** | `GlueDataBrewStartJobOperator` | — |
| **AWS Batch** | `BatchOperator`, `BatchCreateComputeEnvironmentOperator` | `BatchSensor`, `BatchComputeEnvironmentSensor`, `BatchJobQueueSensor` |
| **ECS / Fargate** | `EcsRunTaskOperator`, `EcsCreateClusterOperator`, `EcsDeleteClusterOperator`, `EcsRegisterTaskDefinitionOperator`, `EcsDeregisterTaskDefinitionOperator` | `EcsTaskStateSensor`, `EcsClusterStateSensor`, `EcsTaskDefinitionStateSensor` |
| **EKS** | `EksCreateClusterOperator`, `EksCreateNodegroupOperator`, `EksCreateFargateProfileOperator`, `EksPodOperator`, and the three matching delete operators | `EksClusterStateSensor`, `EksNodegroupStateSensor`, `EksFargateProfileStateSensor` |
| **Lambda** | `LambdaCreateFunctionOperator`, `LambdaInvokeFunctionOperator` | `LambdaFunctionStateSensor` |
| **EC2** | `EC2CreateInstanceOperator`, `EC2StartInstanceOperator`, `EC2StopInstanceOperator`, `EC2RebootInstanceOperator`, `EC2HibernateInstanceOperator`, `EC2TerminateInstanceOperator` | `EC2InstanceStateSensor` |

---

## 6 · AWS query and warehouse

| service | operators | sensors | transfers |
|---|---|---|---|
| **Athena** | `AthenaOperator` | `AthenaSensor` | — |
| **Redshift (data)** | `RedshiftDataOperator` | — | `S3ToRedshiftOperator`, `RedshiftToS3Operator` |
| **Redshift (cluster)** | `RedshiftCreateClusterOperator`, `RedshiftPauseClusterOperator`, `RedshiftResumeClusterOperator`, `RedshiftDeleteClusterOperator`, `RedshiftCreateClusterSnapshotOperator`, `RedshiftDeleteClusterSnapshotOperator` | `RedshiftClusterSensor` | — |
| **RDS** | `RdsCreateDbInstanceOperator`, `RdsStartDbOperator`, `RdsStopDbOperator`, `RdsDeleteDbInstanceOperator`, `RdsCreateDbSnapshotOperator`, `RdsCopyDbSnapshotOperator`, `RdsDeleteDbSnapshotOperator`, `RdsStartExportTaskOperator`, `RdsCancelExportTaskOperator`, `RdsCreateEventSubscriptionOperator`, `RdsDeleteEventSubscriptionOperator` | `RdsDbSensor`, `RdsSnapshotExistenceSensor`, `RdsExportTaskExistenceSensor` | — |
| **DynamoDB** | — | `DynamoDBValueSensor` | `DynamoDBToS3Operator`, `S3ToDynamoDBOperator` |
| **Neptune** | `NeptuneStartDbClusterOperator`, `NeptuneStopDbClusterOperator` | — | — |
| **Neptune Analytics** | `NeptuneCreateGraphOperator`, `NeptuneCreateGraphWithImportOperator`, `NeptuneStartImportTaskOperator`, `NeptuneCancelImportTaskOperator`, `NeptuneCreatePrivateGraphEndpointOperator`, `NeptuneDeleteGraphOperator`, `NeptuneDeletePrivateGraphEndpointOperator` | — | — |
| **OpenSearch Serverless** | `OpenSearchServerlessCreateCollectionOperator` | `OpenSearchServerlessCollectionActiveSensor` | — |
| **QuickSight** | `QuickSightCreateIngestionOperator` | `QuickSightSensor` | — |

> **The green-but-wrong trap.** `AthenaOperator` and `RedshiftDataOperator`
> succeed when the *query* succeeds. A query returning zero rows, or returning
> a number that should have alarmed you, is still a success. Anything that must
> fail on a *value* needs a task that reads the result and raises:

```python
@task
def assert_rows_landed(ds: str) -> int:
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook
    hook = AthenaHook(aws_conn_id="aws_default")
    qid = hook.run_query(
        query=f"SELECT count(*) FROM cdr_events WHERE event_date = DATE '{ds}'",
        query_context={"Database": "silver"},
        result_configuration={"OutputLocation": "s3://bucket/athena/"},
    )
    if hook.poll_query_status(qid, max_polling_attempts=60) != "SUCCEEDED":
        raise RuntimeError(f"query {qid} failed")
    rows = hook.get_query_results(qid)["ResultSet"]["Rows"]
    n = int(rows[1]["Data"][0]["VarCharValue"])
    if n == 0:
        raise ValueError(f"no rows for {ds}")     # THIS is the actual check
    return n
```

---

## 7 · AWS storage and transfer

### S3 and the newer table-shaped storage

| group | classes |
|---|---|
| **S3 objects** | `S3CreateObjectOperator`, `S3ReadObjectOperator`, `S3CopyObjectOperator`, `S3CopyPrefixOperator`, `S3DeleteObjectsOperator`, `S3FileTransformOperator`, `S3ListOperator`, `S3ListPrefixesOperator` |
| **S3 buckets** | `S3CreateBucketOperator`, `S3DeleteBucketOperator`, `S3PutBucketTaggingOperator`, `S3GetBucketTaggingOperator`, `S3DeleteBucketTaggingOperator` |
| **S3 sensors** | `S3KeySensor`, `S3KeysUnchangedSensor` |
| **S3 Tables** (managed Iceberg — atlas 5) | `S3TablesCreateTableBucketOperator`, `S3TablesCreateNamespaceOperator`, `S3TablesCreateTableOperator`, `S3TablesRenameTableOperator`, `S3TablesDeleteTableOperator`, `S3TablesDeleteNamespaceOperator`, `S3TablesDeleteTableBucketOperator`, `S3TablesPutTableBucketPolicyOperator`, `S3TablesDeleteTableBucketPolicyOperator` |
| **S3 Vectors** | `S3VectorsCreateVectorBucketOperator`, `S3VectorsCreateIndexOperator`, `S3VectorsPutVectorsOperator`, `S3VectorsQueryVectorsOperator`, `S3VectorsDeleteIndexOperator`, `S3VectorsDeleteVectorBucketOperator` |
| **Glacier** | `GlacierCreateJobOperator`, `GlacierUploadArchiveOperator`, sensor `GlacierJobOperationSensor` |

> `S3KeysUnchangedSensor` is the one people miss. It waits until the number of
> objects under a prefix **stops changing** — the right answer when a producer
> writes many files and does not write a `_SUCCESS` marker.

### Transfer operators, in full

Every transfer in the amazon provider. All of them stream through the Airflow
worker, so they are for **metadata-scale** movement — thousands of rows, not
billions. Moving a partition is a Spark job.

| from → to | class |
|---|---|
| S3 → Redshift | `S3ToRedshiftOperator` |
| Redshift → S3 | `RedshiftToS3Operator` |
| S3 → DynamoDB | `S3ToDynamoDBOperator` |
| DynamoDB → S3 | `DynamoDBToS3Operator` |
| S3 → SQL | `S3ToSqlOperator` |
| SQL → S3 | `SqlToS3Operator` |
| S3 → SFTP / SFTP → S3 | `S3ToSFTPOperator`, `SFTPToS3Operator` |
| S3 → FTP / FTP → S3 | `S3ToFTPOperator`, `FTPToS3Operator` |
| local → S3 | `LocalFilesystemToS3Operator` |
| HTTP → S3 | `HttpToS3Operator` |
| GCS → S3 | `GCSToS3Operator` |
| Azure Blob → S3 | `AzureBlobStorageToS3Operator` |
| Google API → S3 | `GoogleApiToS3Operator` |
| Glacier → GCS | `GlacierToGCSOperator` |

---

## 8 · AWS messaging, eventing and data movement

| service | classes |
|---|---|
| **SNS** | `SnsPublishOperator` |
| **SQS** | `SqsPublishOperator`, `SqsSensor` |
| **SES** | `SesEmailOperator` |
| **EventBridge** | `EventBridgePutEventsOperator`, `EventBridgePutRuleOperator`, `EventBridgeEnableRuleOperator`, `EventBridgeDisableRuleOperator` |
| **Step Functions** | `StepFunctionStartExecutionOperator`, `StepFunctionGetExecutionOutputOperator`, `StepFunctionExecutionSensor` |
| **DMS** (database migration / CDC) | `DmsCreateTaskOperator`, `DmsStartTaskOperator`, `DmsStopTaskOperator`, `DmsModifyTaskOperator`, `DmsDeleteTaskOperator`, `DmsDescribeTasksOperator`, `DmsReloadTablesOperator`, plus the serverless replication set: `DmsCreateReplicationConfigOperator`, `DmsStartReplicationOperator`, `DmsStopReplicationOperator`, `DmsDescribeReplicationsOperator`, `DmsDeleteReplicationConfigOperator`; sensor `DmsTaskCompletedSensor` |
| **DataSync** | `DataSyncOperator` |
| **AppFlow** (SaaS → AWS) | `AppflowRunOperator`, `AppflowRunFullOperator`, `AppflowRunDailyOperator`, `AppflowRunBeforeOperator`, `AppflowRunAfterOperator`, `AppflowRecordsShortCircuitOperator` |
| **Kinesis Data Analytics v2** | `KinesisAnalyticsV2CreateApplicationOperator`, `KinesisAnalyticsV2StartApplicationOperator`, `KinesisAnalyticsV2StopApplicationOperator`; sensors `KinesisAnalyticsV2StartApplicationCompletedSensor`, `KinesisAnalyticsV2StopApplicationCompletedSensor` |
| **MWAA** | `MwaaTriggerDagRunOperator`, `MwaaDagRunSensor`, `MwaaTaskSensor` — **Airflow triggering another Airflow**, the standard pattern for cross-team, cross-account orchestration |
| **MWAA Serverless** | `MwaaServerlessCreateWorkflowOperator`, `MwaaServerlessStartWorkflowRunOperator`, `MwaaServerlessStopWorkflowRunOperator`, `MwaaServerlessUpdateWorkflowOperator`, `MwaaServerlessDeleteWorkflowOperator`; sensor `MwaaServerlessWorkflowRunSensor` |

> A dispute, an approval chain or anything measured in *human* time belongs in
> Step Functions, not in an Airflow task. Start it with
> `StepFunctionStartExecutionOperator` and stop caring. Never hold an Airflow
> task open for weeks.

---

## 9 · AWS machine learning and AI

| service | operators | sensors |
|---|---|---|
| **SageMaker jobs** | `SageMakerProcessingOperator`, `SageMakerTrainingOperator`, `SageMakerTransformOperator`, `SageMakerTuningOperator`, `SageMakerAutoMLOperator` | `SageMakerProcessingSensor`, `SageMakerTrainingSensor`, `SageMakerTransformSensor`, `SageMakerTuningSensor`, `SageMakerAutoMLSensor` |
| **SageMaker models** | `SageMakerModelOperator`, `SageMakerEndpointConfigOperator`, `SageMakerEndpointOperator`, `SageMakerRegisterModelVersionOperator`, `SageMakerDeleteModelOperator` | `SageMakerEndpointSensor` |
| **SageMaker pipelines** | `SageMakerStartPipelineOperator`, `SageMakerStopPipelineOperator`, `SageMakerConditionOperator`, `SageMakerCreateExperimentOperator` | `SageMakerPipelineSensor` |
| **SageMaker notebooks** | `SageMakerCreateNotebookOperator`, `SageMakerStartNoteBookOperator`, `SageMakerStopNotebookOperator`, `SageMakerDeleteNotebookOperator`, `SageMakerNotebookOperator` (Unified Studio) | `SageMakerNotebookSensor` |
| **Bedrock — inference** | `BedrockInvokeModelOperator`, `BedrockBatchInferenceOperator`, `BedrockRetrieveOperator`, `BedrockRaGOperator` | `BedrockBatchInferenceSensor` |
| **Bedrock — customisation** | `BedrockCustomizeModelOperator`, `BedrockCreateProvisionedModelThroughputOperator`, `BedrockCreateEvaluationJobOperator` | `BedrockCustomizeModelCompletedSensor`, `BedrockProvisionModelThroughputCompletedSensor` |
| **Bedrock — knowledge bases** | `BedrockCreateKnowledgeBaseOperator`, `BedrockCreateDataSourceOperator`, `BedrockIngestDataOperator` | `BedrockIngestionJobSensor`, `BedrockKnowledgeBaseActiveSensor` |
| **Bedrock — guardrails & agents** | `BedrockCreateGuardrailOperator`, `BedrockCreateGuardrailVersionOperator`, `BedrockUpdateGuardrailOperator`, `BedrockDeleteGuardrailOperator`, `BedrockCreateAgentRuntimeOperator`, `BedrockInvokeAgentRuntimeOperator`, `BedrockDeleteAgentRuntimeOperator` | — |
| **Comprehend** | `ComprehendStartPiiEntitiesDetectionJobOperator`, `ComprehendCreateDocumentClassifierOperator` | `ComprehendStartPiiEntitiesDetectionJobCompletedSensor`, `ComprehendCreateDocumentClassifierCompletedSensor` |

---

## 10 · AWS platform and operations

| service | classes |
|---|---|
| **CloudFormation** | `CloudFormationCreateStackOperator`, `CloudFormationDeleteStackOperator`; sensors `CloudFormationCreateStackSensor`, `CloudFormationDeleteStackSensor` |
| **Systems Manager** | `SsmRunCommandOperator`, `SsmGetCommandInvocationOperator`; sensor `SsmRunCommandCompletedSensor` — run a command on an EC2 fleet without SSH |
| **ECR** | `EcrCreateRepositoryOperator`, `EcrDeleteRepositoryOperator`, `EcrSetRepositoryPolicyOperator` |

---

## 11 · The three ways to wait, and why it matters

| mode | worker slot held | use when |
|---|---|---|
| `mode="poke"` (default) | **yes, the whole time** | the wait is under a minute |
| `mode="reschedule"` | no — released between pokes | minutes to hours |
| `deferrable=True` | no — the triggerer waits asynchronously | hours, or hundreds of concurrent waits |

Twenty poke-mode sensors on a sixteen-slot worker pool is a deployment that
looks hung with nothing running. It is the single most common managed-Airflow
support ticket. Every long wait in the enterprise lab uses `deferrable=True`.

---

## 12 · Beyond AWS

Not installed in this repo's image. `pip install <package>` locally, or add it
to `requirements.txt` on MWAA. Names are the PyPI package; the headline classes
are the ones you would actually reach for.

| target | package | headline classes |
|---|---|---|
| Google Cloud | `apache-airflow-providers-google` | `BigQueryInsertJobOperator`, `DataprocSubmitJobOperator`, `DataflowTemplatedJobStartOperator`, `GCSToBigQueryOperator` |
| Azure | `apache-airflow-providers-microsoft-azure` | `AzureDataFactoryRunPipelineOperator`, `AzureSynapseRunSparkBatchOperator`, `WasbBlobSensor` |
| Databricks | `apache-airflow-providers-databricks` | `DatabricksSubmitRunOperator`, `DatabricksRunNowOperator`, `DatabricksSqlOperator` |
| Snowflake | `apache-airflow-providers-snowflake` | `SnowflakeSqlApiOperator`, `CopyFromExternalStageToSnowflakeOperator` |
| Kubernetes | `apache-airflow-providers-cncf-kubernetes` | `KubernetesPodOperator` — run any container image as a task |
| Docker | `apache-airflow-providers-docker` | `DockerOperator` |
| dbt Cloud | `apache-airflow-providers-dbt-cloud` | `DbtCloudRunJobOperator`, `DbtCloudJobRunSensor` |
| dbt Core | `astronomer-cosmos` (community) | renders every dbt model as its own Airflow task |
| Spark (standalone) | `apache-airflow-providers-apache-spark` | `SparkSubmitOperator` |
| Kafka | `apache-airflow-providers-apache-kafka` | `ConsumeFromTopicOperator`, `ProduceToTopicOperator`, `AwaitMessageTrigger` |
| Trino / Presto | `apache-airflow-providers-trino`, `-presto` | via `SQLExecuteQueryOperator` |
| Hive | `apache-airflow-providers-apache-hive` | `HiveOperator`, `NamedHivePartitionSensor` |
| Postgres / MySQL / MSSQL / Oracle | `-postgres`, `-mysql`, `-microsoft-mssql`, `-oracle` | all through `SQLExecuteQueryOperator` |
| SSH / SFTP / FTP | `-ssh`, `-sftp`, `-ftp` | `SSHOperator`, `SFTPOperator`, `SFTPSensor` |
| HTTP | `apache-airflow-providers-http` | `HttpOperator`, `HttpSensor` |
| Slack | `apache-airflow-providers-slack` | `SlackAPIPostOperator`, `SlackWebhookOperator` |
| PagerDuty / Opsgenie | `-pagerduty`, `-opsgenie` | incident creation on failure |
| Email | `apache-airflow-providers-smtp` | `EmailOperator` |
| Salesforce / Zendesk / Jira | `-salesforce`, `-zendesk`, `-atlassian-jira` | SaaS extraction and ticketing |
| GitHub | `apache-airflow-providers-github` | `GithubOperator`, `GithubSensor` |
| Papermill | `apache-airflow-providers-papermill` | parameterised notebook execution |
| Elasticsearch / OpenSearch | `-elasticsearch`, `-opensearch` | search index maintenance |
| MongoDB / Redis / Neo4j / InfluxDB | `-mongo`, `-redis`, `-neo4j`, `-influxdb` | NoSQL and time-series |
| OpenLineage | `apache-airflow-providers-openlineage` | automatic cross-system lineage emission |

**Roughly 90 provider packages exist.** A typical serious deployment installs
between three and six.

---

## 13 · Transformation frameworks

Airflow does not transform data. It runs the thing that does. The three
patterns you will meet:

| pattern | how it looks in a DAG |
|---|---|
| **Spark** (this course) | `EmrServerlessStartJobOperator` / `GlueJobOperator` pointing at a `.py` in S3 |
| **dbt, one task** | `BashOperator("dbt run --select tag:daily")` — simple, but one green box for 200 models |
| **dbt, one task per model** | `astronomer-cosmos` renders the dbt DAG into the Airflow DAG — real per-model retry and lineage |
| **Data quality** | Glue Data Quality rulesets, the `SQLCheckOperator` family, or `great_expectations` |

Airflow and dbt are not alternatives. Airflow triggers dbt. Atlas 8 chapter 7
draws the boundary.

---

## 14 · Notifications and human-in-the-loop

**Notifications** are not tasks. A callback is a plain function, so it uses a
**hook**, never an operator:

```python
def notify(context):
    from airflow.providers.amazon.aws.hooks.sns import SnsHook
    ti = context["task_instance"]
    SnsHook(aws_conn_id="aws_default").publish_to_target(
        target_arn="arn:aws:sns:...:alerts",
        subject=f"FAILED: {ti.dag_id}.{ti.task_id}"[:100],
        message=f"try {ti.try_number}, run {context['dag_run'].run_id}",
    )

default_args = {"on_failure_callback": notify}
```

Airflow 3 also ships a **notifier** abstraction (`on_failure_callback` accepts
a `BaseNotifier`), and the Slack/SMTP providers include ready-made ones.

**Human-in-the-loop** is new and genuinely useful: `ApprovalOperator` and
`HITLBranchOperator` in the standard provider pause a run until a person
answers in the UI. That replaces the old pattern of failing the DAG and having
someone clear the task manually.

> Airflow 3 **removed task-level `sla=`**. A deadline that matters should be an
> explicit check that names what is at risk, which is more useful than an
> "SLA missed" email anyway. DAG 05 of the enterprise lab shows the pattern.

---

## 15 · When there is no operator

There are around 268 AWS classes and you will still hit a gap. The answer is
always the hook, and it is idiomatic:

```python
@task
def tag_glue_table_as_pii(table: str):
    from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
    hook = GlueCatalogHook(aws_conn_id="aws_default")
    client = hook.get_conn()                 # a real boto3 client, fully authenticated
    tbl = client.get_table(DatabaseName="silver", Name=table)["Table"]
    params = {**(tbl.get("Parameters") or {}), "pii": "true"}
    client.update_table(DatabaseName="silver",
                        TableInput={**{k: v for k, v in tbl.items()
                                       if k in ("Name", "StorageDescriptor", "TableType")},
                                    "Parameters": params})
```

`hook.get_conn()` returns a boto3 client with the Connection's credentials
already applied — including the MWAA execution role, cross-account assume-role,
and region. **Never construct `boto3.client()` yourself in a task**: you lose
all of that and hard-code an auth path that will not work in another
environment.

---

## 16 · MWAA — what changes

| | local Docker | MWAA |
|---|---|---|
| add a provider | edit the Dockerfile | add to `requirements.txt` in S3, environment restarts |
| system packages (Java, gcc) | `apt-get` in the Dockerfile | **not possible** |
| deploy a DAG | it is bind-mounted | `aws s3 cp dags/ s3://<bucket>/dags/ --recursive` |
| credentials | keys in a Connection | leave `aws_default` **empty** — the execution role is used |
| the CLI | `docker compose exec … airflow …` | `aws mwaa create-cli-token` + an HTTPS POST |
| executor | LocalExecutor | CeleryExecutor with autoscaling workers |
| `airflow.cfg` | all yours | an allow-list of options only |

**The architectural consequence:** you cannot install Java on an MWAA worker,
so you cannot run Spark there. That is not a limitation, it is MWAA telling you
the correct design. Spark runs on EMR; Airflow submits.

---

## 17 · Anti-patterns

| do not | because | instead |
|---|---|---|
| process a DataFrame in a `@task` | the worker is a small box with no parallelism | submit to EMR or Glue |
| put data in XCom | XCom lives in the metadata database | pass an S3 path or a snapshot id |
| `Variable.get()` at the top of a DAG file | it runs on **every parse**, every ~30 s, for every DAG | read it inside a task, or use `{{ var.value.x }}` |
| build `boto3.client()` by hand in a task | you lose the Connection, the role and the region | `Hook.get_conn()` |
| `mode="poke"` on a multi-hour sensor | it holds a worker slot the entire time | `deferrable=True` |
| map over a million rows | the scheduler and XCom are not built for it | map over partitions or batches |
| let `EmrTerminateJobFlowOperator` default its trigger rule | a failed step leaves the cluster running | `trigger_rule="all_done"` |
| trust a green `AthenaOperator` | it means the query ran, not that the answer was right | read the result in a task and raise |
| use a cron offset to wait for another DAG | it is a guess that breaks the first time upstream is slow | Assets |
| hard-code a table list in a DAG | it rots the moment another team adds a table | read the inventory at run time and `.expand()` |

---

## 18 · Where to see all of this working

| chapter | enterprise lab DAG |
|---|---|
| 5 — EMR on EC2, transient clusters, spot fleets | `01_cdr_mediation_hourly` |
| 5, 11 — EMR Serverless, deferrable, self-healing compaction | `02_ran_kpi_micro_batch` |
| 9 — SageMaker, drift-gated retraining, Assets | `03_subscriber_360_churn_ml` |
| 4, 6, 8 — Glue, DQ rulesets, DMS, Athena, Step Functions | `04_revenue_assurance_interconnect` |
| 5, 15 — EMR on EKS, hooks, runtime-derived fan-out | `05_gdpr_erasure_lakehouse_maintenance` |

---

## Appendix A · Complete `apache-airflow-providers-amazon` inventory

Generated by walking the installed package. Every class below exists in
version 9.34.0; import path is `airflow.providers.amazon.aws.<module>`.


### Operators — 193 classes in 41 modules

| module | classes |
|---|---|
| `operators.appflow` | `AppflowRecordsShortCircuitOperator`, `AppflowRunAfterOperator`, `AppflowRunBeforeOperator`, `AppflowRunDailyOperator`, `AppflowRunFullOperator`, `AppflowRunOperator` |
| `operators.athena` | `AthenaOperator` |
| `operators.batch` | `BatchCreateComputeEnvironmentOperator`, `BatchOperator` |
| `operators.bedrock` | `BedrockBatchInferenceOperator`, `BedrockCreateAgentRuntimeOperator`, `BedrockCreateDataSourceOperator`, `BedrockCreateEvaluationJobOperator`, `BedrockCreateGuardrailOperator`, `BedrockCreateGuardrailVersionOperator`, `BedrockCreateProvisionedModelThroughputOperator`, `BedrockCustomizeModelOperator`, `BedrockDeleteAgentRuntimeOperator`, `BedrockDeleteGuardrailOperator`, `BedrockIngestDataOperator`, `BedrockInvokeAgentRuntimeOperator`, `BedrockInvokeModelOperator`, `BedrockRaGOperator`, `BedrockRetrieveOperator`, `BedrockUpdateGuardrailOperator` |
| `operators.cloud_formation` | `CloudFormationCreateStackOperator`, `CloudFormationDeleteStackOperator` |
| `operators.comprehend` | `ComprehendCreateDocumentClassifierOperator`, `ComprehendStartPiiEntitiesDetectionJobOperator` |
| `operators.datasync` | `DataSyncOperator` |
| `operators.dms` | `DmsCreateReplicationConfigOperator`, `DmsCreateTaskOperator`, `DmsDeleteReplicationConfigOperator`, `DmsDeleteTaskOperator`, `DmsDescribeReplicationConfigsOperator`, `DmsDescribeReplicationsOperator`, `DmsDescribeTasksOperator`, `DmsModifyTaskOperator`, `DmsReloadTablesOperator`, `DmsStartReplicationOperator`, `DmsStartTaskOperator`, `DmsStopReplicationOperator`, `DmsStopTaskOperator` |
| `operators.ec2` | `EC2CreateInstanceOperator`, `EC2HibernateInstanceOperator`, `EC2RebootInstanceOperator`, `EC2StartInstanceOperator`, `EC2StopInstanceOperator`, `EC2TerminateInstanceOperator` |
| `operators.ecr` | `EcrCreateRepositoryOperator`, `EcrDeleteRepositoryOperator`, `EcrSetRepositoryPolicyOperator` |
| `operators.ecs` | `EcsCreateClusterOperator`, `EcsDeleteClusterOperator`, `EcsDeregisterTaskDefinitionOperator`, `EcsRegisterTaskDefinitionOperator`, `EcsRunTaskOperator` |
| `operators.eks` | `EksCreateClusterOperator`, `EksCreateFargateProfileOperator`, `EksCreateNodegroupOperator`, `EksDeleteClusterOperator`, `EksDeleteFargateProfileOperator`, `EksDeleteNodegroupOperator`, `EksPodOperator` |
| `operators.emr` | `EmrAddStepsOperator`, `EmrContainerOperator`, `EmrCreateJobFlowOperator`, `EmrEksCreateClusterOperator`, `EmrModifyClusterOperator`, `EmrServerlessCreateApplicationOperator`, `EmrServerlessDeleteApplicationOperator`, `EmrServerlessStartJobOperator`, `EmrServerlessStopApplicationOperator`, `EmrStartNotebookExecutionOperator`, `EmrStopNotebookExecutionOperator`, `EmrTerminateJobFlowOperator` |
| `operators.eventbridge` | `EventBridgeDisableRuleOperator`, `EventBridgeEnableRuleOperator`, `EventBridgePutEventsOperator`, `EventBridgePutRuleOperator` |
| `operators.glacier` | `GlacierCreateJobOperator`, `GlacierUploadArchiveOperator` |
| `operators.glue` | `GlueDataQualityOperator`, `GlueDataQualityRuleRecommendationRunOperator`, `GlueDataQualityRuleSetEvaluationRunOperator`, `GlueJobOperator` |
| `operators.glue_catalog` | `GlueCatalogBatchDeletePartitionOperator`, `GlueCatalogCreateDatabaseOperator`, `GlueCatalogCreatePartitionOperator`, `GlueCatalogCreateTableOperator`, `GlueCatalogDeleteDatabaseOperator`, `GlueCatalogDeleteTableOperator` |
| `operators.glue_crawler` | `GlueCrawlerCreateOperator`, `GlueCrawlerDeleteOperator`, `GlueCrawlerOperator`, `GlueCrawlerRunOperator`, `GlueCrawlerUpdateOperator` |
| `operators.glue_databrew` | `GlueDataBrewStartJobOperator` |
| `operators.kinesis_analytics` | `KinesisAnalyticsV2CreateApplicationOperator`, `KinesisAnalyticsV2StartApplicationOperator`, `KinesisAnalyticsV2StopApplicationOperator` |
| `operators.lambda_function` | `LambdaCreateFunctionOperator`, `LambdaInvokeFunctionOperator` |
| `operators.mwaa` | `MwaaTriggerDagRunOperator` |
| `operators.mwaa_serverless` | `MwaaServerlessCreateWorkflowOperator`, `MwaaServerlessDeleteWorkflowOperator`, `MwaaServerlessStartWorkflowRunOperator`, `MwaaServerlessStopWorkflowRunOperator`, `MwaaServerlessUpdateWorkflowOperator` |
| `operators.neptune` | `NeptuneStartDbClusterOperator`, `NeptuneStopDbClusterOperator` |
| `operators.neptune_analytics` | `NeptuneCancelImportTaskOperator`, `NeptuneCreateGraphOperator`, `NeptuneCreateGraphWithImportOperator`, `NeptuneCreatePrivateGraphEndpointOperator`, `NeptuneDeleteGraphOperator`, `NeptuneDeletePrivateGraphEndpointOperator`, `NeptuneStartImportTaskOperator` |
| `operators.opensearch_serverless` | `OpenSearchServerlessCreateCollectionOperator` |
| `operators.quicksight` | `QuickSightCreateIngestionOperator` |
| `operators.rds` | `RdsCancelExportTaskOperator`, `RdsCopyDbSnapshotOperator`, `RdsCreateDbInstanceOperator`, `RdsCreateDbSnapshotOperator`, `RdsCreateEventSubscriptionOperator`, `RdsDeleteDbInstanceOperator`, `RdsDeleteDbSnapshotOperator`, `RdsDeleteEventSubscriptionOperator`, `RdsStartDbOperator`, `RdsStartExportTaskOperator`, `RdsStopDbOperator` |
| `operators.redshift_cluster` | `RedshiftCreateClusterOperator`, `RedshiftCreateClusterSnapshotOperator`, `RedshiftDeleteClusterOperator`, `RedshiftDeleteClusterSnapshotOperator`, `RedshiftPauseClusterOperator`, `RedshiftResumeClusterOperator` |
| `operators.redshift_data` | `RedshiftDataOperator` |
| `operators.s3` | `S3CopyObjectOperator`, `S3CopyPrefixOperator`, `S3CreateBucketOperator`, `S3CreateObjectOperator`, `S3DeleteBucketOperator`, `S3DeleteBucketTaggingOperator`, `S3DeleteObjectsOperator`, `S3FileTransformOperator`, `S3GetBucketTaggingOperator`, `S3ListOperator`, `S3ListPrefixesOperator`, `S3PutBucketTaggingOperator`, `S3ReadObjectOperator` |
| `operators.s3_tables` | `S3TablesCreateNamespaceOperator`, `S3TablesCreateTableBucketOperator`, `S3TablesCreateTableOperator`, `S3TablesDeleteNamespaceOperator`, `S3TablesDeleteTableBucketOperator`, `S3TablesDeleteTableBucketPolicyOperator`, `S3TablesDeleteTableOperator`, `S3TablesPutTableBucketPolicyOperator`, `S3TablesRenameTableOperator` |
| `operators.s3_vectors` | `S3VectorsCreateIndexOperator`, `S3VectorsCreateVectorBucketOperator`, `S3VectorsDeleteIndexOperator`, `S3VectorsDeleteVectorBucketOperator`, `S3VectorsPutVectorsOperator`, `S3VectorsQueryVectorsOperator` |
| `operators.sagemaker` | `SageMakerAutoMLOperator`, `SageMakerConditionOperator`, `SageMakerCreateExperimentOperator`, `SageMakerCreateNotebookOperator`, `SageMakerDeleteModelOperator`, `SageMakerDeleteNotebookOperator`, `SageMakerEndpointConfigOperator`, `SageMakerEndpointOperator`, `SageMakerModelOperator`, `SageMakerProcessingOperator`, `SageMakerRegisterModelVersionOperator`, `SageMakerStartNoteBookOperator`, `SageMakerStartPipelineOperator`, `SageMakerStopNotebookOperator`, `SageMakerStopPipelineOperator`, `SageMakerTrainingOperator`, `SageMakerTransformOperator`, `SageMakerTuningOperator` |
| `operators.sagemaker_unified_studio` | `SageMakerNotebookOperator` |
| `operators.sagemaker_unified_studio_notebook` | `SageMakerUnifiedStudioNotebookOperator` |
| `operators.ses` | `SesEmailOperator` |
| `operators.sns` | `SnsPublishOperator` |
| `operators.sqs` | `SqsPublishOperator` |
| `operators.ssm` | `SsmGetCommandInvocationOperator`, `SsmRunCommandOperator` |
| `operators.step_function` | `StepFunctionGetExecutionOutputOperator`, `StepFunctionStartExecutionOperator` |

### Sensors — 59 classes in 30 modules

| module | classes |
|---|---|
| `sensors.athena` | `AthenaSensor` |
| `sensors.batch` | `BatchComputeEnvironmentSensor`, `BatchJobQueueSensor`, `BatchSensor` |
| `sensors.bedrock` | `BedrockBatchInferenceSensor`, `BedrockCustomizeModelCompletedSensor`, `BedrockIngestionJobSensor`, `BedrockProvisionModelThroughputCompletedSensor` |
| `sensors.cloud_formation` | `CloudFormationCreateStackSensor`, `CloudFormationDeleteStackSensor` |
| `sensors.comprehend` | `ComprehendCreateDocumentClassifierCompletedSensor`, `ComprehendStartPiiEntitiesDetectionJobCompletedSensor` |
| `sensors.dms` | `DmsTaskCompletedSensor` |
| `sensors.dynamodb` | `DynamoDBValueSensor` |
| `sensors.ec2` | `EC2InstanceStateSensor` |
| `sensors.ecs` | `EcsClusterStateSensor`, `EcsTaskDefinitionStateSensor`, `EcsTaskStateSensor` |
| `sensors.eks` | `EksClusterStateSensor`, `EksFargateProfileStateSensor`, `EksNodegroupStateSensor` |
| `sensors.emr` | `EmrContainerSensor`, `EmrJobFlowSensor`, `EmrNotebookExecutionSensor`, `EmrServerlessApplicationSensor`, `EmrServerlessJobSensor`, `EmrStepSensor` |
| `sensors.glacier` | `GlacierJobOperationSensor` |
| `sensors.glue` | `GlueDataQualityRuleRecommendationRunSensor`, `GlueDataQualityRuleSetEvaluationRunSensor`, `GlueJobSensor` |
| `sensors.glue_catalog_partition` | `GlueCatalogPartitionSensor` |
| `sensors.glue_crawler` | `GlueCrawlerSensor` |
| `sensors.kinesis_analytics` | `KinesisAnalyticsV2StartApplicationCompletedSensor`, `KinesisAnalyticsV2StopApplicationCompletedSensor` |
| `sensors.lambda_function` | `LambdaFunctionStateSensor` |
| `sensors.mwaa` | `MwaaDagRunSensor`, `MwaaTaskSensor` |
| `sensors.mwaa_serverless` | `MwaaServerlessWorkflowRunSensor` |
| `sensors.opensearch_serverless` | `OpenSearchServerlessCollectionActiveSensor` |
| `sensors.quicksight` | `QuickSightSensor` |
| `sensors.rds` | `RdsDbSensor`, `RdsExportTaskExistenceSensor`, `RdsSnapshotExistenceSensor` |
| `sensors.redshift_cluster` | `RedshiftClusterSensor` |
| `sensors.s3` | `S3KeySensor`, `S3KeysUnchangedSensor` |
| `sensors.sagemaker` | `SageMakerAutoMLSensor`, `SageMakerEndpointSensor`, `SageMakerPipelineSensor`, `SageMakerProcessingSensor`, `SageMakerTrainingSensor`, `SageMakerTransformSensor`, `SageMakerTuningSensor` |
| `sensors.sagemaker_unified_studio` | `SageMakerNotebookSensor` |
| `sensors.sagemaker_unified_studio_notebook` | `SageMakerUnifiedStudioNotebookSensor` |
| `sensors.sqs` | `SqsSensor` |
| `sensors.ssm` | `SsmRunCommandCompletedSensor` |
| `sensors.step_function` | `StepFunctionExecutionSensor` |

### Transfers — 16 classes in 16 modules

| module | classes |
|---|---|
| `transfers.azure_blob_to_s3` | `AzureBlobStorageToS3Operator` |
| `transfers.dynamodb_to_s3` | `DynamoDBToS3Operator` |
| `transfers.ftp_to_s3` | `FTPToS3Operator` |
| `transfers.gcs_to_s3` | `GCSToS3Operator` |
| `transfers.glacier_to_gcs` | `GlacierToGCSOperator` |
| `transfers.google_api_to_s3` | `GoogleApiToS3Operator` |
| `transfers.http_to_s3` | `HttpToS3Operator` |
| `transfers.local_to_s3` | `LocalFilesystemToS3Operator` |
| `transfers.redshift_to_s3` | `RedshiftToS3Operator` |
| `transfers.s3_to_dynamodb` | `S3ToDynamoDBOperator` |
| `transfers.s3_to_ftp` | `S3ToFTPOperator` |
| `transfers.s3_to_redshift` | `S3ToRedshiftOperator` |
| `transfers.s3_to_sftp` | `S3ToSFTPOperator` |
| `transfers.s3_to_sql` | `S3ToSqlOperator` |
| `transfers.sftp_to_s3` | `SFTPToS3Operator` |
| `transfers.sql_to_s3` | `SqlToS3Operator` |


---

## Verification log

| claim | method | result |
|---|---|---|
| Airflow version | `airflow.__version__` in the running scheduler | 3.3.1 |
| amazon provider version | `importlib.metadata.version` | 9.34.0 |
| every AWS class name in chapters 5-10 and Appendix A | walked `airflow.providers.amazon.aws.{operators,sensors,transfers}` with `pkgutil` + `inspect` inside the container | 268 classes across 87 modules, all names copied from that walk |
| standard + common.sql class names (chapters 3-4) | same walk over those packages | standard 1.17.0, common-sql 2.1.0 |
| `Variable.get` keyword rename | `inspect.signature` on both classes | `airflow.sdk.Variable.get(key, default=…)` vs `airflow.models.Variable.get(key, default_var=…)` — **CORRECTED**, the Airflow 2 form raises `TypeError` at parse time |
| the five enterprise DAGs parse | `DagBag` in the running Airflow 3.3.1 image with the amazon provider | 5 DAGs, 98 tasks, **0 import errors**, `dag.validate()` passes on each |
| manual runs have no logical date | observed in the airflow_local lab | **CONFIRMED** — `{{ ds }}` is undefined, not None |
| Airflow 3 seeds no default connections | observed: `FileSensor` failed with `conn_id fs_default isn't defined` | **CONFIRMED** |
| `sla=` removed in Airflow 3 | Airflow 3 release notes; deadline alerting used instead in DAG 05 | **CONFIRMED** |

Non-AWS package names in chapter 12 are PyPI package names, not verified class
signatures — they are not installed in this image. Everything else above was
read from a running system.
