# Azure-Databricks-Optimization


The Azure Databricks Optimization Portfolio is a learning and demonstration project that answers four practical questions:

1. Where is Databricks money being spent?
2. Which teams, jobs, or clusters are responsible for that spending?
3. Is today’s cost unusually high?
4. Which Spark jobs or clusters may need performance tuning?

It is a validated code prototype, but it has not yet been connected to and executed in a real Azure Databricks workspace. Therefore, do not claim real production savings until you deploy it and collect actual results.

1. What is Azure Databricks?

Azure Databricks is a cloud platform used to process large amounts of data.

A simplified flow is:

Raw data
   ↓
Azure Databricks
   ↓
Python / PySpark / SQL transformations
   ↓
Delta Lake tables
   ↓
Reports, machine learning, or downstream applications

Important Databricks concepts:

┌────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────┐
│ Concept            │ Simple explanation                                                                         │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Workspace          │ The Databricks environment where users create notebooks, jobs, clusters, and dashboards    │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Cluster or compute │ The group of cloud machines that executes data-processing code                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Spark              │ The distributed processing engine used to process large datasets                           │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ PySpark            │ The Python interface for writing Spark transformations                                     │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Job                │ A scheduled or manually triggered Databricks workflow                                      │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Task               │ One step inside a Databricks job                                                           │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ DBU                │ Databricks Unit—the Databricks usage measurement used for billing                          │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Delta Lake         │ A reliable table format built on cloud storage                                             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Unity Catalog      │ Databricks governance layer for catalogs, schemas, tables, permissions, and lineage        │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ System tables      │ Databricks-provided tables containing billing, compute, audit, and operational information │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Spark event log    │ A JSON record of Spark jobs, stages, tasks, execution times, and data movement             │
├────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
│ Databricks bundle  │ YAML configuration used to deploy jobs and related resources consistently                  │
└────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘

2. What business problem does this project solve?

Databricks compute can become expensive because:

• Clusters may be larger than required.
• Clusters may remain active when no useful work is running.
• Teams may not know which job generated a cost.
• A failed or looping job can suddenly increase spending.
• Spark jobs may perform excessive shuffling or process unnecessary data.
• Different teams may share infrastructure without clear cost ownership.

The project creates a small optimization platform:

Databricks billing system tables
              ↓
      Calculate daily cost
              ↓
    Attribute cost to resources
              ↓
 ┌────────────┴─────────────┐
 ↓                          ↓
Cost anomaly detection   Right-sizing candidates

Spark event logs
      ↓
Parse tasks and stages
      ↓
Performance metrics
      ↓
Find slow or inefficient processing

3. Project components

After extracting the ZIP file, the structure is:

Azure-Databricks-Optimization-Portfolio
│
├── README.md
├── databricks.yml
├── pyrightconfig.json
│
├── resources
│   └── optimization_job.yml
│
└── src
    ├── build_cost_marts.py
    ├── detect_cost_anomalies.py
    ├── generate_right_sizing_recommendations.py
    └── parse_spark_event_logs.py

 README.md 

This is the project documentation. It explains:

• The problem being solved
• The architecture
• Required Databricks permissions
• How to deploy the project
• Tables created by the project
• The resume-safe project description

 databricks.yml 

This is the main Databricks deployment configuration.

It defines:

• The project name
• Files that contain job resources
• Target catalog and schema
• Spark event-log path
• Development and production deployment targets

For example:

variables:
  catalog:
    default: main

  schema:
    default: databricks_optimization

These variables prevent hardcoding database names inside every script.

You can deploy the same code to different environments:

Development:
main.databricks_optimization

Production:
production_catalog.databricks_optimization

 resources/optimization_job.yml 

This defines the Databricks job and its tasks.

The workflow is:

build_cost_marts
      ↓
 ┌────┴───────────────────┐
 ↓                        ↓
detect_cost_anomalies   generate_right_sizing_recommendations

parse_spark_event_logs

The anomaly and right-sizing tasks wait for the cost table to be created. The event-log parser can run independently because it uses a different input source.

4. Cost attribution

File:  build_cost_marts.py 

This script reads two Databricks system tables:

system.billing.usage
system.billing.list_prices

 system.billing.usage 

This table records billable Databricks usage.

It can contain information such as:

• Usage date
• Workspace ID
• Job ID
• Cluster ID
• SKU
• DBUs consumed
• Custom tags
• Identity or resource metadata

A simplified record could look like:

┌────────────┬─────────┬───────────┬──────┬──────────────────┐
│ Date       │ Job     │ Cluster   │ DBUs │ Team             │
├────────────┼─────────┼───────────┼──────┼──────────────────┤
│ 2026-09-21 │ Job 101 │ Cluster A │   25 │ Data Engineering │
└────────────┴─────────┴───────────┴──────┴──────────────────┘

 system.billing.list_prices 

This contains the list price for each Databricks SKU.

Example:

┌─────────────────────┬───────────┐
│ SKU                 │ DBU price │
├─────────────────────┼───────────┤
│ Jobs Compute        │     $0.15 │
├─────────────────────┼───────────┤
│ All-Purpose Compute │     $0.40 │
└─────────────────────┴───────────┘

The project joins usage with the correct price for that date:

Estimated Databricks cost = DBUs consumed × DBU list price

Example:

25 DBUs × $0.15 = $3.75 estimated Databricks cost

It then groups the result by:

• Date
• Workspace
• Job
• Cluster
• SKU
• Team
• Project

The output table is:

daily_compute_cost

Why tags matter

A cluster or job can have tags such as:

team = data-engineering
project = customer-analytics
environment = production

The project uses  team  and  project  tags to associate costs with the responsible group.

Without tags:

A cluster cost $500

With tags:

Data Engineering / OLC project cost $500

That makes cost reporting and internal chargeback easier.

Important limitation

This calculation estimates the Databricks DBU cost using Databricks list prices. It does not currently calculate the full Azure infrastructure cost for virtual machines, storage, networking, or reserved-instance discounts.

A production version could also integrate Azure Cost Management exports.

5. Cost anomaly detection

File:  detect_cost_anomalies.py 

This script looks for unusual daily spending.

For each workspace, team, and project, it calculates:

• The previous seven days’ average cost
• The previous seven days’ standard deviation
• Whether the current day is significantly above the historical pattern

The simplified rule is:

Anomaly threshold =
seven-day average + two standard deviations

Example:

Seven-day average: $100 per day
Standard deviation: $15

Threshold = $100 + (2 × $15)
          = $130

If today’s cost is  $175 , the project marks it as an anomaly.

The output table is:

daily_cost_anomalies

Example:

┌────────┬──────────────────┬────────────┬─────────┬─────────┐
│ Date   │ Team             │ Daily cost │ Average │ Anomaly │
├────────┼──────────────────┼────────────┼─────────┼─────────┤
│ Sep 20 │ Data Engineering │        $98 │    $100 │ No      │
├────────┼──────────────────┼────────────┼─────────┼─────────┤
│ Sep 21 │ Data Engineering │       $175 │    $101 │ Yes     │
└────────┴──────────────────┴────────────┴─────────┴─────────┘

An anomaly does not automatically mean something is wrong. It means someone should investigate.

Possible causes include:

• A job ran more frequently.
• More data was processed.
• A cluster did not terminate.
• A failed job repeatedly retried.
• A user created an oversized interactive cluster.
• A legitimate monthly workload ran.

6. Compute right-sizing candidates

File:  generate_right_sizing_recommendations.py 

This script summarizes cluster usage and estimated cost over the previous 90 days.

It calculates:

• Number of active days
• Total DBUs
• Total estimated Databricks cost
• Average daily cost

It joins that information with:

system.compute.clusters

This supplies configuration details such as:

• Cluster name
• Driver node type
• Worker node type
• Worker count
• Configuration-change time

The output table is:

compute_right_sizing_candidates

A sample output might look like:

┌────────────────┬─────────────┬─────────┬────────────────┬─────────────────────────────────────┐
│ Cluster        │ Active days │ Workers │ Estimated cost │ Recommendation                      │
├────────────────┼─────────────┼─────────┼────────────────┼─────────────────────────────────────┤
│ ETL-Production │          28 │      16 │         $4,200 │ Review worker count and autoscaling │
├────────────────┼─────────────┼─────────┼────────────────┼─────────────────────────────────────┤
│ Development    │           3 │       2 │            $45 │ Monitor                             │
└────────────────┴─────────────┴─────────┴────────────────┴─────────────────────────────────────┘

The project currently creates a candidate list for manual review. It does not automatically resize or terminate clusters.

A more advanced version should also use CPU and memory utilization from compute metrics or  system.compute.node_timeline . That would support recommendations such as:

Average CPU below 20%
Workers configured: 16
Recommended review: reduce maximum workers to 8

7. Spark event-log analysis

File:  parse_spark_event_logs.py 

Spark divides work into:

Application
   ↓
Jobs
   ↓
Stages
   ↓
Tasks

A task is the smallest unit of Spark execution.

A stage can contain hundreds or thousands of tasks:

Stage 4
 ├── Task 1
 ├── Task 2
 ├── Task 3
 └── Task 500

Spark writes execution details as JSON event logs. This project reads two important event types:

•  SparkListenerStageSubmitted 
•  SparkListenerTaskEnd 

The script extracts and aggregates:

• Stage ID and name
• Number of tasks
• Longest task duration
• Total task duration
• Executor runtime
• Input bytes
• Output bytes
• Remote shuffle bytes read
• Shuffle bytes written

The output table is:

spark_stage_metrics

Example:

┌──────────────────┬───────┬──────────┬───────┬─────────────────┐
│ Stage            │ Tasks │ Duration │ Input │ Shuffle written │
├──────────────────┼───────┼──────────┼───────┼─────────────────┤
│ Read source data │   200 │   45 sec │ 30 GB │            0 GB │
├──────────────────┼───────┼──────────┼───────┼─────────────────┤
│ Join datasets    │   400 │  180 sec │ 30 GB │           80 GB │
└──────────────────┴───────┴──────────┴───────┴─────────────────┘

The second stage may require investigation because it writes much more shuffle data than its input.

What is shuffle?

Shuffle occurs when Spark moves data between machines to perform operations such as:

• Joins
• Grouping
• Sorting
• Distinct
• Repartitioning

Large shuffles can make jobs slow and expensive.

Common improvements include:

• Filtering data earlier
• Selecting only required columns
• Broadcasting small tables
• Avoiding unnecessary repartitioning
• Using appropriate partition keys
• Handling skewed join keys

8. Tables produced by the project

┌─────────────────────────────────┬────────────────────────────────────────────────────────────────────────┐
│ Table                           │ Purpose                                                                │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ daily_compute_cost              │ Estimated daily DBU cost by workspace, job, cluster, team, and project │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ daily_cost_anomalies            │ Detects spending above the recent historical pattern                   │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ compute_right_sizing_candidates │ Lists recurring clusters requiring a configuration review              │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ spark_stage_metrics             │ Stores task, duration, executor, input/output, and shuffle metrics     │
└─────────────────────────────────┴────────────────────────────────────────────────────────────────────────┘

Together, these provide:

Financial visibility + compute visibility + Spark performance visibility

9. How to deploy it

First, the Databricks CLI must be installed and authenticated.

Then extract the ZIP and open PowerShell in the project directory:

databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run cost_and_performance_optimization -t dev

Validate

databricks bundle validate -t dev

Checks whether the YAML is structurally valid for the connected workspace.

Deploy

databricks bundle deploy -t dev

Uploads the files and creates the Databricks job.

Run

databricks bundle run cost_and_performance_optimization -t dev

Runs the workflow and creates the optimization tables.

The local project passed YAML and Python validation, but workspace validation still requires:

• A real Azure Databricks workspace
• Authentication
• Unity Catalog permissions
• System-table access
• A valid Spark event-log location

10. How to explain it in an interview

Use this explanation:

I built an independent Azure Databricks cost and performance optimization prototype. The solution uses Unity Catalog billing and compute system tables to create daily job-, cluster-, workspace-, team-, and project-level cost marts. I joined DBU usage with time-ranged list prices to estimate Databricks cost and implemented rolling anomaly detection to flag unusual spending. I also created a right-sizing candidate table by combining historical cost with cluster configurations. For Spark performance analysis, I built a PySpark parser that aggregates task-level event logs into stage-level duration, executor, input/output, and shuffle metrics. I packaged the workflow as a parameterized Databricks bundle with development and production targets.

If asked about business value:

The solution gives teams visibility into where Databricks spending originates and helps identify workloads that need cost or performance review. It is a portfolio prototype, so I do not claim production savings, but the architecture can be extended with Azure Cost Management and CPU/memory telemetry for production use.

11. What you should learn before listing it confidently

Study these topics in order:

1. Spark fundamentals: driver, executor, job, stage, and task
2. PySpark DataFrames and aggregations
3. Delta Lake tables
4. Azure Databricks clusters and jobs
5. DBUs and Azure Databricks pricing
6. Unity Catalog and system tables
7. Window functions and anomaly detection
8. Spark shuffle and performance tuning
9. Databricks bundles and deployment
10. Azure Cost Management integration
