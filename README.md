

# PRIMSM INSURANCE PVT. LTD — End-to-End Insurance Data Pipeline & Dashboard Project

## Project Aim

To design and implement a comprehensive dashboard for insurance analytics at PRIMSM Insurance Pvt. Ltd., enabling fast decisions and insights into policy, claim, demographic, and financial metrics using interactive visualizations.

## Project Overview

This project implements a complete data pipeline and dashboard for PRIMSM Insurance Pvt. Ltd. using Snowflake, enabling automated and scalable ingestion, transformation, and analytics of insurance data.

## Project Objectives

- Provide real-time visibility into key metrics such as premiums, claims, and coverage.
- Segment data by policy type, claim status, age group, gender, and active/inactive status.
- Help stakeholders understand trends and make informed strategic decisions.
- Automate KPI monitoring for fast performance evaluation across insurance operations.

## End-to-End Process Steps

- **Database and Schema Setup:**  
  Create database and schema (`insurance_db`, `insurance_schema`).

- **Table Definition:**  
  Define `insurance_table` with all necessary fields to store insurance policy and claim data.

- **File Format Creation:**  
  Establish a CSV file format (`insurance_csv`) for incoming data.

- **Cloud Storage Integration:**  
  Set up S3 integration (`insurance_integration_s3`) for secure data staging.

- **Stage Creation:**  
  Create a Snowflake stage (`insurace_stage`) to load files from S3 using defined file format and integration object.

- **File Listing and Type Casting:**  
  List files in stage and cast date fields to correct format during loading.

- **Pipe and Copy Statement:**  
  Create a Snowpipe (`insurance_pipe`) for continuous, automated loading to `insurance_table`.

- **Pipe Management:**  
  Refresh Snowpipe, monitor status with `SYSTEM$PIPE_STATUS`, and troubleshoot errors using the `validate_pipe_load` function.

- **Data Validation and Query:**  
  Use queries and commands (`desc`, `show columns`, `select *`) for meta-information and data extraction.

## KPI Questions and Answers

| KPI Question                                  | Answer / Value                                               |
|-----------------------------------------------|--------------------------------------------------------------|
| What is the total premium amount collected?   | 5.98 Million                                                 |
| What is the total coverage amount provided?   | 600.55 Million                                               |
| What is the total claim amount processed?     | 16.91 Million                                                |
| Number of active vs inactive policies?        | Active: 5,820 (58.13%), Inactive: 4,190 (41.87%)             |
| Gender split among customers?                 | Female: 5,001, Male: 5,003                                   |
| Which policy type has the highest premium?    | Travel: 2.5 Million                                          |
| Status split for claims?                      | Rejected: 4.4k, Settled: 3.4k, Pending: 2.3k                 |
| Highest claim amount by age group?            | Adult: 8.8 Million                                           |
| Pending claim amount by policy type?          | Auto: 2,08,10,615.31; Health: 12,86,782.91; Home: 1,30,01,861.10; Life: 1,72,59,587.97; Travel: 2,47,694.84 |
| Rejected claim amount by policy type?         | Auto: 4,06,71,711.52; Health: 5,24,01,928.46; Home: 7,14,06,202.61; Life: 3,37,22,751.55; Travel: 10,73,95,611.47 |
| Settled claim amount by policy type?          | Auto: 3,29,84,558.26; Health: 4,00,17,100.09; Home: 9,03,11,027.10; Life: 3,21,21,204.60; Travel: 1,46,83,353.08 |

## Interactive Dashboard
<a href="https://github.com/srikanth20k/insurance_project/blob/main/insurance_dashboard.pbix">View Dashboard</a>

## Image of Dashboard
<a href="https://github.com/srikanth20k/insurance_project/blob/main/insurance_project.png">View Image</a>
<img width="1355" height="744" alt="insurance_project" src="https://github.com/user-attachments/assets/0ba6bc22-0433-4bbe-98cc-3598fef1e102" />

## Final Conclusion

This dashboard provides a holistic view of the insurance business performance, highlighting key financials, operations, and customer demographics. Stakeholders can easily track premium generation, claims processing efficiency, policy status distribution, and identify business opportunities or pain points. Automated, data-driven insights foster enhanced decision-making and ongoing business growth.
