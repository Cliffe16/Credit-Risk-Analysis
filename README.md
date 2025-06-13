# Credit Risk Analysis for QuickPesa

## Objective
The objective of this project was to conduct a thorough credit risk analysis to identify the key drivers of loan defaults and uncover characteristics of high-risk customers. The ultimate goal is to provide actionable insights that QuickPesa can use to refine its lending strategies, minimize potential losses, and promote sustainable growth.

## Table of Contents
- [Problem Statement](#problem-statement)
- [Data Source](#data-source)
- [Methodology](#methodology)
- [Key Findings](#key-findings)
- [Business Recommendations](#business-recommendations)
- [Project Structure](#project-structure)
- [How to Run the Project](#how-to-run-the-project)
- [Limitations](#limitations)
- [Future Work](#future-work)

## Problem Statement
QuickPesa is seeking to reduce its loan default rate. This project investigates customer demographic, financial, loan-specific, and behavioral attributes to pinpoint characteristics associated with a higher risk of default. The analysis of the dataset showed that **75.7%** of the loans in this dataset ended in default.

## Data Source
The data was sourced from a SQL Server database named `QuickPesaDB`. The database contains comprehensive information across several tables, including:
* `Customers`: Demographic and contact information.
* `LoanProducts`: Details on different loan types offered.
* `LoanApplications`: Records of all loan applications.
* `Loans`: Details of disbursed loans, including their status (e.g., 'Paid', 'Defaulted').
* `Repayments`: Transactional data for loan repayments.
* `MobileMoneyTransactions`: Customer's mobile money usage data.
* `CustomerCreditInfo`: Detailed credit history and calculated scores for customers.

## Methodology
The analysis followed these steps:
1.  **Data Extraction**: Connected to the `QuickPesaDB` using Python (`pyodbc`) and executed a SQL query (`main_data_extraction.sql`) to join and retrieve relevant data from multiple tables.
2.  **Data Preprocessing & Feature Engineering**:
    * The raw data was cleaned, and missing values were handled. The `AvgDepositLast6Months` column had a significant number of missing values and was imputed with `0`.
    * A binary target variable `IsDefault` was created from the `LoanStatus` column.
    * New features were engineered to capture deeper insights, including Debt-to-Income (DTI) ratio, `LoanToMaxEligibleRatio`, `DaysSinceLastDefault`, and `CustomerTenureDays`.
3.  **Exploratory Data Analysis (EDA)**: The processed data was analyzed to compare the profiles of defaulting and non-defaulting customers, identifying key patterns and correlations.

## Key Findings
The analysis identified several key indicators of a high-risk customer:
* **Credit Score and Affordability**: A low `CreditScore` and a high Debt-to-Income (DTI) ratio are powerful predictors of default.
* **Mobile Money Behavior**: A pattern of frequent overdrafts (`OverdraftsLast3Months`) and low average deposits (`AvgDepositLast6Months`) is strongly correlated with a higher likelihood of default.
* **Employment Status**: Default rates were highest among customers with an `EmploymentStatus` of 'Unemployed' or 'Casual Worker'.
* **Loan History**: A history of `PreviousDefaults` is a significant red flag.

## Business Recommendations
Based on the findings, here are several actionable recommendations for QuickPesa:

1.  **Enhanced Underwriting Rules**: Implement stricter DTI thresholds. For instance, flag applications with a DTI greater than 0.35 for manual review or automatic decline if other risk factors are present.
2.  **Refined Credit Scoring**: Integrate mobile money metrics like `OverdraftsLast3Months` and `AvgDepositLast6Months` into the internal credit assessment model to capture real-time financial health.
3.  **Dynamic Loan Offers**: For moderate-risk applicants, consider offering a smaller loan amount or a shorter term to mitigate risk while still serving them.
4.  **Targeted Monitoring**: Proactively monitor active loans for customers who matched the high-risk profile at the time of application to enable earlier intervention.
5.  **Product Strategy Adjustments**: Review the terms and marketing for loan products that have disproportionately high default rates within specific customer segments.

## Project Structure
The project repository is organized as follows:
Of course. Here is the content in Markdown format, ready to be copied into your README.md file.

Markdown

# Credit Risk Analysis for QuickPesa

## Objective
The objective of this project was to conduct a thorough credit risk analysis to identify the key drivers of loan defaults and uncover characteristics of high-risk customers. The ultimate goal is to provide actionable insights that QuickPesa can use to refine its lending strategies, minimize potential losses, and promote sustainable growth.

## Table of Contents
- [Problem Statement](#problem-statement)
- [Data Source](#data-source)
- [Methodology](#methodology)
- [Key Findings](#key-findings)
- [Business Recommendations](#business-recommendations)
- [Project Structure](#project-structure)
- [How to Run the Project](#how-to-run-the-project)
- [Limitations](#limitations)
- [Future Work](#future-work)

## Problem Statement
QuickPesa is seeking to reduce its loan default rate. This project investigates customer demographic, financial, loan-specific, and behavioral attributes to pinpoint characteristics associated with a higher risk of default. The analysis of the dataset showed that **75.7%** of the loans in this dataset ended in default.

## Data Source
The data was sourced from a SQL Server database named `QuickPesaDB`. The database contains comprehensive information across several tables, including:
* `Customers`: Demographic and contact information.
* `LoanProducts`: Details on different loan types offered.
* `LoanApplications`: Records of all loan applications.
* `Loans`: Details of disbursed loans, including their status (e.g., 'Paid', 'Defaulted').
* `Repayments`: Transactional data for loan repayments.
* `MobileMoneyTransactions`: Customer's mobile money usage data.
* `CustomerCreditInfo`: Detailed credit history and calculated scores for customers.

## Methodology
The analysis followed these steps:
1.  **Data Extraction**: Connected to the `QuickPesaDB` using Python (`pyodbc`) and executed a SQL query (`main_data_extraction.sql`) to join and retrieve relevant data from multiple tables.
2.  **Data Preprocessing & Feature Engineering**:
    * The raw data was cleaned, and missing values were handled. The `AvgDepositLast6Months` column had a significant number of missing values and was imputed with `0`.
    * A binary target variable `IsDefault` was created from the `LoanStatus` column.
    * New features were engineered to capture deeper insights, including Debt-to-Income (DTI) ratio, `LoanToMaxEligibleRatio`, `DaysSinceLastDefault`, and `CustomerTenureDays`.
3.  **Exploratory Data Analysis (EDA)**: The processed data was analyzed to compare the profiles of defaulting and non-defaulting customers, identifying key patterns and correlations.

## Key Findings
The analysis identified several key indicators of a high-risk customer:
* **Credit Score and Affordability**: A low `CreditScore` and a high Debt-to-Income (DTI) ratio are powerful predictors of default.
* **Mobile Money Behavior**: A pattern of frequent overdrafts (`OverdraftsLast3Months`) and low average deposits (`AvgDepositLast6Months`) is strongly correlated with a higher likelihood of default.
* **Employment Status**: Default rates were highest among customers with an `EmploymentStatus` of 'Unemployed' or 'Casual Worker'.
* **Loan History**: A history of `PreviousDefaults` is a significant red flag.

## Business Recommendations
Based on the findings, here are several actionable recommendations for QuickPesa:

1.  **Enhanced Underwriting Rules**: Implement stricter DTI thresholds. For instance, flag applications with a DTI greater than 0.35 for manual review or automatic decline if other risk factors are present.
2.  **Refined Credit Scoring**: Integrate mobile money metrics like `OverdraftsLast3Months` and `AvgDepositLast6Months` into the internal credit assessment model to capture real-time financial health.
3.  **Dynamic Loan Offers**: For moderate-risk applicants, consider offering a smaller loan amount or a shorter term to mitigate risk while still serving them.
4.  **Targeted Monitoring**: Proactively monitor active loans for customers who matched the high-risk profile at the time of application to enable earlier intervention.
5.  **Product Strategy Adjustments**: Review the terms and marketing for loan products that have disproportionately high default rates within specific customer segments.

## Project Structure
The project repository is organized as follows:\
├── config.ini                  # Configuration for database connection\
├── data/                       # Holds raw and processed data\
│   ├── dataset.py\
│   └── raw_loan_data.csv\
├── notebooks/                  # Jupyter notebooks for analysis\
│   ├── 01_Data_Exploration_and_Preprocessing.ipynb\
│   ├── 02_Exploratory_Data_Analysis.ipynb\
│   └── 03_Findings_and_Recommendations.ipynb\
├── sql_queries/                # SQL scripts\
│   ├── Database Generation.sql\
│   └── main_data_extraction.sql\
└── README.md                   # This file\

## How to Run the Project
1.  **Set up the Database**: Execute the `sql_queries/Database Generation.sql` script on your SQL Server to create the `QuickPesaDB` database and its tables.
2.  **Configure Connection**: Update the `config.ini` file with your SQL Server credentials.
3.  **Run the Notebooks**: Execute the Jupyter Notebooks in the `notebooks/` directory in sequential order:
    * `01_Data_Exploration_and_Preprocessing.ipynb`: To extract data from the database and perform initial cleaning and feature engineering.
    * `02_Exploratory_Data_Analysis.ipynb`: To analyze the data and generate insights.
    * `03_Findings_and_Recommendations.ipynb`: To view the summary of findings and business recommendations.

## Limitations
* The analysis is based purely on the historical data available in `QuickPesaDB`. It does not account for external macroeconomic factors.
* A predictive machine learning model was not built; this analysis is diagnostic in nature.

## Future Work
* Develop and deploy a machine learning model for automated, forward-looking default prediction.
* Conduct A/B testing on the recommended underwriting rules to measure their impact.
* Perform a deeper cohort analysis to track customer repayment behavior over longer periods.
