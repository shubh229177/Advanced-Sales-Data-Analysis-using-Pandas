This project demonstrates a complete end-to-end Sales Data Analysis pipeline using Python, Pandas, and NumPy.
It covers everything from dataset loading & merging to advanced analytics like time-series revenue trends, rolling averages, outlier detection, pivot tables, and customer-level insights.

Features Implemented
 1. Data Loading & Cleaning

Read CSV files for orders and customers

Merge datasets (inner, left, right joins)

Column selection & preprocessing

 2. Pivot Tables

City-wise sales summary

Month vs city order distribution

Total revenue & quantity insights

 3. Revenue Calculations

Create total_amount = price × quantity

Apply dynamic discounts:

₹20,000 → 10% discount

₹10,000 → 5% discount

Else → 0%

 4. Time-Series Analysis

Convert dates to datetime

Daily revenue using resample()

3-day rolling revenue average

 5. Outlier Detection (IQR Method)

Detect price outliers using:

Q1, Q3

IQR & bounds

Add:

price_outlier column

price_capped values

 6. Correlation Analysis

Price vs Quantity

Price vs Total Amount

Full correlation matrix for:

price

quantity

total_amount

final_amount

 7. Customer Insights

Total revenue per customer

Top 3 customers

Optional: Customer names included

 Tech Stack

Python

Pandas

NumPy

 Use Cases

✔ Sales Data Analysis
✔ Business Intelligence
✔ Revenue Forecasting
✔ Customer Segmentation
✔ Data Cleaning Pipelines

 How to Run
pip install pandas numpy
python analysis.py