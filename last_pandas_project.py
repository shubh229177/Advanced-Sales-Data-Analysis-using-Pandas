import pandas as pd
import numpy as np
# read the file 
order_csv = pd.read_csv('extended_orders.csv')
print(order_csv)
customer_csv = pd.read_csv('customers_extended.csv')
print(customer_csv)

# merge the dataframe
merged_df = pd.merge(order_csv, customer_csv, on='customer_id', how = 'inner')
print(merged_df)

# left merge 

print('left merge :')
left_merged_df = pd.merge(order_csv, customer_csv, on='customer_id', how = 'left')
print(left_merged_df)

# right merge 

print('right merged :')
right_merged_df = pd.merge(order_csv, customer_csv , on = 'customer_id', how = 'right')
print(right_merged_df)

# create a dataframe customer_name, city, product, price, order_date
print('selected columns :')

selected_column_df = merged_df[['customer_id' , 'city', 'product', 'price', 'order_date']]
print(selected_column_df)

# pivate table 

# craete a pivote table index = city  values = price * quantity , aggfunc = sum

print('pivote table :')

pivate_table = pd.pivot_table(merged_df, index = 'city', values = 'price', aggfunc = 'sum' )
print(pivate_table)

# month vs city total orders index = month,  columns = city , vaules = ordre_id , aggfunc = 'count' 

print('month vs city total orders :')

month_vs_city = pd.pivot_table(merged_df, index= 'order_date', columns= 'city', values= 'order_id', aggfunc= 'count')
print(month_vs_city)

# create new column total_amount = price * quantity

order_csv['total_amount'] = order_csv['price'] * order_csv['quantity']
merged_df = pd.merge(order_csv, customer_csv, on='customer_id', how = 'inner')
print('with total amount column :', merged_df)

# apply discount if total_amount > 20000 → 10% discount else if > 10000 → 5% else 0%

def apply_discount(total_amount):
    if total_amount > 20000:
        return total_amount * 0.9
    elif total_amount > 10000:
        return total_amount * 0.95
    else:
        return total_amount
    
merged_df['final_amount'] = merged_df['total_amount'].apply(apply_discount)
print('with final amount after discount :', merged_df)

# groupby multiple index (customer_id, product) → total_quantity and the usking .unstack make it in table formate 

grouped_qty = merged_df.groupby(['customer_id', 'product'])['quantity'].sum()
grouped_table = grouped_qty.unstack(fill_value = 0)
print('grouped (customer_id * product) total_quantity table:',grouped_table)

# create a time series (make date -> datetime), compute day-wise revenue sum and 3-day rolling average
# - will use 'final_amount' if present else fallback to 'total_amount'
if 'order_date' not in merged_df.columns:
    raise KeyError("order_date column not found in merged_df")

# parse/construct datetime (combine order_date + order_time if order_time exists)
merged_df['order_date'] = pd.to_datetime(merged_df['order_date'], errors='coerce')

if 'order_time' in merged_df.columns:
    merged_df['order_datetime'] = pd.to_datetime(
        merged_df['order_date'].dt.strftime('%Y-%m-%d') + ' ' + merged_df['order_time'].astype(str),
        errors='coerce'
    )
else:
    merged_df['order_datetime'] = merged_df['order_date']

# choose revenue column
if 'final_amount' in merged_df.columns:
    rev_col = 'final_amount'
elif 'total_amount' in merged_df.columns:
    rev_col = 'total_amount'
else:
    raise KeyError("No revenue column found. Add 'total_amount' or 'final_amount' before this block.")

# resample day-wise and compute sums
merged_df = merged_df.dropna(subset=['order_datetime'])
merged_df['order_datetime'] = pd.to_datetime(merged_df['order_datetime'], errors='coerce')
daily_revenue = merged_df.set_index('order_datetime').resample('D')[rev_col].sum().fillna(0)

# 3-day rolling average (include current day; min_periods=1 to compute for first days)
rolling_3d = daily_revenue.rolling(window=3, min_periods=1).mean()

print('Daily revenue:')
print(daily_revenue)
print('\n3-day rolling average of daily revenue:')
print(rolling_3d)

# outlier detection using iqr method find price column outlier 
print('oputlier detection :')
if 'price' not in merged_df.columns:
    raise KeyError('price column not ')

q1 = merged_df['price'].quantile(0.25)
q3 = merged_df['price'].quantile(0.75)
iqr = q3 - q1
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr
merged_df['price_outlier'] = (merged_df['price'] < lower_bound) | (merged_df['price'] > upper_bound)
outliers = merged_df[merged_df['price_outlier']]
print(f'Price IQR: Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}, lower={lower_bound:.2f}, upper={upper_bound:.2f}')
print(f'Found {len(outliers)} price outlier(s).')
if not outliers.empty:
    print(outliers[['order_id','customer_id','product','price']])

merged_df['price_capped'] = merged_df['price'].clip(lower=lower_bound, upper=upper_bound)
print('Added boolean column "price_outlier" and "price_cappPribted".')

#print correlation matrix
print('correlation :')
required = ['price' , 'quantity' , 'total_amount', 'final_amount']
available = [c for c in required if c in merged_df.columns] 

if 'price' not in merged_df.columns or 'quantity' not in  merged_df.columns:
    raise KeyError("need both 'price' and 'quantity' columns to compute correlations.")

# specific pair correlations ( drop NA for pairs)
pq_corr =  merged_df[['price', 'quantity']].dropna().corr().loc['price','quantity']
print(f'correlation price vs quantity :{pq_corr:.4f}')

if 'total_amount' in merged_df.columns:
    pt_corr = merged_df[['price', 'total_amount']].dropna().corr().loc()['price','total_amount']
    print(f'correlation price vs total_amount: {pt_corr:.4f}')
else:
    print("columns 'total_amount'not found - skipping price vs total_amount.")

# build and print correlation matrix for numeric revenue/quantity related columns found
matrix_cols = [c for c in ['price', 'quantity', 'total_amount', 'final_amount'] if c in merged_df.columns]
if matrix_cols:
    corr_matrix = merged_df[matrix_cols].corr()
    print("\nCorrelation matrix:")
    print(corr_matrix)
else:
    print("No numeric columns available to build correlation matrix.")

# correlation checks: price vs quantity, price vs total_amount and full correlation matrix
required = ['price', 'quantity', 'total_amount', 'final_amount']
available = [c for c in required if c in merged_df.columns]

if 'price' not in merged_df.columns or 'quantity' not in merged_df.columns:
    raise KeyError("Need both 'price' and 'quantity' columns to compute correlations.")

# specific pair correlations (drop NA for pairs)
pq_corr = merged_df[['price', 'quantity']].dropna().corr().loc['price', 'quantity']
print(f"Correlation price vs quantity: {pq_corr:.4f}")

if 'total_amount' in merged_df.columns:
    pt_corr = merged_df[['price', 'total_amount']].dropna().corr().loc['price', 'total_amount']
    print(f"Correlation price vs total_amount: {pt_corr:.4f}")
else:
    print("Column 'total_amount' not found — skipping price vs total_amount.")

# build and print correlation matrix for numeric revenue/quantity related columns found
matrix_cols = [c for c in ['price', 'quantity', 'total_amount', 'final_amount'] if c in merged_df.columns]
if matrix_cols:
     corr_matrix = merged_df[matrix_cols].corr()
     print("\nCorrelation matrix:")
     print(corr_matrix)
else:
    print("No numeric columns available to build correlation matrix.")

# top customers
print('top customers analysis:')

# total revenue per customer

revenue_col = 'final_amount' if 'final_amount' in merged_df.columns else 'total_amount'
customer_revenue = merged_df.groupby('customer_id')[revenue_col].sum().sort_values(ascending=False)
print("\n total revenue per customer:",customer_revenue)

# top three best customers
top_3_customers = customer_revenue.head(3)
print("\n Top 3 best customers (highest spend):",top_3_customers)

# customer names with revenue:

print("names of cuistomer with revenue :")
if 'customer_name' in merged_df.columns:
    customer_names = merged_df[['customers_id','customer_name']].drop_duplicates().set_index('customer_id')
    top_3_customers = top_3_customers.to_frame().join(customer_names)
    print("\n top 3 customers with names:")
    print(top_3_customers)