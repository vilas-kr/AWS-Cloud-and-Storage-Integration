import logging
import pandas as pd

from config import settings 
from services.s3_services import S3FileService

logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------------------
# 1. Read data from S3
# -------------------------------------------------------------------------
logging.info('Reading file from S3')
data = pd.read_csv(f's3://{settings.BUCKET_NAME}/raw/sales_dataset.csv')
logging.info('Data loaded successfully')

# -------------------------------------------------------------------------
# 2. Basic Cleaning
# -------------------------------------------------------------------------
logging.info('Cleaning data...')

# Remove duplicate rows
data = data.drop_duplicates(subset=['transaction_id'])

# -------------------------------------------------------------------------
# 3. Handle missing values
# -------------------------------------------------------------------------
# remove critical missing data
data.dropna(subset=['transaction_id', 'order_date', 'customer_id'], inplace=True)

# remove invalid quantity
data.drop(data[data['quantity'] < 0].index, inplace=True)

# recalculate total amount
data['total_amount'] = data['quantity'] * data['price']

# fill missing values
fill_values = {
    'product_name' : 'unknown',
    'category' : 'unknown',
    'region' : 'unknown',
    'quantity': 0,
    'price' : 0,
}
data.fillna(value=fill_values, inplace=True)

# -------------------------------------------------------------------------
# 4. Correct Data Types
# -------------------------------------------------------------------------
# Convert date column to datetime
data['order_date'] = pd.to_datetime(data['order_date'], format='%Y-%m-%d')

# -------------------------------------------------------------------------
# 5. Summary Metrics
# -------------------------------------------------------------------------
logging.info('Generating summary metrics...')
total_revenue = data['total_amount'].sum()
total_orders = data['transaction_id'].count()
total_quantity_sold = data['quantity'].sum()
average_order_value = total_revenue/total_orders
average_product_price = data['price'].mean()
total_customers = data['customer_id'].nunique()

summary_df = pd.DataFrame({
    'total_revenue': [total_revenue],
    'total_orders': [total_orders],
    'total_quantity_sold': [total_quantity_sold],
    'average_order_value': [average_order_value],
    'average_product_price': [average_product_price],
    'total_customers': [total_customers]
})

# -------------------------------------------------------------------------
# 6. Group-wise Aggregation
# -------------------------------------------------------------------------
logging.info('Performing group-wise aggregation...')

category_revenue = data.groupby('category')['total_amount'] \
     .sum().sort_values(ascending=False).reset_index()
     
region_revenue = data.groupby('region')['total_amount'] \
    .sum().sort_values(ascending=False).reset_index()

# -------------------------------------------------------------------------
# 7. Date-based Analysis
# -------------------------------------------------------------------------
logging.info('Performing date-based analysis...')

year_wise_revenue = data.groupby(data['order_date'].dt.year) \
    ['total_amount'].sum().reset_index()
    
monthly_revenue = data.groupby(data['order_date'].dt.to_period('M')) \
    ['total_amount'].sum().reset_index()

# -------------------------------------------------------------------------
# 8. Store processed data back into S3
# -------------------------------------------------------------------------
logging.info('Uploading processed data to S3...')

summary_df.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'summary_metrics.parquet', index=False)

category_revenue.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'category_revenue.parquet', index=False)

region_revenue.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'region_revenue.parquet', index=False)

year_wise_revenue.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'year_wise_revenue.parquet', index=False)

monthly_revenue.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'monthly_revenue.parquet', index=False)

data.to_parquet(f's3://{settings.BUCKET_NAME}/processed/'
    f'sales_dataset_cleaned.parquet', index=False)