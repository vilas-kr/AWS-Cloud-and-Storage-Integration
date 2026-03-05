# Part 6: SQL Practice & Analytics
# Candidates should write and explain SQL queries for:
# Aggregations (SUM, AVG, COUNT)
# GROUP BY and HAVING clauses
# Date-based filtering
# Sorting and ranking results
# Identifying top or bottom performers based on metrics

import logging

from config.db_connection import get_connection

logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------------------
# Establish a connection to RDS SQL server database using pyodbc
# -------------------------------------------------------------------------
conn = get_connection()
logging.info('Connected to RDS SQL server database')

cursor = conn.cursor()
# -------------------------------------------------------------------------
# 1. Aggregations
# -------------------------------------------------------------------------
logging.info('Performing aggregations...')
aggregations_query = '''
SELECT 
    SUM(total_amount) AS total_revenue,
    AVG(price) AS average_price,
    COUNT(*) AS total_orders
FROM orders.sales;
'''
cursor.execute(aggregations_query)
aggregations_result = cursor.fetchone()
logging.info(f'Aggregations result: ')
logging.info(f'Total Revenue: {aggregations_result.total_revenue}')
logging.info(f'Average Price: {aggregations_result.average_price}')
logging.info(f'Total Orders: {aggregations_result.total_orders}')
# -------------------------------------------------------------------------
# 2. Group By and Having Clauses
# -------------------------------------------------------------------------
logging.info('Performing GROUP BY and HAVING operations...')
group_by_query = '''
SELECT category, SUM(total_amount) AS category_revenue
FROM orders.sales
GROUP BY category
HAVING SUM(total_amount) > 10000;
'''
cursor.execute(group_by_query)
group_by_result = cursor.fetchall()
logging.info(f'Group By result:')
for row in group_by_result:
    logging.info(f'Category: {row.category}, Total Revenue: {row.category_revenue}')

# -------------------------------------------------------------------------
# 3. Date-based Filtering
# -------------------------------------------------------------------------
logging.info('Performing date-based filtering...')
date_filter_query = '''
SELECT *
FROM orders.sales
WHERE order_date >= '2023-01-01' AND order_date < '2024-01-01';
'''
cursor.execute(date_filter_query)
date_filter_result = cursor.fetchall()
logging.info(f'Date-based filtering result: {len(date_filter_result)} '
             f'records found')

# -------------------------------------------------------------------------
# 4. Sorting and Ranking Results
# -------------------------------------------------------------------------
logging.info('Performing sorting and ranking...')
sorting_query = '''
SELECT category, SUM(total_amount) AS category_revenue, 
    rank() OVER (ORDER BY SUM(total_amount) DESC) AS revenue_rank
FROM orders.sales
GROUP BY category
'''
cursor.execute(sorting_query)
sorting_result = cursor.fetchall()
logging.info(f'Sorting and ranking result:')
for row in sorting_result:
    logging.info(f'Category: {row.category}, Revenue: {row.category_revenue}, '
                 f'Rank: {row.revenue_rank}')

# -------------------------------------------------------------------------
# 5. Identifying Top Performers
# -------------------------------------------------------------------------
logging.info('Identifying top performers...')
top_performers_query = '''
SELECT TOP(5) product_name, SUM(total_amount) AS product_revenue
FROM orders.sales
GROUP BY product_name
ORDER BY product_revenue DESC;
'''
cursor.execute(top_performers_query)
top_performers_result = cursor.fetchall()
logging.info(f'Top selling products result:')
for row in top_performers_result:
    logging.info(f'Product: {row.product_name}, Revenue: {row.product_revenue}')

# Close the database connection
conn.close()
logging.info('Database connection closed')


