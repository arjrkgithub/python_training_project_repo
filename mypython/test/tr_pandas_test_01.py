import pandas as pd

# Read the CSV file into a pandas DataFrame
file_path = r'C:\Users\hp sys\Downloads\mall_customers.csv'
df = pd.read_csv(file_path)

# You can perform other aggregate operations similarly, for example:
# Total spending_score for each customer
total_spending_by_customer = df.groupby('customer_id')['spending_score'].sum()
print("\nTotal spending score for each customer:")
print(total_spending_by_customer)
type(total_spending_by_customer)

