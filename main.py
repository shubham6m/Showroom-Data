import dask.dataframe as dd
import pandas as pd
import glob
from dask import delayed
import matplotlib.pyplot as plt

file_pattern = "./Showroom Data/Showroom_*_sales.xlsx"
files = glob.glob(file_pattern)

# Use dask.delayed to read each file lazily
dfs = [delayed(pd.read_excel)(file, usecols=["Showroom_Name", "Price", "Car_Model","Car_Type", "Employee_Name","Day","Month","Date","Customer_Gender","Customer_Age","Location"]) for file in files]

# Convert list of delayed pandas DataFrames into a dask DataFrame
df = dd.from_delayed(dfs)

# Which showroom is performing the best in terms of total revenue?
showroom_revenue = df.groupby("Showroom_Name")["Price"].sum().compute()
best_showroom = showroom_revenue.idxmax()
best_revenue = showroom_revenue.max()
print(f"Best showroom by revenue: {best_showroom} with total revenue of {best_revenue}")

#Which showroom is selling the highest number of cars?
showroom_sales = df.groupby("Showroom_Name").size().compute()
top_selling_showroom = showroom_sales.idxmax()
most_cars_sold = showroom_sales.max()
print(f"Showroom with most cars sold: {top_selling_showroom} with {most_cars_sold} cars sold")

# Which car model is sold the most?
most_sold_car_model = df.groupby("Car_Model").size().compute().idxmax()
print(f"Most sold car model: {most_sold_car_model}")

# What car type (e.g., Sedan, SUV, Hatchback) is sold the most? 
most_sold_car_type = df.groupby("Car_Type").size().compute().idxmax()
print(f"Most sold car type: {most_sold_car_type}")

# Which employee is selling the most cars?
most_car_sold_by_emp = df.groupby("Employee_Name").size().compute().idxmax()
print(f"Employee who selling the most car: {most_car_sold_by_emp}")

# Which employee generates the highest revenue?
highest_revenue_by_emp = df.groupby("Employee_Name")["Price"].sum().compute().idxmax()
print(f"The employee who generates more revenue: {highest_revenue_by_emp}")

# On which day of the week are cars sold the most?
df["Day_of_Week"] = dd.to_datetime(df["Date"]).dt.day_name()
most_cars_sold_day_of_week = df.groupby("Day_of_Week").size().compute().idxmax()
print(f"Most cars sold on day of the week: {most_cars_sold_day_of_week}")

most_car_sold_on_day = df.groupby("Day").size().compute().idxmax()
print(f"Most card sold on day: {most_car_sold_on_day}")

# On which day of the month are cars sold the most? 
df["Day_of_Month"] = dd.to_datetime(df["Date"]).dt.day
most_cars_sold_day_of_month = df.groupby("Day_of_Month").size().compute().idxmax()
print(f"Most cars sold on day of the month: {most_cars_sold_day_of_month}")


#Which month has the highest sales?
df["Month"] = dd.to_datetime(df["Date"]).dt.month_name()
most_cars_sold_month = df.groupby("Month").size().compute().idxmax()
print(f"Most cars sold on month: {most_cars_sold_month}")

# Determine the most common age group of buyers
most_common_age_group = df.groupby("Customer_Age").size().compute().idxmax()
print(f"Most common age group of buyers: {most_common_age_group}")

# Which gender purchases cars the most?
most_common_gender = df.groupby("Customer_Gender").size().compute().idxmax()
print(f"Gender, who purchases the most cars: {most_common_gender}")

# What is the average purchase amount per customer?
average_purchase_amount = df.groupby("Customer_Age")["Price"].mean().compute()
average_purchase_amount = average_purchase_amount.mean()
print(f"Average purchase amount per customer: {average_purchase_amount}")

# Which showroom location attracts the most customers?
best_location = df.groupby("Location").size().compute().idxmax()
print(f"The location which attracts the most customers: {best_location}")

# Analyze correlation between showroom location and revenue
location_revenue = df.groupby("Location")["Price"].sum().compute()

# Display revenue by location
print("Revenue by location:")
print(location_revenue)
location_revenue.plot(kind='bar', figsize=(10, 6), title="Revenue by Showroom Location")
plt.xlabel("Location")
plt.ylabel("Total Revenue")
plt.show()