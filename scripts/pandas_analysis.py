import pandas as pd
from scripts.pyshpark_transformation import convert_into_pandas
import  matplotlib.pyplot as plt

# Load transformed pandas Dataframes
dfs=convert_into_pandas()

vendors_metrics=dfs["vendor_metrics"]
total_purchase=dfs["total_purchase"]
total_sales=dfs["total_sales"]
inventory=dfs["inventory"]
top_product=dfs["top_products"]
top_vendors=dfs["top_vendors"]
year_wise_sales=dfs["year_wise_sales"]
top_monthly=dfs["top_monthly"]
top_5_product=dfs["top_5_products"]
least_5=dfs["least_5"]
top_purchases_product=dfs["top_product_purchases"]

print("All Transformation load into pandas successfully")


# ----------------------------- VISUALIZATIONS -----------------------------

# Lead Time
plt.figure(figsize=(10,6))
x_positions = range(len(vendors_metrics))
plt.bar(x_positions, vendors_metrics["avg_lead_time_days"], color="red")
plt.xticks(x_positions, vendors_metrics["VendorNumber"], rotation=45)
plt.xlabel("Vendor Number")
plt.ylabel("Average Lead Time (Days)")
plt.title("Lead Time by Vendor")
plt.tight_layout()
plt.show()


# Invoice Delay
plt.figure(figsize=(10,6))
x_pos = range(len(vendors_metrics))
plt.bar(x_pos, vendors_metrics["avg_invoice_delay_days"], color="orange")
plt.xticks(x_pos, vendors_metrics["VendorNumber"], rotation=45)
plt.xlabel("Vendor Number")
plt.ylabel("Delay (Days)")
plt.title("Invoice Delay by Vendor")
plt.tight_layout()
plt.show()

# Top Seller product
import squarify
plt.figure(figsize=(12,6))
squarify.plot(sizes=top_product["total_sales"], label=top_product["InventoryId"], alpha=0.8)
plt.axis("off")
plt.title("Top Selling Products (Treemap)")
plt.show()

# --- Monthly Sales Trend ----
monthly_group = top_monthly.groupby("month")["total_sales"].sum()
plt.figure(figsize=(8,5))
plt.plot(monthly_group.index, monthly_group.values, marker="o", color="green")
plt.title("Monthly Sales Trend")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.grid(True)
plt.tight_layout()
plt.show()

# ----------------------------- TOP 5 PRODUCTS -----------------------------
plt.figure(figsize=(10, 6))
plt.bar(top_5_product["InventoryId"], top_5_product["total_sales"], color="#4CAF50")
plt.title("Top 5 Products (Year-wise Highest Sales)")
plt.xlabel("Product")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ----------------------------- LEAST 5 PRODUCTS -----------------------------
plt.figure(figsize=(10, 6))
plt.bar(least_5["InventoryId"], least_5["total_sales"], color="#E63946")
plt.title("Least 5 Products (Year-wise Lowest Sales)")
plt.xlabel("Product")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

