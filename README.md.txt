# Supply Chain Data Integration System

This project is an end-to-end implementation of supply chain data processing using PySpark, followed by analytics and visualization using Pandas and Matplotlib. It performs complete ETL, data transformation, reporting, and visualization on supply chain datasets.

## Technologies Used

- PySpark for ETL, data cleaning, and transformations
- Pandas for analytics and reporting
- Matplotlib for charts and graphs
- Squarify for treemap visualizations
- Python 3.x as the base language

## Key Features

### Data Cleaning
- Detects and handles null values
- Identifies and removes duplicates
- Converts date columns into proper formats
- Ensures correct schema inference

### PySpark Transformations
The project includes the following transformation outputs:
- Vendor metrics
- Average lead time calculation
- Invoice delay calculation
- Order cycle time calculation
- Product-level purchase and sales aggregation
- Remaining inventory calculation
- Top-selling products
- Top vendors
- Year-wise sales data
- Month-wise sales data

### Pandas Analytics
After converting Spark DataFrames into Pandas, the following analytics DataFrames are generated:
- vendor_metrics
- total_purchase
- total_sales
- inventory
- top_products
- top_vendors
- year_wise_sales
- top_monthly

### Visualizations
The project generates the following charts using Matplotlib and Squarify:
- Bar chart for vendor lead time
- Bar chart for invoice delay
- Treemap chart for top-selling products
- Line chart for monthly sales trend

## How to Run the Project

1. Install all required Python libraries (PySpark, Pandas, Matplotlib, Squarify).
2. Configure Java and Spark paths in your PySpark script.
3. Run the transformation script to generate cleaned and processed data.
4. Run the analytics and visualization script to generate charts and DataFrames.

## Insights Delivered

The project helps in understanding and analyzing:
- Supplier performance
- Inventory optimization
- Customer and product demand
- Yearly and monthly sales patterns
- Vendor-wise operational efficiency
- Top and bottom performing products

## Dataset Source

The dataset used is the Vendor Performance Supply Chain dataset from Kaggle.

## Conclusion

This project provides a complete supply chain analytics workflow using PySpark and Pandas. It helps in automating data transformation, generating insights, and creating visualizations that support decision-making in supply chain operations.
