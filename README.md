# 🛒 End-to-End E-commerce Product Data Exploration on Microsoft Azure

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)  
A complete data engineering and analytics pipeline built on Microsoft Azure to analyze 3.14M product ratings and 994K votes across 8,019 brands. The project integrates Azure services with Python, SQL, and Power BI to uncover key sales insights, data quality gaps, and consumer behavior trends.

---

## 🚀 Project Highlights

- ✅ Built an automated pipeline using **Azure Data Factory** for ingesting large-scale datasets from **GitHub** and **JSON**.
- 🧪 Cleaned and transformed raw data using **PySpark in Databricks**, with business logic applied through SQL.
- 🧊 Stored structured data in **Azure Synapse Analytics** with a **Star Schema** for optimized analytics.
- 📊 Created an **interactive Power BI dashboard** to visualize top brands, ratings distribution, review trends, and verified purchases.
- ⚙️ Leveraged **Azure Data Lake Storage Gen2** for scalable and secure data handling.

---

## 🛠️ Tech Stack

**Cloud & Tools:**  
☁️ Azure Data Factory · Azure Data Lake Storage Gen2 · Azure Synapse Analytics · Databricks · Power BI  
**Programming:**  
🐍 Python · SQL · PySpark  
**Data Sources:**  
📂 GitHub · JSON  

---

## 🔄 Project Workflow

> A high-level overview of the full data flow architecture:

![Workflow](./Workflow%20-%20Microsoft%20Azure.jpg)

---

## 🗃️ Data Modeling

- Applied **Star Schema** design using **Azure Synapse**, separating fact and dimension tables to support analytical queries and Power BI performance.

![Star Schema](./Star%20Schema.jpeg)

---

## 📈 Power BI Dashboard

- Designed an **interactive Power BI dashboard** to visualize:
  - 🔟 Top 10 brands with average rating 5
  - 🕐 Ratings by year and average price
  - ✅ Verified vs non-verified reviews
  - 📊 Count of each rating from 1–5
  - 📦 Product counts and reviewer distribution

> Screenshot of the Power BI dashboard:

![Power BI Dashboard](./E-Commerce%20Dashboard.jpg)

---
📊 **How to Explore the Dashboard**  
🖥️ Download & Open the pbix file using Power BI Desktop

---

### 🔍 Explore the interactive visuals and insights

- 🔑 **Key Insights** 🧮 1.14M ratings analyzed across 35.45K products and 8,019 brands  
- 💳 Verified purchases represent ~88% of total transactions  
- 🕒 Most reviews occurred post-2015, peaking between 2016–2018  
- ⭐ Rating 5 dominated (0.79M reviews), with verified reviews receiving more upvotes  
- ❗ Outliers found in brand naming, average prices, and review counts  
- 💗 Ratings follow a skewed distribution; top 10 brands account for a large majority of 5-star reviews  

---

📄 **License**  
Licensed under the Apache 2.0 License.
