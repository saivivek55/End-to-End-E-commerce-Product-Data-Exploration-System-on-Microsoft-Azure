#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
import numpy as np

# concate the dataframes 
file_paths = ['data/merge_gift_cards_final.csv', 'data/merge_cellphone_final.csv', 'data/merge_grocery_final.csv', 'data/merge_lux_final.csv']

# Create an empty list to store the DataFrames
dfs = []

columns_with_mixed_types = [16, 17]
# Loop through the file paths and read each CSV file into a DataFrame
for file_path in file_paths:
    df_sample = pd.read_csv(file_path, dtype={col: str for col in columns_with_mixed_types})
    dfs.append(df_sample)

# Concatenate the list of DataFrames along rows
df = pd.concat(dfs, axis=0, ignore_index=True)


# In[2]:


df['reviewTime'] = pd.to_datetime(df['reviewTime'], errors='coerce')

# Format the 'date' column to the desired format 'yyyy-mm-dd'
df['reviewTime'] = df['reviewTime'].dt.strftime('%Y-%m-%d')
df['reviewTime'] = df['reviewTime'].astype(str)

new_column_names = {'rank': 'sales_rank'}
df = df.rename(columns=new_column_names)


# In[3]:


def split_dataframe_and_save_csv(df):
    product_df = df[["asin", "title", "description", "price", "brand", "sales_rank", "main_cat", "sub-category", "product_image"]]
    product_df = product_df.drop_duplicates()
    new_column_names = {'sub-category': 'sub_category'}
    product_df = product_df.rename(columns=new_column_names)
    product_df.to_csv("dim_product.csv", index=False)
    
    dim_reviewer_df = df[["reviewerID", "reviewerName"]]
    dim_reviewer_df = dim_reviewer_df.drop_duplicates()
    new_column_names = {'reviewerID': 'reviewer_id', 'reviewerName': 'reviewer_name'}
    dim_reviewer_df = dim_reviewer_df.rename(columns=new_column_names)
    dim_reviewer_df.to_csv("dim_reviewer.csv", index=False) 
    
    
    dim_time_df = df[["unixReviewTime", "reviewTime"]]
    dim_time_df = dim_time_df.drop_duplicates()
    new_column_names = {'unixReviewTime': 'unixreview_time', 'reviewTime': 'review_time'}
    dim_time_df = dim_time_df.rename(columns=new_column_names)
    dim_time_df.to_csv("dim_time.csv", index=False)
    
    fact_df = df[["asin", "reviewerID", "unixReviewTime", "overall", "verified", "vote", "summary"]]
    fact_df = fact_df.drop_duplicates()
    # modifying the boolean values 
    fact_df['verified'] = np.where(fact_df['verified']== 'True', 1, 0)
    # Reset the index starting from 1
    fact_df.index = range(1, len(fact_df) + 1)
    #Renaming column names 
    new_column_names = {'reviewerID': 'reviewer_id', 'overall': 'rating_id', 'unixReviewTime': 'time_id'}
    fact_df = fact_df.rename(columns=new_column_names)
    fact_df.to_csv("fact_review.csv", index_label="review_id")
    
    dim_rating_df = df[["overall"]]
    dim_rating_df = dim_rating_df.drop_duplicates()
    dim_rating_df.to_csv("dim_rating.csv", index=False)
    


# In[4]:


split_dataframe_and_save_csv(df)


# In[ ]:




