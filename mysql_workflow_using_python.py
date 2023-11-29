from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
from sqlalchemy import create_engine

def sql_connection_setup():
    try:
        with connect(
                host="localhost",
                user=input("Enter username: ", ),
                password=getpass("Enter password: "),
                buffered=True,
        ) as connection:
            print(f"connection has established : {connection.is_connected()}")

            # Check for available dbs in mysql before creating a new db
            c = connection.cursor()  # cursor makes use of a MySQLConnection to interact with MySQL server.
            c.execute("SHOW DATABASES")  # sql query to show databases
            db_list = c.fetchall()
            print("Available Databases are as below:")
            print(*db_list, sep="\n")

            # Create new database
            c.execute("CREATE DATABASE amazon_product_review_analysis")
            connection.commit()
            print("Database is created.")

            # Switch to the database
            c.execute("USE amazon_product_review_analysis")
            connection.commit()

            # Create dimension and fact tables
            # 1. Product dimension
            c.execute("""
            CREATE TABLE IF NOT EXISTS DimProduct (
            asin VARCHAR(255) PRIMARY KEY,
            title TEXT,
            price DECIMAL(10, 2),
            brand VARCHAR(255),
            sales_rank DECIMAL(10,2),
            main_cat VARCHAR(255),
            sub_category VARCHAR(255),
            product_image VARCHAR(255)
            );
            """)

            # 2. Reviewer dimension
            c.execute("""
            CREATE TABLE IF NOT EXISTS DimReviewer (
            reviewer_id VARCHAR(255) PRIMARY KEY,
            reviewer_name VARCHAR(255)
            );
            """)

            # 3. Time dimension
            c.execute("""
            CREATE TABLE IF NOT EXISTS DimTime (
            unixreview_time varchar(255) PRIMARY KEY,
            review_time DATE
            );
            """)

            # 4. Rating dimension
            c.execute("""
            CREATE TABLE IF NOT EXISTS DimRating (
            overall INT PRIMARY KEY
            );
            """)

            #Create Fact Table - Review fact
            c.execute('''
                CREATE TABLE FactReview (
                    review_id INT PRIMARY KEY,
                    asin VARCHAR(255),
                    reviewer_id VARCHAR(255),
                    time_id varchar(255),
                    rating_id INT,
                    verified BOOLEAN,
                    vote INT,
                    summary TEXT,
                    FOREIGN KEY (asin) REFERENCES DimProduct(asin),
                    FOREIGN KEY (reviewer_id) REFERENCES DimReviewer(reviewer_id),
                    FOREIGN KEY (time_id) REFERENCES DimTime(unixreview_time),
                    FOREIGN KEY (rating_id) REFERENCES DimRating(overall)
                );
            ''')

            # Read CSV into DataFrame
           # giftcards_csv_file_path = 'D:\Amazon db project data\My cleaned data\Product_review_join_code\merge_gift_cards_final.csv'
           # df = pd.read_csv(giftcards_csv_file_path)

            # List of your CSV files
            csv_files = ['D:\Amazon db project data\My cleaned data\Product_review_join_code\merge_cellphone_final.csv',
                         'D:\Amazon db project data\My cleaned data\Product_review_join_code\merge_gift_cards_final.csv',
                         'D:\Amazon db project data\My cleaned data\Product_review_join_code\merge_grocery_final.csv',
                         'D:\Amazon db project data\My cleaned data\Product_review_join_code\merge_lux_final.csv']

            # Load all CSV data into a list of DataFrames
            dfs = [pd.read_csv(file, low_memory=False) for file in csv_files]

            # Merge DataFrames into a single DataFrame
            merged_df = pd.concat(dfs, ignore_index=True)

            # Create SQLAlchemy engine from MySQL connection
            engine = create_engine('mysql+mysqlconnector://{}:{}@localhost/amazon_product_review_analysis'.format(
                input("Enter username: "),
                getpass("Enter password: ")
            ))

            # Load data into Dimension Tables
            df_product = merged_df[
                ['asin', 'title', 'description', 'price', 'brand', 'sales_rank', 'main_cat', 'sub-category', 'product_image']]
            df_product.to_sql('DimProduct', con=engine, if_exists='replace', index=False)

            df_reviewer = merged_df[['reviewerID', 'reviewerName']]
            df_reviewer.to_sql('DimReviewer', con=engine, if_exists='replace', index=False)

            df_time = merged_df[['reviewTime', 'unixReviewTime']]
            df_time['reviewTime'] = pd.to_datetime(df_time['reviewTime']).dt.date
            df_time.to_sql('DimTime', con=engine, if_exists='replace', index=False)

            df_rating = merged_df[['overall']]
            df_rating.to_sql('DimRating', con=engine, if_exists='replace', index=False)

            # Load data into Fact Table
            df_fact = merged_df[['asin', 'reviewerID ', 'unixReviewTime ', 'overall', 'verified', 'vote', 'summary']]
            #df_fact['reviewTime'] = pd.to_datetime(df_fact['reviewTime']).dt.date
            df_fact.to_sql('FactReview', con=engine, if_exists='replace', index=False)

            print("Data loaded into MySQL successfully.")

            # Commit changes
            connection.commit()


    except Error as e:
        print(e)


def main():
    # sql connection setup
    sql_connection_setup()


if __name__ == '__main__':
    main()
