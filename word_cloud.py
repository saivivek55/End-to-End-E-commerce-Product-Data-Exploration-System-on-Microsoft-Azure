import streamlit as st
import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Function to generate word cloud
def generate_wordcloud(reviews_text):
    wordcloud = WordCloud(width=800, height=400, random_state=21, max_font_size=110, background_color='white').generate(reviews_text)
    plt.figure(figsize=(10, 7))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis('off')
    st.pyplot()


def display_product_metadata(metadata):
    st.subheader("Product Metadata")
    st.image(metadata['product_image'].values[0])
    st.write    (f"**Title:** {metadata['title'].values[0]}  \n**Price:** {metadata['price'].values[0]}  \n**Brand:** {metadata['brand'].values[0]}")


# Main function to run the Streamlit app
def main():
    st.title("Customer Reviews Word Cloud")
    st.set_option('deprecation.showPyplotGlobalUse', False)

    # Upload CSV file
    fact_data_file = "fact_review.csv" 

    # Product meta data file 
    product_meta_data = "dim_product.csv"

    if fact_data_file is not None:
        # Read CSV file into a Pandas DataFrame
        df = pd.read_csv(fact_data_file)
        meta_data_df = pd.read_csv(product_meta_data)

        # Select ASIN
        asin_options = df['asin'].unique()
        selected_asin = st.selectbox("Select ASIN", asin_options)

        # # # Display the DataFrame
        # st.subheader("ASIN details")
        # st.write(meta_data_df[meta_data_df['asin'] == selected_asin])

        meta_data_df = meta_data_df[meta_data_df['asin'] == selected_asin]
        display_product_metadata(meta_data_df)

        # Filter reviews based on selected ASIN
        selected_reviews = df[df['asin'] == selected_asin]['summary'].str.cat(sep=' ')

        # Display word cloud
        st.subheader("Word Cloud for Selected Product")
        generate_wordcloud(selected_reviews)


if __name__ == "__main__":
    main()
