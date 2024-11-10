from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from datetime import datetime
import os
import pandas as pd
import logging


# Variables - adjust if necessary
QUERY_ID = 4242978
AGGS = ['vertical', 'protocol']
START_DATE = '2023-01-01 00:00:00'
END_DATE = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
# Set a specific end date
#END_DATE = '2024-11-10 00:00:00'


def extract(query_id: int, start_date: str, end_date: str) -> pd.DataFrame:
    """Extracts data from the Dune API for a given query ID and date range."""
    
    # Get API key from enviromnent and create a client to make requests to the API
    dune = DuneClient(os.getenv('DUNE_API_KEY'))
    
    # Set query_id and paramter for the request
    query = QueryBase(
        query_id=query_id,
        params=[
            QueryParameter.date_type(name='start_date', value=start_date),
            QueryParameter.date_type(name='end_date', value=start_date)
        ],
    )
    # Wait for query to finish
    logging.info('Results available at', query.url())
    
    # Save results in a dataframe
    df = dune.run_query_dataframe(query)
    
    # As alternative to save time and credits get the results from the last time the query was completed
    #df = dune.get_latest_result_dataframe(query_id)
    
    return df


def transform(df: pd.DataFrame, agg: str) -> pd.DataFrame:
    """Transforms the data, aggregating by week and specified aggregation level."""
    
    # Make sure block_date is in the right format
    df['block_date'] = pd.to_datetime(df['block_date'])
    
    # Add 'week' column to group data by week
    df['week'] = df['block_date'].dt.to_period('W')
    
    # Group the data by week and the provided agg (vertical/protocol in our case)
    summary = df.groupby(['week', agg]).agg({
        'safe_address': pd.Series.nunique,
        'tx_hash': 'count',
        'amount_usd': 'sum' 
        }).rename(columns={
            'safe_address': 'unique_safes',
            'tx_hash': 'total_transactions',
            'amount_usd': 'outgoing_tvp_usd'
        }).reset_index()
    
    return summary
          

def load(df: pd.DataFrame, aggs: list[str]) -> None:
    """Loads transformed data by aggregating and saving in parquet format."""
    
    # Loop through the different aggs 
    for agg in aggs:
        try:
            # Transform the provided data
            summary = transform(df, agg)
            
            # Safe the transformed data to a parquet file
            summary.to_parquet(agg+'_summary.parquet', index=False)
            
            # Safe the transformed data in newly created Dune table
            create_table(summary, agg)
            
        except:
            # Error logging
            logging.exception(f"Error in transformation for {agg}: {e}")

def top_k(agg: str, measure: str) -> pd.DataFrame:
    """Returns top-k ranked DataFrame based on the specified measure."""
    
    # Read the data from the parquet file
    df = pd.read_parquet(agg+'_summary.parquet')
    
    # Sort the data
    df = df.sort_values(['week', measure], ascending=[True, False])
    
    # Rank the data
    df['rank'] = df.groupby('week')[measure].rank(method='min', ascending=False)
    
    # Only keep the rows with the top 5 values - it could be more than 5 rows if two rows have the same number 
    df_ranked = df[df['rank'] <= 5].reset_index(drop=True)
    
    return df_ranked.drop(columns=['rank'])


def create_table(df: pd.DataFrame, agg: str) -> None:
    """Creates a new Dune table with the provided data."""
    
    # Get API key from enviromnent and create a client to make requests to the API
    dune = DuneClient(os.getenv('DUNE_API_KEY'))
    
    # Transform the data to a CSV format for the upload
    data = df.to_csv()
    
    # Create the table and with the uploaded data - if the table already exists it gets overwritten
    table = dune.upload_csv(
        data=str(data),
        description='Weekly TVP data for Safe accounts.',
        table_name='safe_tvp_'+agg,
        is_private=True
    )



def main():
    # Extract, transfrom and load the data 
    load(extract(QUERY_ID, START_DATE, END_DATE), AGGS)
    
    # Loop through the aggs and give the results for the analysis
    for agg in AGGS:
        try:
            logging.info(f'Top 5 TVP {agg} sorted by outgoing usd.')
            logging.info(top_k(agg, 'outgoing_tvp_usd'))
            logging.info(f'Top 5 TVP {agg} sorted by transaction count.')
            logging.info(top_k(agg, 'total_transactions'))
            
        # Error logging    
        except:
            logging.exception(f'Error in analysis for {agg}: {e}')
            
            
            
if __name__ == "__main__":
    main()