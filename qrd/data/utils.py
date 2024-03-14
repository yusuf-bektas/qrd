import pandas as pd
import numpy as np
import re
import os

"""
This file contains utility functions for the project.
"""

def read_data(path, file_name, px_type=np.int32, qty_type=np.int32, vol_type=np.int32):
    df=pd.read_csv(os.path.join(path,file_name),index_col=0)
    df.index=pd.to_datetime(df.index)
    df=df.drop(columns=[col for col in df.columns if "unnamed" in col.lower()])
    df['volume']=df.volume.astype(vol_type)
    df['teo']=df.teo.astype(px_type)
    for col in df.columns:
        if "px" in col.lower():
            df[col]=df[col].astype(px_type)
        elif "qty" in col.lower():
            df[col]=df[col].astype(qty_type)
    return df

def read_spot(path, file_name):  
    spot = read_data(path, file_name)
    return spot

def read_futures(path,file_name):
    fut=read_data(path, file_name)
    for col in fut.columns:
        if "px" in col.lower():
            fut[col]=fut[col]*10
    return fut

#excluding the rows after devre kesici

def filter_rows(group, col='flag', flag='P_DK_TEKFIY_EMIR_TPL'):
    flag_index = group[group['flag'] == flag].index.min()
    if pd.isna(flag_index):
        return group
    return group.loc[:flag_index - 1]

def filter_dates(dt_index, df):
    daily_bounds = dt_index.to_series().groupby(dt_index.date).agg(['min', 'max'])

    # Merge the original DataFrame with the daily bounds
    index_name=df.index.name
    df = df.reset_index().rename(columns={index_name: 'DateTime'})
    df['Date'] = df['DateTime'].dt.date

    daily_bounds = daily_bounds.reset_index().rename(columns={'index': 'Date'})
    merged_df = pd.merge(df, daily_bounds, on='Date', how='left')

    # Keep only the rows where the time is within the bounds
    filtered_df = merged_df[(merged_df['DateTime'] >= merged_df['min']) & 
                            (merged_df['DateTime'] <= merged_df['max'])].set_index('DateTime')

    # Return the filtered DataFrame without the additional columns
    return filtered_df.drop(columns=['Date', 'min', 'max'])

def extract_messages(messages_series : pd.Series):
    """
    Extracts data from messages using an enhanced regex pattern.

    messages_df: DataFrame containing the messages.
    date_column: Name of the column containing date values.
    message_column: Name of the column containing message strings.
    date_value: Specific date value to filter messages on.
    
    returns: DataFrame with extracted message components.
    """

    pattern = r"""
        (?P<Type>[A-Z])-(?P<Direction>[BS]);\s*  # Captures Type and Direction with a dash in between
        (?P<px>\d+);\s*                          # Captures px
        (?P<qty>\d+);\s*                         # Captures qty
        (?P<id>\d+)                              # Captures id directly without a following semicolon
        |                                        # 
        (?P<Type_O>[A-Z]);\s*                    # Captures Type_O for messages without Direction
        (?P<flag>[^)]+)                          # Captures flag for Type_O messages
    """
    extracted_data = messages_series.str.extractall(pattern, flags=re.VERBOSE)

    # Post-processing to clean and format extracted data
    extracted_data['Type'] = extracted_data['Type'].fillna(extracted_data['Type_O'])
    extracted_data.drop(columns=['Type_O'], inplace=True)
    extracted_data['Direction'] = extracted_data['Direction'].fillna('DNE')
    extracted_data['flag'] = extracted_data['flag'].fillna('NONE')
    extracted_data['px'] = pd.to_numeric(extracted_data['px'], errors='coerce').fillna(0)
    extracted_data['qty'] = pd.to_numeric(extracted_data['qty'], errors='coerce').fillna(0)
    extracted_data['id'] = extracted_data['id'].fillna(0).astype(np.int64)

    extracted_data.loc[(extracted_data.id.diff()==0) & (extracted_data.Type=='A') & (extracted_data.qty.diff()<0) & (extracted_data.px.diff()==0),'flag']='SIZE_REDUCTION'
    extracted_data.loc[(extracted_data.id.diff(-1)==0) & (extracted_data.Type=='D') & (extracted_data.qty.diff(-1)>0) & (extracted_data.px.diff(-1)==0),'flag']='SIZE_REDUCTION'
    extracted_data[extracted_data.flag=='SIZE_REDUCTION']
    return extracted_data.reset_index(level=1, drop=True)