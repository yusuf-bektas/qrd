import pandas as pd
import numpy as np
import re
import os

"""
This file contains utility functions for the project.
"""

def read_data(path, file_name, px_type=np.uint32, qty_type=np.uint32, vol_type=np.uint32):
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


def parse_messages(series: pd.Series):
    """
    This function assumes that the input series contains messages in the format and indexed by time
    """
    # Initialize dictionary with lists for each key
    parsed_data = {
        "date":[],
        "Type": [],
        "Direction": [],
        "px": [],
        "qty": [],
        "id": [],
        "flag": []
    }
    for date_time, messages in series.items():
        segments = messages.strip().split(') ')
        # Parse each segment and add to the dictionary
        for segment in segments:
            parts = segment.replace('(', '').replace(')', '').split('; ')
            # Determine if the segment type is 'O'
            if parts[0] == 'O':
                type_ = 'O'
                direction = 'DNE'
                px = np.uint32(0)
                qty = np.uint32(0)
                id_ = np.uint64(0)
                flag = parts[1].strip()
            else:
                # Parse other types (A, E, D)
                type_ = parts[0].split('-')[0]  # Type
                direction = parts[0].split('-')[1]  # Direction
                px = np.uint32(parts[1])  # px
                qty = np.uint32(parts[2])  # qty
                id_ = np.uint64(parts[3])  # id
                flag = 'NONE'  # flag (default)
            parsed_data["date"].append(date_time)
            parsed_data["Type"].append(type_)
            parsed_data["Direction"].append(direction)
            parsed_data["px"].append(px)
            parsed_data["qty"].append(qty)
            parsed_data["id"].append(id_)
            parsed_data["flag"].append(flag)
    return pd.DataFrame(parsed_data)    
    

