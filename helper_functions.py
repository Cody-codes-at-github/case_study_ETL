import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import os


def load_csv(path, file_name):
    """
    Loads csv files and returns a data frame
    :param path: file path without file name
    :param file_name: file name
    :return: data frame
    """
    return pd.read_csv(os.path.join(path, file_name))


def load_to_sql(df, name, db, password, if_exists):
    """
    Load data frame to sql
    :param df: data frame
    :param name: table name
    :param db: target data base
    :param password: data base user password
    :param if_exists: action if table already exist
    :return: None
    """
    engine = create_engine(f'postgresql://postgres:{password}@localhost:5432/{db}')
    df.to_sql(name=name, con=engine, index=False, if_exists=if_exists)
    print(f'Done loading {name} to sql')


def transform_customers(df):
    """
    Cleans customer table
    :param df: customer table
    :return: Processed customer table
    """
    if df.duplicated(keep='first').sum() != 0:
        df.drop_duplicated(keep='first', inplace=True)

    df.dropna(axis=0, thresh=7, inplace=True)

    if df.isnull().sum().loc['Email'] != 0:
        df['Email'].fillna('Not provided', inplace=True)

    if df.isnull().sum().loc['Phone'] != 0:
        df['Phone'].fillna('Not provided', inplace=True)

    if df['Date Created'].dtype != '<M8[ns]':
        df['Date Created'] = pd.to_datetime(df['Date Created'])

    if df['Date Updated'].dtype != '<M8[ns]':
        df['Date Updated'] = pd.to_datetime(df['Date Updated'])

    return df


def transform_stocks(df):
    """
    Cleans stock table
    :param df: stock table
    :return: Processed stock table
    """
    if df.duplicated(keep='first').sum() != 0:
        df.drop_duplicated(keep='first', inplace=True)

    if df['Inventory Date'].dtype != '<M8[ns]':
        df['Inventory Date'] = pd.to_datetime(df['Inventory Date'])

    return df


def transform_sales(df):
    """
    Cleans Sales table
    :param df: Sales table
    :return: Processed Sales table
    """
    if df.duplicated(keep='first').sum() != 0:
        df.drop_duplicates(keep='first', inplace=True)

    df.rename(columns={'Product Qty': 'Qty Sold'}, inplace=True)

    if df['Bill Date'].dtype != '<M8[ns]':
        df['Bill Date'] = pd.to_datetime(df['Bill Date'])

    return df



