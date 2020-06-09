from helper_functions import load_csv, load_to_sql, transform_customers, transform_stocks, transform_sales


if __name__ == "__main__":

    path = 'C:/Users/zhimi/Desktop/Case study/Data Engineering and Architecture Case'

    df_customers = load_csv(path=path, file_name='Customer.csv')
    df_products = load_csv(path=path, file_name='Product.csv')
    df_sales = load_csv(path=path, file_name='Sales.csv')
    df_salesman = load_csv(path=path, file_name='Salesman.csv')
    df_stocks = load_csv(path=path, file_name='Stock.csv')

    df_customers = transform_customers(df_customers)
    df_stocks = transform_stocks(df_stocks)
    df_sales = transform_sales(df_sales)


    for df, name in zip([df_customers, df_products, df_sales, df_salesman, df_stocks], ['Customers', 'Products', 'Sales', 'Salesman', 'Stocks']):
        load_to_sql(df=df, name=name, db='Store', password='####', if_exists='replace')

    print('Completed')
