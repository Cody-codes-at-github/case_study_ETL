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

    def get_transactions_table():
        transaction = df_sales.merge(right=df_salesman[['Salesman Code', 'Salesman Name', 'Active']], how='left',
                                     left_on='Salesman Code', right_on='Salesman Code')
        transaction.rename(columns={'Active': 'Salesman Active'}, inplace=True)
        transaction.sort_values(by='Bill Date', ascending=True, inplace=True)
        transaction.reset_index(drop=True, inplace=True)
        return transaction


    def get_members_table():
        member = df_customers
        member.rename(columns={'Active': 'Member Active'}, inplace=True)
        member.sort_values(by=['Date Created', 'Date Updated'], ascending=True, inplace=True)
        member.reset_index(drop=True, inplace=True)
        return member


    def get_inventory_table():
        inventory = df_stocks.merge(right=df_products, how='outer', left_on=['Product Code', 'Product Unit'],
                                    right_on=['Product Code', 'Product Unit'])
        inventory['Product Name'] = inventory['Product Name_x'].combine_first(inventory['Product Name_y'])
        inventory['Distrib_ID'] = inventory['Distrib ID_x'].combine_first(inventory['Distrib ID_y'])
        inventory.drop(columns=['Distrib ID_x', 'Distrib ID_y', 'Product Name_x', 'Product Name_y'], inplace=True)
        inventory['Product Qty'].fillna(0, inplace=True)
        inventory.sort_values(by='Inventory Date', ascending=True, inplace=True)
        inventory.reset_index(drop=True, inplace=True)
        return inventory


    transactions = get_transactions_table()
    members = get_members_table()
    inventory = get_inventory_table()

    def to_sql():
        for df, name in zip([transactions, members, inventory], ['Transactions', 'Members', 'Inventory']):
            load_to_sql(df=df, name=name, db='Store', password='#####', if_exists='replace')

    to_sql()
    print('Completed')
