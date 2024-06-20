from DataProcess import Data

path_json = 'first_company_data.json'
path_csv = 'second_company_data.csv'

first_company_data = Data.read_data(path_json, 'json')
print(first_company_data.column_names)
print(first_company_data.row_count)

second_company_data = Data.read_data(path_csv, 'csv')
print(second_company_data.column_names)
print(second_company_data.row_count)

# Transform
key_mapping = {
    'Nome do Item': 'Product Name',
    'Classificação do Produto': 'Product Category',
    'Valor em Reais (R$)': 'Product Price (R$)',
    'Quantidade em Estoque': 'Stock Quantity',
    'Nome da Loja': 'Store Name',
    'Data da Venda': 'Sale Date'
}

second_company_data.rename_columns(key_mapping)
print(second_company_data.column_names)

merged_data = Data.join(first_company_data, second_company_data)
print(merged_data.column_names)
print(merged_data.row_count)

path_combined_data = 'data_processed/combined_data.csv'
merged_data.save_data(path_combined_data)
print(path_combined_data)
