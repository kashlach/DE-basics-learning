import pandas as pd
encoding = 'cp1251'

def df_to_csv(df, filename, encoding, index=False, folder='output'):
    import os

    os.makedirs(folder, exist_ok=True)

    filename = f'{filename}.csv' if not filename.endswith('.csv') else filename
    full_path = os.path.join(folder, filename)
    df.to_csv(full_path, index=index, encoding=encoding)
    print(f'  {filename}')
    return full_path

print('__Extract from csv__')

df_orders = pd.read_csv('orders.csv', delimiter=';', encoding=encoding)
df_districts = pd.read_csv('district.csv', delimiter=';', encoding=encoding)

#print(df_orders.describe())
#print(df_districts.describe())

print('\nСтрок в исходных файлах:')
print(f' Orders - {len(df_orders)}')
print(f' Districts - {len(df_districts)}')

print('\n__Transform__')
# удаляем полные дубликаты
df_orders = df_orders.drop_duplicates()

# провалидируем сумму
df_orders['Sales'] = df_orders['Sales'].fillna(0).apply(lambda x: x if x >= 0 else 0)

# строковые данные приведем к единообразию
string_cols = df_orders.select_dtypes(include=['object', 'string']).columns
df_orders[string_cols] = df_orders[string_cols].apply(
    lambda s: s.strip().lower() if isinstance(s, str) else s
)
df_orders['ClientStatus'] = df_orders['ClientStatus'].fillna('Не указан')

# удаляем дубликаты заказов
df_districts = df_districts.drop_duplicates(subset=['OrderID'])
df_districts['DeliveryDistrictName'] = df_districts['DeliveryDistrictName'].str.strip().str.lower()

print('\nСтрок после очистки дубликатов:')
print(f' Orders - {len(df_orders)}')
print(f' Districts - {len(df_districts)}')

# объединяем датасеты
df_merged = df_orders.merge(df_districts, on='OrderID', how='left')
print(f'\nСтрок после JOIN: {len(df_merged)}')
#print(f'Район доставки не заполнен у {df_merged['DeliveryDistrictName'].isna().sum()} строк')
      
print('\n__Analyze__')
sales_by_category = df_merged.groupby('ProductCategory').agg(
    total_sales=('Sales', 'sum'),
    avg_sale=('Sales', 'mean'),
    order_cnt=('OrderID', 'nunique')
).reset_index()
print('\nПродажи по категориям:')
print(sales_by_category.head(5), end='\n')

sales_by_distr = df_merged.groupby('DeliveryDistrictName').agg(
    total_sales=('Sales', 'sum'),
    avg_sale=('Sales', 'mean'),
    order_cnt=('OrderID', 'nunique')
).reset_index()
print('\nПродажи по районам:')
print(sales_by_distr.head(5), end='\n')

group_by_distr_prod = df_merged.groupby(['DeliveryDistrictName', 'ProductName']).agg(
    total_sales=('Sales', 'sum')
).reset_index()
sorted_by_distr_sales = group_by_distr_prod.sort_values(
    ['DeliveryDistrictName', 'total_sales'], 
    ascending=[True, False]  # район по алфавиту, продажи по убыванию
)
top3_prod_by_distr = sorted_by_distr_sales.groupby('DeliveryDistrictName').head(3).reset_index(drop=True)
print('\nТоп 3 продукта в каждом районе:')
print(top3_prod_by_distr.head(6), end='\n')

print('\n__Load__')

# сохраняем итоговый файль и агрегаты
df_to_csv(df_merged, 'orders_and_districts', encoding, folder='data')
df_to_csv(sales_by_category, 'sales_by_category', encoding, folder='reports')
df_to_csv(sales_by_distr, 'sales_by_district', encoding, folder='reports')
df_to_csv(top3_prod_by_distr, 'top_product_by_district', encoding, folder='reports')

print("\n__ETL done__")