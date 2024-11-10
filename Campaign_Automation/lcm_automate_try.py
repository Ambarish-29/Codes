import pandas as pd
import json

def file_load(file_name):
    df = pd.read_csv(file_name)
    return df


def pandas_df_load(file_name,type):
    with open(file_name, 'r', encoding='utf-8') as f:
        data_dict = json.load(f)

    data_list = data_dict[type]

    mapping_df = pd.DataFrame(data_list)

    return mapping_df

csv_file_name = 'pf.csv'
json_file_name = 'lcm_mapping.json'
find_type = 'sa'
filter_dict = {"dnc_flag": "no", "control_group": "test"}
map_columns = ['product','language']
lst = ["SMS"]
is_split = False
split_filters = {"language": ("AR","EN")}
rename_columns = {"mobile": "Mobile", "language": "Language", "amount": "Amount"}
select_columns = ["Mobile","Amount","Language","SMS"]
write_file_name = "DTS_SA_SMS"
output_format = 'xlsx'


df = file_load(csv_file_name)
mapping_df = pandas_df_load(json_file_name,find_type)

#print(df)

#print(mapping_df['Takaful'].dropna())

def campaign_language_mapping(mapping_df,lst):
    lgth = len(lst)
    #print(lgth)
    str = ''
    for i in range(lgth):
        #print(i)
        str += f"['{lst[i]}']"
        str += '[0]' if i+1 != lgth else ''
        gh = mapping_df[lst[i]].dropna().reset_index(drop=True) if i == 0 else gh[lst[i]]
        #print(gh)
        gh = gh[0] if i+1 != lgth else gh
        #print(gh)
        #print(f"mapping_df{str}")
    #print(gh)
    #dg = mapping_df['Takaful'][0]['EN'][0]['Push_Title']

    #gh = eval(f"{mapping_df}{str}")
    #print(gh)
    #print(dg)
    return gh
    
#lst = ['Push_Title','Push_Body','Link']
dict1 = {}
#campaign_language_mapping(mapping_df,lst)

def add_extra_columns(row):
    if map_columns:
        new_lst = []
        for map_column in map_columns:
            new_lst.append(row[map_column])
    #lcm_camp = row['lcm_camp'] if find_type == 'lcm' else ''
    #language = row['language']
    #product = row['product'] if find_type == 'sa' else ''
    #print(lst)
    for item in lst:
        #print(item)
        newest_lst = new_lst.copy()
        #print(newest_lst)
        #print(new_lst)
        #new_lst = [lcm_camp,language] if find_type == 'lcm' else [product,language] if find_type == 'sa' else []
        newest_lst.append(item)
        #print(newest_lst)
        #print('hi')
        mapping = campaign_language_mapping(mapping_df,newest_lst)
        #print(mapping)
        if mapping:
            row[item] = mapping
    return row


def filter_df(filter_dict,df):
    if filter_dict:
        for filters,value in filter_dict.items():
            df = df[df[filters] == value]
            #print(filters,value)
    return df


df = filter_df(filter_dict,df)     
df = df.apply(add_extra_columns, axis=1)

def rename_df_columns(df,rename_columns):
    if rename_columns:
        for rename_column,rename_value in rename_columns.items():
            df = df.rename(columns={rename_column:rename_value})
    return df


def write_file(output_format,df,write_file_name,encode):
    if output_format == 'text':
        for write_index,write_row in df.iterrows():
            if write_index == 0:
                with open(write_file_name, 'w', encoding='utf-8' if encode != None else None) as file:
                    header = '\t'.join(write_row.index)
                    file.write(f"{header}\n")
                    data = '\t'.join(map(str, write_row.values))
                    file.write(f"{data}\n")
            else:
                with open(write_file_name, 'a', encoding='utf-8' if encode != None else None) as file:
                    data = '\t'.join(map(str, write_row.values))
                    file.write(f"{data}\n")

    elif output_format == 'xlsx':
        write_file_name = write_file_name + '.xlsx'
        df.to_excel(write_file_name, index=False, engine='openpyxl')


if is_split:
    #print(split_filters)
    for split_filter_column,split_filter_column_values in split_filters.items():
        for split_filter_column_value in split_filter_column_values:
            split_df = df[df[split_filter_column] == split_filter_column_value]
            split_df = split_df.reset_index(drop=True)
            split_df = rename_df_columns(split_df,rename_columns)
            split_df = split_df[select_columns]
            encode = None if split_filter_column_value != 'AR' else "yes"
            from datetime import datetime
            current_date = datetime.now()
            formatted_date = current_date.strftime("%d%m%Y")
            write_file_name_final =  write_file_name + '_' + f'{split_filter_column_value}' + '_' + f'{formatted_date}'
            write_file(output_format,split_df,write_file_name_final,encode)

else:
    df = rename_df_columns(df,rename_columns)
    df = df[select_columns]
    encode = "yes"
    from datetime import datetime
    current_date = datetime.now()
    formatted_date = current_date.strftime("%d%m%Y")
    write_file_name_final =  write_file_name + '_' + f'{formatted_date}'
    write_file(output_format,df,write_file_name_final,encode)



'''
campaign_language_mapping = {
    ('Takaful','EN'): {'Push_Title': 'Find Peace of mind', 'Push_Body': 'Help Safeguard Yourself', 'Link': 'https://dummy'},
    ('Credit Card Xsell','EN'): {'Push_Title': 'Unlock World', 'Push_Body': 'Our range of', 'Link': 'https://dummy2'},
    ('Takaful','AR'): {'Push_Title': 'اعثر على راحة البال', 'Push_Body': 'ساعد في حماية نفسك', 'Link': 'https://dummy'}
}


json_df = pd.read_json('lcm_mapping.json')
print(json_df)
print(json_df['Takaful'])
val = '(\'Takaful\',\'EN\')'
#print(json_df.loc[0, val]['Push_Title'])


df = df.apply(add_extra_columns, axis=1)


arabic_df = df[df['language'] == 'AR']
arabic_df = arabic_df.reset_index(drop=True)
arabic_df = arabic_df.rename(columns={'rim_no':'Customer_no', 'language': 'Language'})
arabic_df = arabic_df[['Customer_no','Language', 'Push_title', 'Push_body', 'Link']]


english_df = df[df['language'] == 'EN']
english_df = english_df.reset_index(drop=True)
english_df = english_df.rename(columns={'rim_no':'Customer_no', 'language': 'Language'})
english_df = english_df[['Customer_no','Language', 'Push_title', 'Push_body', 'Link']]


for index,row in english_df.iterrows():
    file_name = "test_lcm.txt"
    if index == 0:
        with open(file_name, 'w') as file:
            header = '\t'.join(row.index)
            file.write(f"{header}\n")
            data = '\t'.join(map(str, row.values))
            file.write(f"{data}\n")
    else:
        with open(file_name, 'a') as file:
            data = '\t'.join(map(str, row.values))
            file.write(f"{data}\n")

for ar_index,ar_row in arabic_df.iterrows():
    #print(f"test - {ar_index}")
    file_name = "test_lcm_ar.txt"
    if ar_index == 0:
        with open(file_name, 'w', encoding='utf-8') as file:
            header = '\t'.join(ar_row.index)
            file.write(f"{header}\n")
            data = '\t'.join(map(str, ar_row.values))
            file.write(f"{data}\n")
    else:
        with open(file_name, 'a', encoding='utf-8') as file:
            data = '\t'.join(map(str, ar_row.values))
            file.write(f"{data}\n")


'''
