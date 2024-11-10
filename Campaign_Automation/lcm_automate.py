import pandas as pd
import json
import jsonschema

#Campaign Class
class CampaignFileProcessor:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __getattr__(self, name):
        return self.__dict__.get(name, None)

    def __setattr__(self, name, value):
        self.__dict__[name] = value

    #Input config schema for input validation
    def validate_input_json(self, input_json):
        config_schema = {
            "type": "object",
            "properties": {
                "csv_file_name": {"type": "string", "pattern": ".+\\.csv$"},
                "json_file_name": {"type": "string", "pattern": ".+\\.json$"},
                "campaign_type": {"type": "string", "enum": ["lcm", "pf"]},
                "filter_dict": {"type": "object"},
                "map_columns": {"type": "array", "items": {"type": "string"}},
                "required_columns": {"type": "array"},
                "is_split": {"type": "boolean"},
                "split_filters": {"type": "object"},
                "rename_columns": {"type": "object"},
                "select_columns": {"type": "array", "minItems": 1},
                "write_file_name": {"type": "string"},
                "output_format": {"type": "string", "enum": ["xlsx", "text"]}
            },
            "required": ["csv_file_name", "json_file_name", "campaign_type", "map_columns",
                         "required_columns", "is_split", "select_columns", "write_file_name", "output_format"]
        }

        #Validate input JSON against the schema
        try:
            jsonschema.validate(input_json, config_schema)
        except jsonschema.ValidationError as e:
            print("Validation error:", e)
            exit(1)

        # Additional checks for specific keys
        if input_json["is_split"] and not input_json.get("split_filters"):
            print("Validation error: 'split_filters' required when 'is_split' is true.")
            exit(1)

    def file_load(self):
        df = pd.read_csv(self.csv_file_name)
        return df

    #mapping json df load
    def pandas_df_load(self):
        with open(self.json_file_name, 'r', encoding='utf-8') as f:
            data_dict = json.load(f)

        data_list = data_dict[self.campaign_type]
        mapping_df = pd.DataFrame(data_list)
        return mapping_df

    #mapping function
    def campaign_language_mapping(self, mapping_df, required_columns):
        length = len(required_columns)
        str = ''
        for i in range(length):
            str += f"['{required_columns[i]}']"
            str += '[0]' if i+1 != length else ''
            map = mapping_df[required_columns[i]].dropna().reset_index(drop=True) if i == 0 else map[required_columns[i]]
            map = map[0] if i+1 != length else map
        return map

    def add_extra_columns(self, row, mapping_df):
        if self.map_columns:
            new_lst = []
            for map_column in self.map_columns:
                new_lst.append(row[map_column])
        for item in self.required_columns:
            newest_lst = new_lst.copy()
            newest_lst.append(item)
            mapping = self.campaign_language_mapping(mapping_df, newest_lst)
            if mapping:
                row[item] = mapping
        return row

    def filter_df(self, df):
        if self.filter_dict:
            for filters, value in self.filter_dict.items():
                df = df[df[filters] == value]
        return df

    def rename_df_columns(self, df):
        if self.rename_columns:
            for rename_column, rename_value in self.rename_columns.items():
                df = df.rename(columns={rename_column: rename_value})
        return df

    def write_file(self, df, write_file_name, encode):
        if self.output_format == 'text':
            for write_index, write_row in df.iterrows():
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
        elif self.output_format == 'xlsx':
            write_file_name = write_file_name + '.xlsx'
            df.to_excel(write_file_name, index=False, engine='openpyxl')

    def process_file(self):
        df = self.file_load()
        mapping_df = self.pandas_df_load()
        df = self.filter_df(df)
        df = df.apply(self.add_extra_columns, args=(mapping_df,), axis=1)
        if self.is_split:
            for split_filter_column, split_filter_column_values in self.split_filters.items():
                for split_filter_column_value in split_filter_column_values:
                    split_df = df[df[split_filter_column] == split_filter_column_value]
                    split_df = split_df.reset_index(drop=True)
                    split_df = self.rename_df_columns(split_df)
                    split_df = split_df[self.select_columns]
                    encode = None if split_filter_column_value != 'AR' else "yes"
                    from datetime import datetime
                    current_date = datetime.now()
                    formatted_date = current_date.strftime("%d%m%Y")
                    write_file_name_final =  self.write_file_name + '_' + f'{split_filter_column_value}' + '_' + f'{formatted_date}'
                    self.write_file(split_df, write_file_name_final, encode)
        else:
            df = self.rename_df_columns(df)
            df = df[self.select_columns]
            encode = "yes"
            from datetime import datetime
            current_date = datetime.now()
            formatted_date = current_date.strftime("%d%m%Y")
            write_file_name_final =  self.write_file_name + '_' + f'{formatted_date}'
            self.write_file(df, write_file_name_final, encode)

# Load parameters from JSON file
with open('input.json', 'r') as file:
    parameters = json.load(file)

# Create an instance of CampaignFileProcessor
processor = CampaignFileProcessor(**parameters)

# Validate input JSON
processor.validate_input_json(parameters)

# Process the file
processor.process_file()