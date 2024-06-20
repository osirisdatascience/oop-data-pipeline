import json
import csv

class Data:

    def __init__(self, data):
        self.data = data
        self.column_names = self.__get_columns()
        self.row_count = self.__size_data()

    def __read_json(path):
        json_data = []
        with open(path, 'r') as file:
            json_data = json.load(file)
        return json_data

    def __read_csv(path):

        csv_data = []
        with open(path, 'r') as file:
            reader = csv.DictReader(file, delimiter=',')
            for row in reader:
                csv_data.append(row)

        return csv_data

    @classmethod
    def read_data(cls, path, data_type):
        data = []

        if data_type == 'csv':
            data = cls.__read_csv(path)
        
        elif data_type == 'json':
            data = cls.__read_json(path)

        return cls(data)

    def __get_columns(self):
        return list(self.data[-1].keys())

    def rename_columns(self, key_mapping):
        new_data = []

        for old_dict in self.data:
            temp_dict = {}
            for old_key, value in old_dict.items():
                temp_dict[key_mapping[old_key]] = value
            new_data.append(temp_dict)
        
        self.data = new_data
        self.column_names = self.__get_columns()

    def __size_data(self):
        return len(self.data)

    def join(dataA, dataB):
        combined_list = []
        combined_list.extend(dataA.data)
        combined_list.extend(dataB.data)
        
        return Data(combined_list)

    def __transform_data_to_table(self):
        
        combined_data_table = [self.column_names]

        for row in self.data:
            line = []
            for column in self.column_names:
                line.append(row.get(column, 'Unavailable'))
            combined_data_table.append(line)
        
        return combined_data_table

    def save_data(self, path):

        combined_data_table = self.__transform_data_to_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(combined_data_table)
