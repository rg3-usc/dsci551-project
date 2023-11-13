import json
import os
import re

class KeyValueStore:
    PRIMARY_KEY_LOCATION = "_primary_key"

    def __init__(self, file_path):
        self.file_path = file_path
        self.primary_keys = set()  # Set to store primary keys
        if not os.path.exists(file_path):
            with open(file_path, 'w'):  # Create the file if it doesn't exist
                pass
        # Load the primary key from the database file
        self.load_primary_key()

    def load_primary_key(self):
        with open(self.file_path, 'r') as file:
            data = file.readline()
            if data:
                first_line = json.loads(data)
                stored_primary_key = first_line.get(self.PRIMARY_KEY_LOCATION)
                self.primary_key = stored_primary_key if stored_primary_key else "business_id"
            else:
                self.primary_key = None

        # Populate primary_keys set with existing keys
        self.populate_primary_keys()

    def populate_primary_keys(self):
        with open(self.file_path, 'r') as file:
            for line in file:
                if line.strip():
                    record = json.loads(line)
                    self.primary_keys.add(record.get(self.primary_key))

    def save_primary_key(self):
        with open(self.file_path, 'w') as file:
            primary_key_data = {self.PRIMARY_KEY_LOCATION: self.primary_key}
            file.write(json.dumps(primary_key_data) + '\n')

    def insert(self, data, force=False):
        key = data.get(self.primary_key)
        if not force and key in self.primary_keys:
            # If the key exists and force is not set, prompt for confirmation to delete the old record
            confirmation = input(f"A record with the key '{key}' already exists. Do you want to replace it? (y/n): ").lower()
            if confirmation != 'y':
                print("\nInsert operation cancelled.")
                return False  # Insertion was not successful
            # Delete the old record
            self.delete(key)
        self.write_data(data)
        self.primary_keys.add(key)
        return True  # Insertion was successful

    def write_data(self, data):
        with open(self.file_path, 'a') as file:
            data_str = json.dumps(data) + '\n'
            file.write(data_str)

    def delete(self, key):
        success_flag = False
        # Check if the key is in self.primary_keys before attempting to remove it
        key in self.primary_keys
        if key in self.primary_keys:
            self.primary_keys.remove(key)
            success_flag = True
        # Write data excluding the record with the specified key
        data = [record for record in self.read_data() if record.get(self.primary_key) != key]
        with open(self.file_path, 'w') as file:
            for record in data:
                file.write(json.dumps(record) + '\n')
        # Return True if a record was deleted, False otherwise
        return success_flag

    def read_data(self, read_primary_keys=False):
        data = []
        if read_primary_keys:
            return list(self.primary_keys)
        with open(self.file_path, 'r') as file:
            for line in file:
                if line.strip():
                    data.append(json.loads(line))
        return data

    def update(self, key, new_values):
        # Check if the key exists
        if key not in self.primary_keys:
            print(f"No record found with key '{key}'. Update operation cancelled.")
            return False
        # Find the record with the specified key
        data = self.read_data()
        updated_data = []
        record_updated = False
        for record in data:
            if record.get(self.primary_key) == key:
                # Update the record
                record.update(new_values)
                updated_data.append(record)
                record_updated = True
            else:
                updated_data.append(record)
        if record_updated:
            # Write the updated data back to the file
            with open(self.file_path, 'w') as file:
                for record in updated_data:
                    file.write(json.dumps(record) + '\n')
            print(f"'{key}' updated successfully.")
            return True
        else:
            print(f"No record found with key '{key}'. Update operation cancelled.")
            return False

    def batch_insert_from_file(self, json_file_path):
        try:
            existing_keys_detected = set()  # Set to track existing keys detected
            success_flag = False  # Flag to track if any insertion was successful
            confirmation_asked = False
            confirmation = 'y'
            with open(json_file_path, 'r') as file:
                for line in file:
                    try:
                        data = json.loads(line)
                        key = data.get(self.primary_key)
                        if key in self.primary_keys and key not in existing_keys_detected:
                            # If the key already exists and this is the first encounter, prompt the user
                            if not confirmation_asked:
                                confirmation = input("\nAt least one record whose primary key already exists in the database. "
                                                "Batch insert will replace the values of keys that already exist. "
                                                "Would you still like to proceed? (y/n): ").lower()
                            confirmation_asked = True
                            if confirmation != 'y':
                                print(f"Skipping key '{key}' since it already exists.")
                                existing_keys_detected.add(key)
                            else:
                                print(f"Replacing values for key '{key}'.")
                        # Insert the key (whether it's a duplicate or a new key)
                        if key not in self.primary_keys or confirmation == 'y':
                            if self.insert(data, force=True):  # Use force=True to avoid confirmation prompt during batch insert
                                success_flag = True
                    except json.JSONDecodeError:
                        print("Invalid JSON format in file. Skipping line.")
            return success_flag
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return False

    def close(self):
        pass

def load_json_data(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def main():
    file_path = input("Enter the path to the database file: ")
    database = KeyValueStore(file_path)
        
    def project(data, fields):
        return [{field: item.get(field) for field in fields} for item in data]

    def filter_data(data, query):
        match = re.match(r'(\w+)\s*([=><!]+)\s*(\'[^\']*\'|"[^"]*"|\w+)', query)
        if not match:
            match = re.match(r'(\w+)\s+contains\s+(\'[^\']*\'|"[^"]*"|\w+)', query)
            if not match:
                raise ValueError(f'Invalid query: {query}')
            field, value = match.groups()
            # Convert both the field value and the search value to lowercase for case-insensitive matching
            condition = f"{value.lower()} in item.get('{field}').lower()"
        else:
            field, op, value = match.groups()
            if op == '=': op = '=='
            condition = f"item.get('{field}') {op} {value}"
        filtered_data = [item for item in data if eval(condition, {"item": item})]
        return filtered_data

    '''def custom_merge(data, key, other_data):
        merged_data = []
        key_index = {item[key]: item for item in other_data}
        for item in data:
            key_value = item.get(key)
            if key_value in key_index:
                item.update(key_index[key_value])
                merged_data.append(item)
        return merged_data'''
    
    def count(data, group_by=None):
        results = []
        if group_by is not None:
            def group(data, field):
                grouped_data = {}
                for item in data:
                    key = item[field]
                    grouped_data.setdefault(key, []).append(item)
                return grouped_data
            grouped_data = group(data, group_by)
            for key, group in grouped_data.items():
                count_result = len(group)
                results.append({group_by: key, "count": count_result})
        else:
            count_result = len(data)
            results.append({"count": count_result})
        return results
    def find(data, aggregation, field, group_by=None):
        results = []
        if group_by is not None:
            def group(data, field):
                grouped_data = {}
                for item in data:
                    key = item[field]
                    grouped_data.setdefault(key, []).append(item)
                return grouped_data
            grouped_data = group(data, group_by)
            for key, group in grouped_data.items():
                if aggregation == "sum":
                    agg_result = sum(item.get(field, 0) for item in group)
                elif aggregation == "min":
                    agg_result = min((item.get(field, None) for item in group if field in item), default=None)
                elif aggregation == "max":
                    agg_result = max((item.get(field, None) for item in group if field in item), default=None)
                elif aggregation == "average":
                    valid_items = [item.get(field) for item in group if field in item]
                    agg_result = sum(valid_items) / len(valid_items) if valid_items else None
                elif aggregation == "count":
                    agg_result = sum(1 for item in group)
                results.append({group_by: key, field: agg_result, "aggregation": aggregation})
        else:
            if aggregation == "sum":
                result = sum(item.get(field, 0) for item in data)
            elif aggregation == "min":
                result = min((item.get(field, None) for item in data if field in item), default=None)
            elif aggregation == "max":
                result = max((item.get(field, None) for item in data if field in item), default=None)
            elif aggregation == "average":
                valid_items = [item.get(field) for item in data if field in item]
                result = sum(valid_items) / len(valid_items) if valid_items else None
            elif aggregation == "count":
                result = sum(1 for item in data)
            results.append({field: result, "aggregation": aggregation})
        return results

    def sort(data, fields):
        sorted_data = data
        for field in reversed(fields):
            if field.startswith('-'):
                field = field[1:]  # Remove the '-' for sorting
                sorted_data = sorted(sorted_data, key=lambda item: item.get(field), reverse=True)
            else:
                sorted_data = sorted(sorted_data, key=lambda item: item.get(field))
        return sorted_data

    # Check if the primary key is already set
    if not database.primary_key:
        primary_key = input("Enter the primary key for the dataset: ")
        database.primary_key = primary_key
        database.save_primary_key()
    else:
        print(f"Using primary key: {database.primary_key}")

    while True:
        print("\nAvailable commands:")
        print("1. Insert data (1 record in JSON format)")
        print("2. Insert data (Batch insert of JSON file)")
        print("3. Update data (JSON format)")
        print("4. Delete data")
        print("5. Display data")
        print("6. Query data")
        print("7. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            json_data = input("Enter the JSON data to insert: ")
            try:
                data_dict = json.loads(json_data)
            except json.JSONDecodeError:
                print("\nInvalid JSON format. Please try again.")
                continue
            if database.insert(data_dict):
                print("\nData inserted successfully.")

        elif choice == '2':
            json_file_path = input("Enter the path to the JSON file: ")
            if database.batch_insert_from_file(json_file_path):
                print("\nData inserted successfully.")

        elif choice == '3':
            key = input("Enter the key to update: ")
            json_data = input("Enter the new JSON value: ")
            try:
                new_value_dict = json.loads(json_data)
            except json.JSONDecodeError:
                print("\nInvalid JSON format. Please try again.")
                continue
            database.update(key, new_value_dict)

        elif choice == '4':
            key = input("Enter the key to delete: ")
            if database.delete(key):
                print("\nData deleted successfully.")
            else:
                print("\nNo data found for the given key.")

        elif choice == '5':
            data = database.read_data()
            print("\nDatabase contents:")
            for item in data:
                print(item)

        elif choice == '6':
            query = input("Enter your custom query (e.g., 'project name stars | filter stars > 4'): ")
            queries = query.strip().split('|')
            data = database.read_data()
            for query in queries:
                parts = query.strip().split()
                operation = parts[0]
                if operation == 'project':
                    fields = parts[1:]
                    data = project(data, fields)
                elif operation == 'filter':
                    condition = ' '.join(parts[1:])
                    data = filter_data(data, condition)
                elif operation == 'join':
                    pass
                elif operation == 'find':
                    aggregation = parts[1]
                    if aggregation == "count":
                        group_by = parts[3] if len(parts) > 2 and parts[2] == 'by' else None
                        data = count(data, group_by)
                    else:
                        field = parts[2]
                        group_by = parts[4] if len(parts) > 4 and parts[3] == 'by' else None
                        data = find(data, aggregation, field, group_by)
                elif operation == 'order':
                    fields = parts[1:]
                    data = sort(data, fields)
            print("\nQuery result:")
            for item in data:
                print(item)

        elif choice == '7':
            print("\nExiting the program.")
            database.close()
            break
        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
