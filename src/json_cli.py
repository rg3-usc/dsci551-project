import json
import os

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

def main():
    file_path = input("Enter the path to the database file: ")
    database = KeyValueStore(file_path)

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
        print("6. Exit")
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
            print("\nData updated successfully.")

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
            print("\nExiting the program.")
            database.close()
            break
        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
