import csv
import os

class BabyNamesDatabase:
    PRIMARY_KEY_LOCATION = "Name"

    def __init__(self, file_path):
        self.file_path = file_path
        self.names_data = []  # List to store baby names data
        
        # Create the file if it doesn't exist and add an index column
        if not os.path.exists(file_path):
            with open(file_path, 'w', newline='') as csvfile:
                fieldnames = ["Index", "Name", "Gender", "Year", "Count"]  # CSV column headers
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()  # Write header to the CSV file

        # Load baby names data from the CSV file
        self.load_data()

    def load_data(self):
        with open(self.file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.names_data.append(row)  # Store each row as a dictionary

    def load_data_from_csv(self, csv_file_path):
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            self.names_data = list(reader)

    def write_data_to_csv(self):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.names_data)
    
    def write_new_data(self, data):
        with open(self.file_path, 'a', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()  # Write header only if the file is empty
            writer.writerow(data)  # Append data to the CSV file

    def write_data(self, data=None):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            if data:
                writer.writerow(data)
            else:
                for record in self.names_data:
                    writer.writerow(record)

    def get_total_names_count(self):
        name_counts = {}  # Dictionary to store name counts
        for record in self.names_data:
            name = record["Name"].lower()
            if name in name_counts:
                name_counts[name] += 1
            else:
                name_counts[name] = 1
        return name_counts
    
    def get_next_index(self):
        # Get the next available unique index
        existing_indices = set(int(record["Index"]) for record in self.names_data)
        next_index = 1
        while next_index in existing_indices:
            next_index += 1
        return next_index
    
    def insert(self, name, gender, year):
        name_counts = self.get_total_names_count()
        count = name_counts.get(name.lower(), 1) + 1  # Get the count for the name, default to 1 if not found, and increment it

        index = self.get_next_index()  # Generate index for the new entry
        new_data = {"Index": index, "Name": name, "Gender": gender, "Year": year, "Count": count}
        self.write_new_data(new_data)
        self.names_data.append(new_data)

    # def insert (self, name, gender, year, count):
    #     index = self.get_next_index()  # Generate index for the new entry
    #     name_counts = self.get_total_names_count()
    #     for i in name_counts:
    #         if name == name_counts[name]:
    #             count = #print value
    #     new_data = {"Index": index, "Name": name, "Gender": gender, "Year": year, "Count": count}
    #     self.write_new_data(new_data)
    #     self.names_data.append(new_data)


    # def insert(self, name, gender, year, count):
    #     # Check if the name already exists in the database for the given year and gender
    #     existing_records = [record for record in self.names_data if record["Name"].lower() == name.lower() and record["Year"] == year and record["Gender"].lower() == gender.lower()]

    #     if existing_records:
    #         # If the name exists for the given year and gender, increment the count
    #         existing_records[0]["Count"] = str(int(existing_records[0]["Count"]) + 1)
    #     else:
    #         # If the name is new for the given year and gender, set the count to the number of existing occurrences + 1
    #         count = str(len([record for record in self.names_data if record["Name"].lower() == name.lower() and record["Year"] == year and record["Gender"].lower() == gender.lower()]) + 1)
    #         index = self.get_next_index()  # Generate index for the new entry
    #         new_data = {"Index": index, "Name": name, "Gender": gender, "Year": year, "Count": count}
    #         self.write_new_data(new_data)
    #         self.names_data.append(new_data)

    def delete(self, name, year, gender):
        # Iterate through the records and remove the matching record
        for record in self.names_data:
            if record["Name"].lower() == name.lower() and record["Year"] == year and record["Gender"].lower() == gender.lower():
                self.names_data.remove(record)
                self.write_data()  # Rewrite the CSV file after removing the record
                return True  # Record deleted successfully
        return False  # Record not found

    def edit(self, name, year, gender, new_name, new_year, new_gender):
        # Search for the existing record
        for record in self.names_data:
            if record["Name"].lower() == name.lower() and record["Year"] == year and record["Gender"].lower() == gender.lower():
                # Update the existing record with new data
                record["Name"] = new_name
                record["Year"] = new_year
                record["Gender"] = new_gender
                self.write_data()  # Rewrite the CSV file after updating the record
                return True  # Record edited successfully
        return False  # Record not found

    def read_data(self):
            with open(self.file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                data = []
                for row in reader:
                    data.append(row)
                return data

    def close(self):
        pass

def main():
    file_path = input("Enter the path to the Baby Names CSV file: ")
    database = BabyNamesDatabase(file_path)

    while True:
        print("\nAvailable options:")
        print("1. Insert new record")
        print("2. Batch load data from existing CSV")
        print("3. Delete existing entry")
        print("4. Edit existing entry")
        print("5. Display data")
        print("6. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            name = input("Enter the name to add: ")
            year = input("Enter year of birth: ")
            gender = input("Enter gender (M or F): ")
            # count = database.get_total_names_count()
            # count = database.calculate_name_count(name)
            database.insert(name, gender, year, count)
            print(f"New entry for {name} added successfully!")

        elif choice == '2':
            csv_file_path = input("Enter CSV filepath: ")
            database.load_data_from_csv(csv_file_path)
            database.write_data_to_csv()  # Write loaded data back to the CSV fil
            print("Data loaded successfully from the CSV file.")

        elif choice == '3':
            name = input("Enter the name to delete: ")
            year = input("Enter year of birth: ")
            gender = input("Enter gender (M or F): ")
            if database.delete(name, year, gender):
                print(f"Record for {name} deleted successfully!")
            else:
                print(f"No record found for {name} in {year} with gender {gender}.")

        elif choice == '4':
            name = input("Enter the name to edit: ")
            year = input("Enter year of birth: ")
            gender = input("Enter gender (M or F): ")
            new_name = input("Enter the new name: ")
            new_year = input("Enter the new year of birth: ")
            new_gender = input("Enter the new gender (M or F): ")
            if database.edit(name, year, gender, new_name, new_year, new_gender):
                print(f"Record for {name} edited successfully!")
            else:
                print(f"No record found for {name} in {year} with gender {gender}.")

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
