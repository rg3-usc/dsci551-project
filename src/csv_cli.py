import csv
import os

class BabyNamesDatabase:
    def __init__(self, file_path):
        self.file_path = file_path
        self.names_data = None  # Initialize to None, and load data on demand

    def load_data(self):
        if self.names_data is None and os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                reader = csv.DictReader(file)
                self.names_data = list(reader)
        elif self.names_data is None:
            self.names_data = []  # Initialize as empty list if the file does not exist
        return self.names_data

    def print_data(self):
        print("\nCurrent data in self.names_data:")
        for record in self.names_data:
            print(record)
# ----------------------------------------------------------- #
# Count Records
# ----------------------------------------------------------- #
    def get_total_names_count(self):
        name_counts = {}
        for entry in self.names_data:
            name = entry["Name"].lower()
            count = int(entry["Count"])
            name_counts[name] = name_counts.get(name, 0) + count
        return name_counts

    def update_counts(self, name, new_count):
        self.names_data = [
            {**entry, "Count": new_count} if entry["Name"].lower() == name.lower() else entry
            for entry in self.names_data
        ]

# ----------------------------------------------------------- #
# Indexing
# ----------------------------------------------------------- #
    def get_next_index(self):
        if not self.names_data:
            return 1  # If there is no existing data, start indexing from 1
        else:
            existing_indexes = {int(entry["Index"]) for entry in self.names_data}
            all_indexes = set(range(1, max(existing_indexes) + 1))
            available_indexes = all_indexes - existing_indexes

            if available_indexes:
                return min(available_indexes)
            else:
                return max(existing_indexes) + 1
# ----------------------------------------------------------- #
# For Option #1
# ----------------------------------------------------------- #
    # def write_data_chunk(self, new_data):
    #     name = new_data["Name"]
    #     gender = new_data["Gender"]
    #     starting_letter = name[0].lower()
    #     gender_prefix = "female" if gender.lower() == "f" else "male"

    #     # Generate the filename based on gender and the starting letter
    #     filename = f"{gender_prefix}_{starting_letter}.csv"
    #     file_path = os.path.join("data_chunks", filename)

    #     # Create the 'data_chunks' directory if it doesn't exist
    #     os.makedirs("data_chunks", exist_ok=True)

    #     # Write header only if the file is newly created
    #     write_header = not os.path.exists(file_path)

    #     with open(file_path, 'a', newline='') as csvfile:
    #         fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
    #         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    #         if write_header:
    #             writer.writeheader()

    #         # Write data to the file
    #         writer.writerow(new_data)

    def write_data_chunk(self, new_data):
        name = new_data["Name"]
        gender = new_data["Gender"]
        starting_letter = name[0].lower()
        gender_prefix = "female" if gender.lower() == "f" else "male"

        # Generate the filename based on gender and the starting letter
        filename = f"{gender_prefix}_{starting_letter}.csv"
        file_path = os.path.join("data_chunks", filename)

        # Print debug information
        print(f"Writing to file: {file_path}")
        print(f"File exists: {os.path.exists(file_path)}")

        # Create the 'data_chunks' directory if it doesn't exist
        os.makedirs("data_chunks", exist_ok=True)

        # Write header only if the file is newly created
        write_header = not os.path.exists(file_path)

        with open(file_path, 'a', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if write_header:
                writer.writeheader()

            # Write data to the file
            writer.writerow(new_data)

    def insert(self, name, gender, year):
        
        # Update counts for all existing records with the same name
        name_counts = self.get_total_names_count()
        count = name_counts.get(name.lower(), 0) + 1  # Get the count for the name, default to 0 if not found, and increment it
        self.update_counts(name, count)

        # Assign Index
        index = self.get_next_index()  # Generate index for the new entry
       
        # Define new data
        new_data = {"Index": index, "Name": name, "Gender": gender, "Year": year, "Count": count}

        # Write new data
        self.write_data_chunk(new_data)
        self.names_data.append(new_data)

# ----------------------------------------------------------- #
# For Option #2
# ----------------------------------------------------------- #
    def load_data_from_csv(self, csv_file_path):
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                name = row["Name"]
                gender = row["Gender"]
                starting_letter = name[0].lower()
                gender_prefix = "female" if gender.lower() == "f" else "male"

                # Generate the filename based on gender and the starting letter
                filename = f"{gender_prefix}_{starting_letter}.csv".lower()
                file_path = os.path.join("data_chunks", filename)

                # Print debugging information
                print(f"Processing record: {name}, {gender}, {file_path}")

                # Create the 'data_chunks' directory if it doesn't exist
                os.makedirs("data_chunks", exist_ok=True)

                # Write header only if the file is newly created
                write_header = not os.path.exists(file_path)

                with open(file_path, 'a', newline='') as chunk_csvfile:
                    fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
                    chunk_writer = csv.DictWriter(chunk_csvfile, fieldnames=fieldnames)

                    if write_header:
                        chunk_writer.writeheader()

                    # Write data to the file
                    chunk_writer.writerow(row)

                self.names_data.append(row)
        
        # Print the names_data after loading
        print("")
        print("names_data after loading:")
        for record in self.names_data:
            print(record)

        print("")

    def write_data_to_csv(self):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.names_data)

# ----------------------------------------------------------- #
# For Option #3
# ----------------------------------------------------------- #
    def write_data(self, data=None):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            if data:
                for record in data:
                    writer.writerow(record)
            else:
                for record in self.names_data:
                    writer.writerow(record)

    def delete(self, name, year, gender):
        # Determine the filename based on gender and the starting letter of the name
        starting_letter = name[0].lower()
        gender_prefix = "female" if gender.lower() == "f" else "male"
        filename = f"{gender_prefix}_{starting_letter}.csv"

        # Full path to the file
        file_path = os.path.join("data_chunks", filename)

        # Check if the file exists
        if os.path.exists(file_path):
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)

                # Get indices of records that match the specified criteria
                matching_records = [record for record in reader if
                                    record["Name"] == name  # Use the original case for comparison
                                    and record["Year"] == year
                                    and record["Gender"].lower() == gender.lower()]

            # Check if any records were found
            if matching_records:
                print("")
                print("Matching records:")
                for index, record in enumerate(matching_records, start=1):
                    print(f"{index}. {record}")

                # Ask the user which records to delete
                while True:
                    indices_input = input("Enter the index or indices to delete (comma-separated): ")
                    indices_to_delete = [index.strip() for index in indices_input.split(",")]

                    # Validate input
                    if all(index.isdigit() and 1 <= int(index) <= len(matching_records) for index in indices_to_delete):
                        break
                    else:
                        print("Invalid input. Please enter valid indices.")

                indices_to_delete = [int(index) for index in indices_to_delete]
                print("")
                print("Index or Indices to delete:")
                print(indices_to_delete)

                # Delete the selected records
                updated_data = [record for index, record in enumerate(matching_records, start=1) if index not in indices_to_delete]
                
                # Rewrite the CSV file with the updated records
                self.write_data(updated_data)

                print("")
                print(f"{len(indices_to_delete)} record(s) for {name} in {year} with gender {gender} deleted successfully!")
                return True
            else:
                print(f"No records found for {name} in {year} with gender {gender}.")
                return False
        else:
            print(f"The file '{filename}' does not exist.")
            return False

# ----------------------------------------------------------- #
# For Option #4
# ----------------------------------------------------------- #
    def write_edit_data(self, data=None):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Index", "Name", "Gender", "Year", "Count"]
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)

            if data:
                for record in data:
                    writer.writerow([record[field] for field in fieldnames])
            else:
                for record in self.names_data:
                    writer.writerow([record[field] for field in fieldnames])

    def edit(self, name, year, gender, new_name, new_year, new_gender):
        # Determine the filename based on gender and the starting letter of the name
        starting_letter = name[0].lower()
        gender_prefix = "female" if gender.lower() == "f" else "male"
        filename = f"{gender_prefix}_{starting_letter}.csv"

        # Full path to the file
        file_path = os.path.join("data_chunks", filename)

        # Check if the file exists
        if os.path.exists(file_path):
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)

                # Get indices of records that match the specified criteria
                matching_records = [record for record in reader if
                                    record["Name"] == name  # Use the original case for comparison
                                    and record["Year"] == year
                                    and record["Gender"].lower() == gender.lower()]

            # Check if any records were found
            if matching_records:
                print("")
                print("Matching records:")
                for index, record in enumerate(matching_records, start=1):
                    print(f"{index}. {record}")

                # Ask the user which records to edit
                while True:
                    indices_input = input("Enter the index or indices to edit (comma-separated): ")
                    indices_to_delete = [index.strip() for index in indices_input.split(",")]

                    # Validate input
                    if all(index.isdigit() and 1 <= int(index) <= len(matching_records) for index in indices_to_delete):
                        break
                    else:
                        print("Invalid input. Please enter valid indices.")

                # Search for the existing record
                for record in self.names_data:
                    if record["Name"].lower() == name.lower() and record["Year"] == year and record["Gender"].lower() == gender.lower():
                        # Update the existing record with new data
                        record["Name"] = new_name
                        record["Year"] = new_year
                        record["Gender"] = new_gender
                        self.write_edit_data()  # Rewrite the CSV file after updating the record
                        return True  # Record edited successfully
                return False  # Record not found

# ----------------------------------------------------------- #
# For Option #5
# ----------------------------------------------------------- #
    def read_data(self, filename):
        file_path = os.path.join("data_chunks", filename)

        if os.path.exists(file_path):
            with open(file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                data = []
                for row in reader:
                    data.append(row)
                return data
        else:
            print(f"The file '{filename}' does not exist.")
            return []

    def list_files(self):
        data_chunks_dir = "data_chunks"
        files = [f for f in os.listdir(data_chunks_dir) if os.path.isfile(os.path.join(data_chunks_dir, f)) and f != ".DS_Store"]
        files.sort()  # Sort the filenames alphabetically
        return files
# ----------------------------------------------------------- #
# Clear Data
# ----------------------------------------------------------- #
    def clear_data(self):
        # Clear local data
        self.names_data = []

        # Remove all files in the 'data_chunks' directory
        data_chunks_dir = "data_chunks"
        for filename in os.listdir(data_chunks_dir):
            file_path = os.path.join(data_chunks_dir, filename)
            try:
                os.remove(file_path)
                print(f"File '{filename}' removed.")
            except OSError as e:
                print(f"Error removing file '{filename}': {e}")

# ----------------------------------------------------------- #
# Close
# ----------------------------------------------------------- #
    def close(self):
        pass

# ----------------------------------------------------------- #
# Execute
# ----------------------------------------------------------- #
def main():
    directory = input("Enter the directory where data chunks will be or are stored: ")
    os.makedirs(directory, exist_ok=True)

    # Generate the filename based on gender and the starting letter
    dummy_file_path = os.path.join(directory, "dummy.csv")

    # Create the database instance with the correct file path
    database = BabyNamesDatabase(dummy_file_path)
    database.load_data()

    while True:
        print("\nAvailable options:")
        print("1. Insert new record")
        print("2. Batch load data from existing CSV")
        print("3. Delete existing entry")
        print("4. Edit existing entry")
        print("5. Display data")
        print("6. Clear Data")
        print("7. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            name = input("Enter the name to add: ")
            year = input("Enter 4-digit year of birth: ")
            gender = input("Enter gender (M or F): ")
            database.insert(name, gender, year) # inserts new data
            print(f"New entry for {name} added successfully!")

        # elif choice == '2':
        #     csv_file_path = input("Enter CSV filepath: ")
        #     database.load_data_from_csv(csv_file_path)
        #     database.write_data_to_csv()  # Write loaded data back to the CSV file
        #     print("Data loaded successfully from the CSV file.")

        elif choice == '2':
            csv_file_path = input("Enter CSV filepath: ")
            database.load_data_from_csv(csv_file_path)
            print("Data loaded successfully from the CSV file.")
        
        elif choice == '3':
            ## ISSUE ## NEED to direct deletion to the right place ## ISSUE ## 
            name = input("Enter the name to delete: ")
            year = input("Enter 4-digit year of birth: ")
            gender = input("Enter gender (M or F): ")
            # data = database.read_data(filename)
            if database.delete(name, year, gender):
                print(f"Record for {name} deleted successfully!")

        elif choice == '4':
            ## ISSUE ## NEED to direct edit to the right place ## ISSUE ## 
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
            available_files = database.list_files()
            print("\nAvailable CSV files:")
            for file in available_files:
                print(file)
            print("")
            filename = input("Enter the filename to display data (e.g., male_a.csv): ")
            data = database.read_data(filename)
            print("\nFile contents:")
            for item in data:
                print(item)

        elif choice == '6':
            database.clear_data()
            print("\nData has been cleared.")
        
        elif choice == '7':
            print("\nExiting the program.")
            database.close()
            break

        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
