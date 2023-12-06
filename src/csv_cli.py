import os
import csv


class BabyNamesDatabase:
    def __init__(self, file_path):
        self.file_path = file_path
        self.names_data = []  #Initialize a list

    #used by choices #1, 2
    def filename_path(self, directory, name, gender):
        name_lower = name.lower()
        gender_lower = gender.lower()
        first_letter = name_lower[0]
        gender_prefix = "female" if gender_lower == "f" else "male"
        filename = f"{gender_prefix}_{first_letter}.csv"
        file_path = os.path.join(directory, filename)
        return filename, file_path
    
    #used by choices #5
    def read_data(self, directory, selected_file):
        file_path = os.path.join(directory, selected_file)

        if os.path.exists(file_path):
            with open(file_path, 'r', newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                data = [row for row in reader]

            return data
        else:
            print(f"File not found: {file_path}")
            return []
    
# ----------------------------------------------------------- #
# Choice #1: INSERT DATA
# ----------------------------------------------------------- #
    def insert(self, directory, name, gender, year):
        _, file_path = self.filename_path(directory, name, gender) 

        # Create the directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)

        # Check if the CSV file already exists
        if os.path.exists(file_path):
            self._update_existing_file(file_path, name, gender, year)
        else:
            self._create_new_file(file_path, name, gender, year)

    def _create_new_file(self, file_path, name, gender, year):
        # Create a new CSV file with the given record
        with open(file_path, 'w', newline='') as csv_file:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({"Id": 0, "Name": name, "Year": year, "Gender": gender, "Count": 1})
            print(f"New file created: {file_path}")

    def _update_existing_file(self, file_path, name, gender, year):
        # Read existing data using the read_data method
        existing_data = self.read_data(os.path.dirname(file_path), os.path.basename(file_path))

        # Check if a record with the same name and year already exists
        matching_records = [record for record in existing_data if record["Name"] == name and record["Year"] == year]

        if matching_records:
            # Increment count of existing data
            for record in existing_data:
                if record["Name"] == name and record["Year"] == year:
                    record["Count"] = str(int(record["Count"]) + 1)
        else:
            # Append the new record to existing data
            existing_ids = [int(row['Id']) for row in existing_data]
            new_id = max(existing_ids) + 1
            existing_data.append({"Id": new_id, "Name": name, "Year": year, "Gender": gender, "Count": 1})

        # Write the updated data back to the file
        with open(file_path, 'w', newline='') as csv_file:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_data)

# ----------------------------------------------------------- #
# Choice #2: BATCH UPLOAD
# ----------------------------------------------------------- #
# Batch uploads reads a CSV data file in chunks so as not to overload main memory
    def batch_iterator(self, iterable, batch_size):
        batch = []
        for item in iterable:
            batch.append(item)
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
    
    def load_batch_data(self, directory, csv_file_path, batch_size=200000):
        #open file indicated by choice #2
        with open(csv_file_path, 'r') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for batch in self.batch_iterator(csv_reader, batch_size):
                #organize data into chunks
                for record in batch:
                    gender = record['Gender']
                    name = record['Name']

                    _, file_path = self.filename_path(directory, name, gender)   

                    # Create the directory if it doesn't exist
                    os.makedirs(directory, exist_ok=True)

                    # Write header only if the file is newly created
                    write_header = not os.path.exists(file_path)

                    with open(file_path, 'a', newline='') as chunk_csvfile:
                        fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
                        chunk_writer = csv.DictWriter(chunk_csvfile, fieldnames=fieldnames)
                        
                        if write_header:
                            chunk_writer.writeheader()

                        # Write data to the file
                        chunk_writer.writerow(record)

                print("Batch run successfully. On to next")

                self.names_data.extend(batch) #process batch and add to names_data

# ----------------------------------------------------------- #
# Choice #3: DELETE DATA
# ----------------------------------------------------------- #
    def delete(self, directory, name, year, gender):
        _, file_path = self.filename_path(directory, name, gender)

        # Read existing data using the read_data method
        existing_data = self.read_data(os.path.dirname(file_path), os.path.basename(file_path))

        # Find matching records
        matching_records = [record for record in existing_data if record["Name"] == name and record["Year"] == year]

        if not matching_records:
            print(f"No records found for {name} in {year} with gender {gender}.")
            return False

        # Display matching records and their counts
        print(f"\nMatching records for {name} in {year} with gender {gender}:")
        for i, record in enumerate(matching_records):
            print(f"{i + 1}. {record}")

        # Prompt user for the number of records to delete
        total_count = sum(int(record["Count"]) for record in matching_records)
        delete_count = input(f"\nEnter the number of records to delete (1-{total_count} or 'all'): ")

        if delete_count.lower() == 'all':
            # Delete all matching records
            existing_data = [record for record in existing_data if record not in matching_records]
        else:
            # Delete specified number of records
            try:
                delete_count = int(delete_count)
                if 1 <= delete_count <= total_count:
                    for record in matching_records:
                        if delete_count > 0:
                            current_count = int(record["Count"])
                            if delete_count >= current_count:
                                # Delete the entire record
                                existing_data.remove(record)
                                delete_count -= current_count
                            else:
                                # Update the count of the remaining record
                                record["Count"] = str(current_count - delete_count)
                                delete_count = 0
                else:
                    print("Invalid input. Please enter a valid number.")
                    return False
            except ValueError:
                print("Invalid input. Please enter a valid number.")
                return False

        # Write the updated data back to the file
        with open(file_path, 'w', newline='') as csv_file:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_data)

        print(f"\n{delete_count} records deleted successfully for {name} in {year} with gender {gender}.")
        return True

# ----------------------------------------------------------- #
# Choice #4: UPDATE DATA
# ----------------------------------------------------------- #
    def update(self, directory, name, year, gender):
        _, file_path = self.filename_path(directory, name, gender)

        # Read existing data using the read_data method
        existing_data = self.read_data(os.path.dirname(file_path), os.path.basename(file_path))

        # Find matching records
        matching_records = [record for record in existing_data if record["Name"] == name and record["Year"] == year]

        if not matching_records:
            print(f"No records found for {name} in {year} with gender {gender}.")
            return False

        # Display matching records and their counts
        print(f"\nMatching records for {name} in {year} with gender {gender}:")
        for i, record in enumerate(matching_records):
            print(f"{i + 1}. {record}")

        while True:
            # Prompt user for the new count
            new_count = input(f"\nEnter the new count for {name} in {year} with gender {gender}: ")

            if int(new_count) >= 0:
                break
            else:
                print("New count must be greater than or equal to 0.")

        # Update the count of the matching records
        for record in matching_records:
            record["Count"] = str(new_count)

        # Write the updated data back to the file
        with open(file_path, 'w', newline='') as csv_file:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_data)

        print(f"\nCount updated successfully for {name} in {year} with gender {gender}.")
        return True

# ----------------------------------------------------------- #
# Choice #5: DISPLAY DATA
# ----------------------------------------------------------- #
    def list_files(self, directory):
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f != ".DS_Store"]
        files.sort()  # Sort the filenames alphabetically
        return files
# ----------------------------------------------------------- #
# Choice #6: QUERY DATA
# ----------------------------------------------------------- #




# ----------------------------------------------------------- #
# Choice #7: CLEAR DATA
# ----------------------------------------------------------- #
    def clear_data(self, directory):
        # Clear local data
        self.names_data = []

        # Remove all files in the 'data_chunks' directory
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
                print(f"File '{filename}' removed.")
            except OSError as e:
                print(f"Error removing file '{filename}': {e}")
                
# ----------------------------------------------------------- #
# Choice #8: CLOSE / EXIT
# ----------------------------------------------------------- #
    def close(self):
        pass

# ----------------------------------------------------------- #
# EXECUTE MAIN
# ----------------------------------------------------------- #
def main():
    # set up directory for loading data and creating the instance
    directory = input("Enter the directory where data chunks will be stored: ")
    os.makedirs(directory, exist_ok=True) 

    dummy_file_path = os.path.join(directory, "dummy.csv") #create directory with dummy file as placholder until there is real data
    database = BabyNamesDatabase(dummy_file_path) #Create instance using dummy file path

    # Prompt User interaction
    while True:
        print("\nAvailable options:")
        print("1. Insert new record")
        print("2. Batch load data from existing CSV")
        print("3. Delete existing entry")
        print("4. Update existing entry")
        print("5. Display data")
        # print("6. Query")
        print("7. Clear Data")
        print("8. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            name = input("Enter the name to add: ")
            year = input("Enter 4-digit year of birth: ")
            gender = input("Enter gender (M or F): ")
            database.insert(directory, name, gender, year) # inserts new data
            print(f"\nNew entry for {name} added successfully!")

        elif choice == '2':
            csv_file_path = input("Enter CSV filepath: ")
            database.load_batch_data(directory, csv_file_path) #loads real data
            print("\nData loaded successfully from the CSV file.")

        elif choice == '3':
            name = input("Enter the name to delete: ")
            year = input("Enter 4-digit year of birth: ")
            gender = input("Enter gender (M or F): ")
            if database.delete(directory, name, year, gender):
                print(f"\nThe record for {name} in {year} with gender {gender} deleted successfully!")
            else:
                print(f"\nNo records found for {name} in {year} with gender {gender}.")
        
        elif choice == '4':
            print(f"\nNOTE: Only total count may be updated using this option. Use Insert or Delete for all other edits to the data.")
            name = input("Enter the name to edit: ")
            year = input("Enter year of birth: ")
            gender = input("Enter gender (M or F): ")
            if database.update(directory, name, year, gender):
                print(f"\nThe count for the record was updated successfully")
            else:
                print(f"\nNo record found for {name} in {year} with gender {gender}.")

        elif choice == '5':
            available_files = database.list_files(directory)
            print("\nAvailable CSV files:")
            for file in available_files:
                print(file)
            print("")
            selected_file = input("Enter the filename to display data (e.g., male_a.csv): ")
            data = database.read_data(directory, selected_file)
            print("\nFile contents:")
            for item in data:
                print(item)

        # ADD CHOICE #6

        elif choice == '7':
            database.clear_data(directory)
            print("\nData has been cleared.")
        
        elif choice == '8':
            print("\nExiting the program.")
            database.close()
            break
        
        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
