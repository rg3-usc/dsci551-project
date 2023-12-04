import os
import csv


class BabyNamesDatabase:
    def __init__(self, file_path):
        self.file_path = file_path
        self.names_data = []  #Initialize a list

# ----------------------------------------------------------- #
# Choice #1: INSERT DATA
# ----------------------------------------------------------- #





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
                    gender = record['Gender'].lower()
                    name = record['Name'].lower()
                    first_letter = name[0].lower()
                    gender_prefix = "female" if gender.lower() == "f" else "male"

                    # Generate the filename based on gender and the starting letter
                    filename = f"{gender_prefix}_{first_letter}.csv"
                    file_path = os.path.join(directory, filename)

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





# ----------------------------------------------------------- #
# Choice #4: UPDATE DATA
# ----------------------------------------------------------- #





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
        # print("1. Insert new record")
        print("2. Batch load data from existing CSV")
        # print("3. Delete existing entry")
        # print("4. Update existing entry")
        print("5. Display data")
        # print("6. Query")
        print("7. Clear Data")
        print("8. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            name = input("Enter the name to add: ")
            year = input("Enter 4-digit year of birth: ")
            gender = input("Enter gender (M or F): ")
            database.insert(name, gender, year) # inserts new data
            print(f"\nNew entry for {name} added successfully!")

        elif choice == '2':
            csv_file_path = input("Enter CSV filepath: ")
            database.load_batch_data(directory, csv_file_path) #loads real data
            print("\nData loaded successfully from the CSV file.")

        # ADD CHOICE #3 


        
        # ADD CHOICE #4 


        elif choice == '5':
            available_files = database.list_files(directory)
            print("\nAvailable CSV files:")
            for file in available_files:
                print(file)
            print("")
            filename = input("Enter the filename to display data (e.g., male_a.csv): ")
            data = database.read_data(filename)
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
