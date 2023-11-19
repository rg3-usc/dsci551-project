import csv
import os
import pandas as pd
import string

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
    # def get_next_index(self):
    #     if not self.names_data:
    #         return 1  # If there is no existing data, start indexing from 1
    #     else:
    #         existing_indexes = {int(entry["Id"]) for entry in self.names_data}
    #         all_indexes = set(range(1, max(existing_indexes) + 1))
    #         available_indexes = all_indexes - existing_indexes

    #         if available_indexes:
    #             return min(available_indexes)
    #         else:
    #             return max(existing_indexes) + 1

    def get_next_id(self):
        if not self.names_data:
            return 1  # If there is no existing data, start Id from 1
        else:
            existing_ids = {int(entry["Id"]) for entry in self.names_data}
            all_ids = set(range(1, max(existing_ids) + 1))
            available_ids = all_ids - existing_ids

        if available_ids:
            return min(available_ids)
        else:
            return max(existing_ids) + 1

# ----------------------------------------------------------- #
# Choice #1: INSERT
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
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
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

        # Assign Id
        new_id = self.get_next_id()  # Generate Id for the new entry
        
        # Define new data
        new_data = {"Id": new_id, "Name": name, "Year": year, "Gender": gender, "Count": count}

        # Write new data
        self.write_data_chunk(new_data)
        self.names_data.append(new_data)

# ----------------------------------------------------------- #
# Choice #2: BATCH LOAD
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
                # print(f"Processing record: {name}, {gender}, {file_path}")

                # Create the 'data_chunks' directory if it doesn't exist
                os.makedirs("data_chunks", exist_ok=True)

                # Write header only if the file is newly created
                write_header = not os.path.exists(file_path)

                with open(file_path, 'a', newline='') as chunk_csvfile:
                    fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
                    chunk_writer = csv.DictWriter(chunk_csvfile, fieldnames=fieldnames)

                    if write_header:
                        chunk_writer.writeheader()

                    # Write data to the file
                    chunk_writer.writerow(row)

                self.names_data.append(row)
        
        # Print the names_data after loading
        # print("")
        # print("names_data after loading:")
        # for record in self.names_data:
        #     print(record)

        # print("")

    def write_data_to_csv(self):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.names_data)

# ----------------------------------------------------------- #
# Choice #3: DELETE
# ----------------------------------------------------------- #
    def write_data(self, data=None):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
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

                # Get records that match the specified criteria
                matching_records = [record for record in reader if
                                    record["Name"] == name  # Use the original case for comparison
                                    and record["Year"] == year
                                    and record["Gender"].lower() == gender.lower()]

            # Check if any records were found
            if matching_records:
                print("")
                print("Matching records:")
                for record in matching_records:
                    print(record)

                # Ask the user which records to delete
                while True:
                    indices_input = input("Enter the Id or Ids to delete (comma-separated): ")
                    ids_to_delete = [id.strip() for id in indices_input.split(",")]

                    # Validate input
                    if all(id.isdigit() and int(id) in {int(record["Id"]) for record in matching_records} for id in ids_to_delete):
                        break
                    else:
                        print("Invalid input. Please enter valid Ids.")

                ids_to_delete = [int(id) for id in ids_to_delete]
                print("")
                print("Ids to delete:")
                print(ids_to_delete)

                # Delete the selected records
                updated_data = [record for record in matching_records if int(record["Id"]) not in ids_to_delete]

                # Rewrite the CSV file with the updated records
                self.write_data(updated_data)

                print("")
                print(f"{len(ids_to_delete)} record(s) for {name} in {year} with gender {gender} deleted successfully!")
                return True
            else:
                print(f"No records found for {name} in {year} with gender {gender}.")
                return False
        else:
            print(f"The file '{filename}' does not exist.")
            return False

# ----------------------------------------------------------- #
# Choice #4: EDIT
# ----------------------------------------------------------- #
    def write_edit_data(self, data=None):
        with open(self.file_path, 'w', newline='') as csvfile:
            fieldnames = ["Id", "Name", "Year", "Gender", "Count"]
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

                # Get records that match the specified criteria
                matching_records = [record for record in reader if
                                    record["Name"] == name  # Use the original case for comparison
                                    and record["Year"] == year
                                    and record["Gender"].lower() == gender.lower()]

            # Check if any records were found
            if matching_records:
                print("")
                print("Matching records:")
                for record in matching_records:
                    print(record)

                # Ask the user which records to edit
                while True:
                    ids_input = input("Enter the Id or Ids to edit (comma-separated): ")
                    ids_to_edit = [id.strip() for id in ids_input.split(",")]

                    # Validate input
                    if all(id.isdigit() and int(id) in {int(record["Id"]) for record in matching_records} for id in ids_to_edit):
                        break
                    else:
                        print("Invalid input. Please enter valid Ids.")

                ids_to_edit = [int(id) for id in ids_to_edit]
                print("")
                print("Ids to edit:")
                print(ids_to_edit)

                # Search for the existing record by Id
                for record in self.names_data:
                    if int(record["Id"]) in ids_to_edit:
                        # Update the existing record with new data
                        record["Name"] = new_name
                        record["Year"] = new_year
                        record["Gender"] = new_gender
                        self.write_edit_data()  # Rewrite the CSV file after updating the record
                        return True  # Record edited successfully
                return False  # Record not found
            else:
                print(f"No records found for {name} in {year} with gender {gender}.")
                return False
        else:
            print(f"The file '{filename}' does not exist.")
            return False


# ----------------------------------------------------------- #
# Choice #5: Display Data
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
# Choice #6: QUERY
# ----------------------------------------------------------- #
    def create_temp_query_table(self):
    #################### QUERY SYNTAX ######################
        # Instructions for Query Syntax
        indent = 8
        text_name = " " * indent + "<name> (a-z, [a,b,d,m], Kevin, or [Kevin,Sarah,Dave])"
        text_gender = " " * indent + "<gender> (M, F, Female, M-F or M/F)"
        text_year = " " * indent + "<year> (2009, 2000-2009, or [2000,1999,2010])"
        text_aggregate = " " * indent + "<aggregate> (count, sum, top, bottom, or None)"
        text_order = " " * indent + "desc, asc or None"
        text_by = " " * indent + "number or None"
        text_return =  " " * indent + "<col, [col1,col2...,col5], or all>"


        print("INSTRUCTIONS: Follow example formats below exactly.")
        print("Syntax: FIND <Name> <Gender> <Year> CONDITION <aggregate> <value> ORDER <desc/asc> BY <col name> RETURN <col names>")
        print(text_name)
        print(text_gender)
        print(text_year)
        print(text_aggregate)
        print(text_order)
        print(text_by)
        print(text_return)

        print("")
        print("Example: FIND Kevn M 2003 CONDITION None None ORDER None BY None RETURN [Id, Name]")
        print("Example: FIND a-z M/F 1999-2010 CONDITION top 100 ORDER desc BY Count RETURN [Id, Name]")
        print("")

        query = input("Enter your query: ")
    
    #################### QUERY PARSER ######################
        try:
            # Parse the query to extract table names, columns, and conditions
            parse = query.split()

            # PARSE[0]
            # action = parse[0]

            # PARSE[1] Find alphabet/names to prep for finding files
            if parse[1] == '[a-z]':
                print("Invalid syntax. Strictly requires, a-z, [a,b,d,m], Kevin, or [Kevin,Sarah,Dave]. NO SPACES")
                
            elif parse[1].startswith('[') and parse[1].endswith(']'):
                content_in_brackets = parse[1][1:-1].replace(',', ', ').split(', ')
                a_list = []
                for i in content_in_brackets:
                    a_list.append(i[0].lower())
                    alphabet = a_list
                
            else:
                if parse[1] == 'a-z':
                    alphabet = string.ascii_lowercase
                
                elif isinstance(parse[1], str) and not parse[1].isspace():
                    alphabet = parse[1][0].lower()

            alphabet = list(alphabet)

            # PARSE[2] GENDER
            if 'M' in parse[2] and 'F' in parse[2]:
                gender = ['male', 'female']
            elif parse[2] == 'F':
                gender = ['female']
            elif parse[2] == 'M':
                gender = ['male']
            else:
                print("Invalid gender inforamtion. Enter case-sensitive value, e.g., M, F, Female, M-F or M/F")
            
            # PARSE[3] YEAR
            if "-" in parse[3]:
                year_range = parse[3]
                start_year, end_year = map(str, year_range.split('-'))

                if start_year.isnumeric() and end_year.isnumeric() and len(start_year) == 4 and len(end_year) == 4:
                    start_year, end_year = int(start_year), int(end_year)

                    if start_year < end_year:
                        years = list(range(start_year, end_year + 1))
                    else:
                        print("Invalid year range. Start year must be less than end year.")
                else:
                    print("Invalid year range. Start and end years must be numeric 4-digit values.")

            elif parse[3].startswith('[') and parse[3].endswith(']'):
                content_in_brackets = parse[3][1:-1].replace(',', ', ').split(', ')
                for i in content_in_brackets:
                    if len(i) != 4:
                        print("Invalid year range. Year must be numeric 4-digit values.")
                years = content_in_brackets
                
            else:
                if parse[3].isnumeric() and len(parse[3]) == 4:
                    years = [int(parse[3])]
                else:
                    print("Invalid year. Must be numeric 4-digits.")

            years = [int(i) for i in years]

    #################### CREATE / JOIN TABLE ######################
            # READ & CREATE TABLES based on projection criteria 
            # JOIN & Filter Data based on criteria
              
            # Create list of filenames according to the query based on name and gender
            csv_filenames = []

            for g in gender:
                for l in alphabet:
                    csv_filenames.append(f"{g}_{l}.csv")

            # Merge all files / create temporary file
            # Initialize an empty DataFrame to store the merged data
            joined_data = pd.DataFrame()

            # Iterate through each CSV file and append its data to the merged_data DataFrame
            for i in csv_filenames:
                file_path = f"data_chunks/{i}"
                data = pd.read_csv(file_path)
                joined_data = pd.concat([joined_data, data])
            
            # filter data for years specified
            joined_data = joined_data[joined_data['Year'].isin(years)]

            # filter for each names only
            try:
                if parse[1].startswith('[') and parse[1].endswith(']'):
                    content_in_brackets = parse[1][1:-1].replace(',', ', ').split(', ')
                    print(content_in_brackets)
                    
                    # Filter the data for all names in the list
                    filtered_for_names = joined_data[joined_data['Name'].str.lower().isin([name.lower() for name in content_in_brackets])]
                    
                else:
                    # Filter the data for a given name
                    filtered_for_names = joined_data[joined_data['Name'].str.lower() == parse[1].lower()]

            except KeyError:
                print(f"Name(s) {parse[1]} not found in the dataframe.")

    #################### APPLY CONDITIONS ######################
            # Apply conditions to the joined table
            # Prep Data
            str_gender = '/'.join(gender)

            if filtered_for_names.empty:
                data_joined = joined_data
            else:
                data_joined = filtered_for_names

            data = data_joined

            # PARSE [5]: AGGREGATE
            if parse[5] == 'count':
                count_rows = len(data)
                count_rows = '{:,.0f}'.format(count_rows)
                print_out = (f"\nFor {str_gender} names {parse[1]} during {parse[3]}, total number of rows/records is {count_rows}.")

            elif parse[5] == 'sum':
                sum = data['Count'].sum()
                sum = '{:,.0f}'.format(sum)
                print_out = (f"\nFor {str_gender} during {parse[3]}, total number named {parse[1]} is {sum}.")

            elif parse[5] != 'sum' or parse[5] != 'count':

                if parse[5] == 'top':
                    if parse[6] == None or not parse[6].isnumeric() :
                        print('QUERY ERROR: Please enter numerical value for top condition.')
                    else:
                        data = data.sort_values(by=['Count'], ascending=False)
                        data = data.head(int(parse[6]))
                        print_out = data

                elif parse[5] == 'bottom':
                    if parse[6] == None or not parse[6].isnumeric() :
                        print('QUERY ERROR: Please enter numerical value for top condition.')
                    else:
                        data = data.sort_values(by=['Count'], ascending=True)
                        data = data.head(int(parse[6]))
                        print_out = data
                else:
                    print("QUERY ERROR: Invalid CONDITION(S)")

                # PARSE[8]: ORDER BY
                if parse[8].startswith('[') and parse[8].endswith(']'):
                    content_in_8 = parse[8][1:-1].replace(',', ', ').split(', ')
                    order_list = [False if i == 'desc' else True for i in content_in_8]
                            
                    content_in_10 = parse[10][1:-1].replace(',', ', ').split(', ')
                    data = data.sort_values(by=content_in_10, ascending=order_list)
                    print_out = data

                elif parse[8] == 'desc':
                    if parse[10] == None:
                        print('QUERY ERROR: Please enter value(s) for BY condition.')
                    else:
                        bool = False
                        data = data.sort_values(by=parse[10], ascending=bool)
                        print_out = data

                elif parse[8] == 'asc':
                    if parse[10] == None:
                        print('QUERY ERROR: Please enter value(s) for BY condition.')
                    else:
                        bool = True
                        data = data.sort_values(by=parse[10], ascending=bool)
                        print_out = data
                
                else:
                    print("QUERY ERROR: Invalid ORDER or BY entry.)")

                # PARSE[12]: RETURN COLUMNS
                if parse[12] == 'all':
                    print_out = data
                elif parse[12].startswith('[') and parse[12].endswith(']'):
                    content_in_12 = parse[12][1:-1].replace(',', ', ').split(', ')
                    data = data[content_in_12]
                    print_out = data
                else:
                    data = data[parse[12]]
                    print_out = data

    #################### RETURN QUERY RESULT ######################
            print_line_1 = (f"\nJoined table for querying:")
            print_line_2 = (f"\nReturn final results:")
            query_result = [print_line_1, data_joined, print_line_2, print_out]


            return query_result

        except Exception as e:
            print(f"Error executing the query: {e}")
            return []


        # try:
        #     # Ask the user if they want to save the temporary table
        #     save_table = input("\nDo you want to save this table temporarily? (yes/no): ").lower()
        #     if save_table == 'yes':
        #         # Save the temporary table in memory for the user to view later
        #         self.temporary_table = result
        #         print("Table saved temporarily.")
        #     else:
        #         print("Table not saved.")

        #     # Return the result
        #     return result

        # except Exception as e:
        #     print(f"Error executing the query: {e}")
        #     return None

# ----------------------------------------------------------- #
# Choice #7: Clear Data
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
# Choice #8: Close
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
        print("6. Query")
        print("7. Clear Data")
        print("8. Exit")
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
            result = database.create_temp_query_table()
            if result is not None:
                for record in result:
                    print(record)
            else:
                print("\nError executing the query.")
            
        elif choice == '7':
            database.clear_data()
            print("\nData has been cleared.")


        elif choice == '8':
            print("\nExiting the program.")
            database.close()
            break

        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
