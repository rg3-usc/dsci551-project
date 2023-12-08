import os
import csv
import string
import pandas as pd

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

                print("Batch run successfully. On to next...")

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
    def query(self, directory):
        #################### QUERY SYNTAX ######################
        # Instructions for Query Syntax
        indent = 6

        print("\nINSTRUCTIONS: Follow example formats below exactly.")

        print("\nSYNTAX: FIND <name> <gender> <year> CONDITION <aggregate> <value> ORDER <order> BY <col name> RETURN <col names>")
        print(" " * indent + "<name> a-z, a, [a,b,d,m], Kevin, or [Kevin,Sarah,Dave]")
        print(" " * indent + "<gender> M, F, Female, M-F or M/F")
        print(" " * indent + "<year> 2009, 2000-2009, or [2000,1999,2010]")
        print(" " * indent + "<aggregate> count, sum, top, bottom, or None")
        print(" " * indent + "<value> numeric (e.g., 10, 100)")
        print(" " * indent + "<order> desc, asc, [desc,asc] or None")
        print(" " * indent + "<col name> col1, col2, [col1,col2] or None")
        print(" " * indent + "<col name> col1, col2, [col1,col2] or all")

        print("\nEXAMPLES:")
        print(" " * indent + "Find Riley M/F 1995-2000 CONDITION None None ORDER [asc,asc] BY [Gender,Count] RETURN all")
        print(" " * indent + "FIND [Mary,Ivy] F [2000,1983,2010,2020] CONDITION None None ORDER [asc,desc] BY [Name,Year] RETURN [Name,Year,Gender,Count]")
        print(" " * indent + "FIND Kevin M 2010-2014 CONDITION sum")
        print(" " * indent + "FIND e F 2014 CONDITION top 10 ORDER desc BY Count Return all")

        print("\nEXAMPLE ERROR HANDLING:")
        print(" " * indent + "FIND Kevin lksjdfljs 2010-2014 CONDITION sum")
        print(" " * indent + "FIND Kevin M 200009 CONDITION CONDITION sum")
        print("")

        query = input("ENTER YOUR QUERY: ")
        
        #################### QUERY PARSER ######################
        try:
            # Parse the query to extract table names, columns, and conditions
            parse = query.split()

            # PARSE[1] Find alphabet/names to prep for finding files
            if parse[1] == '[a-z]':
                print("\nInvalid syntax. Strictly requires, a-z, [a,b,d,m], Kevin, or [Kevin,Sarah,Dave]. NO SPACES")
                
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
                print("\nInvalid gender inforamtion. Enter case-sensitive value, e.g., M, F, Female, M-F or M/F")

            # PARSE[3] YEAR
            if "-" in parse[3]:
                year_range = parse[3]
                start_year, end_year = map(str, year_range.split('-'))

                if start_year.isnumeric() and end_year.isnumeric() and len(start_year) == 4 and len(end_year) == 4:
                    start_year, end_year = int(start_year), int(end_year)

                    if start_year < end_year:
                        years = list(range(start_year, end_year + 1))
                    else:
                        print("\nInvalid year range. Start year must be less than end year.")
                else:
                    print("\nInvalid year range. Start and end years must be numeric 4-digit values.")

            elif parse[3].startswith('[') and parse[3].endswith(']'):
                content_in_brackets = parse[3][1:-1].replace(',', ', ').split(', ')
                for i in content_in_brackets:
                    if len(i) != 4:
                        print("\nInvalid year range. Year must be numeric 4-digit values.")
                years = content_in_brackets
                
            else:
                if parse[3].isnumeric() and len(parse[3]) == 4:
                    years = [int(parse[3])]
                else:
                    print("\nInvalid year. Must be numeric 4-digits.")

            years = [int(i) for i in years]

            # Create list of filenames according to the query based on name and gender
            csv_filenames = []

            for g in gender:
                for l in alphabet:
                    csv_filenames.append(f"{g}_{l}.csv")

            #################### CREATE / JOIN TABLE ######################
            # READ & CREATE TABLES based on projection criteria 
            # JOIN & Filter Data based on criteria
            # Merge all files / create temporary file

            # Initialize lists to store filtered data and chunk sums
            filtered_data_list = []
            total_chunk_sum = []

            for i in csv_filenames:
                file_path = os.path.join(directory, i)

                # Define chunk size to read in the data
                chunk_size = 2000000
                chunk_sum = 0  # Initialize chunk_sum for each file

                try:
                    data_chunks = pd.read_csv(file_path, chunksize=chunk_size)

                    for chunk in data_chunks:
                        # Filter for each name only within the chunk
                        if parse[1].startswith('[') and parse[1].endswith(']'):
                            content_in_brackets = parse[1][1:-1].replace(',', ', ').split(', ')
                            # Filter the data for exact matches or names starting with the letters in the list
                            filtered_for_names = chunk[chunk['Name'].str.lower().isin([name.lower() for name in content_in_brackets]) |
                                                    chunk['Name'].str.lower().str.startswith(tuple([letter.lower() for letter in content_in_brackets]))]
                        else:
                            # Filter the data for exact matches or names starting with the specified letter
                            filtered_for_names = chunk[chunk['Name'].str.lower().isin([parse[1].lower()]) |
                                                    chunk['Name'].str.lower().str.startswith(parse[1].lower())]

                        # Filter for the specified years
                        filter_year = filtered_for_names[filtered_for_names['Year'].isin(years)]

                        # Add the data filter_year to filtered_data_list
                        filtered_data_list.append(filter_year)
                        
                        # APPLY CONDITIONS
                        if parse[5] == "sum":
                            chunk_sum += filter_year['Count'].sum()  # Sum for each chunk

                    total_chunk_sum.append(chunk_sum)  # Add the total sum for the file

                except FileNotFoundError:
                    # Handle the case where the file is not found
                    print(f"File not found: {file_path}. Skipping...")
                    continue

            # Concatenate all filtered data into one DataFrame
            data = pd.concat(filtered_data_list, ignore_index=True)
                
            #################### APPLY CONDITIONS ######################
            # Apply conditions to the joined table
            # Prep Data
            str_gender = '/'.join(gender)
            no_print = 25

            # PARSE [5]: AGGREGATE
            if parse[5] == 'count':
                count_rows = len(data)
                count_rows = '{:,.0f}'.format(count_rows)
                print_out = (f"For {str_gender} names {parse[1]} during {parse[3]}, total number of rows/records is {count_rows}.")

            elif parse[5] == 'sum':
                total_sum = sum(total_chunk_sum)
                total_sum = '{:,.0f}'.format(total_sum)
                print_out = (f"For {str_gender} during {parse[3]}, total number named {parse[1]} is {total_sum}.")

            elif parse[5] != 'sum' or parse[5] != 'count':

                if parse[5] == 'top':
                    if parse[6] == None or not parse[6].isnumeric() :
                        print('/nQUERY ERROR: Please enter numerical value for top condition.')
                    else:
                        data = data.sort_values(by=['Count'], ascending=False)
                        data = data.head(int(parse[6]))
                        print_out = data.head(no_print).to_string(index=False)

                elif parse[5] == 'bottom':
                    if parse[6] == None or not parse[6].isnumeric() :
                        print('/nQUERY ERROR: Please enter numerical value for bottom condition.')
                    else:
                        data = data.sort_values(by=['Count'], ascending=True)
                        data = data.head(int(parse[6]))
                        print_out = data.head(no_print).to_string(index=False)

                elif parse[5] == 'group':
                    if parse[6] == None:
                        print('/nQUERY ERROR: Please enter numerical value for top condition.')
                    else:
                        data = data.groupby(parse[6], as_index=False)['Count'].sum()
                        print_out = data.head(no_print).to_string(index=False)
                
                # PARSE[8]: ORDER BY
                if parse[8].startswith('[') and parse[8].endswith(']'):
                    content_in_8 = parse[8][1:-1].replace(',', ', ').split(', ')
                    order_list = [False if i == 'desc' else True for i in content_in_8]
                            
                    content_in_10 = parse[10][1:-1].replace(',', ', ').split(', ')
                    data = data.sort_values(by=content_in_10, ascending=order_list)
                    print_out = data.head(no_print).to_string(index=False)

                elif parse[8] == 'desc':
                    if parse[10] == None:
                        print('/nQUERY ERROR: Please enter value(s) for BY condition.')
                    else:
                        bool = False
                        data = data.sort_values(by=parse[10], ascending=bool)
                        print_out = data.head(no_print).to_string(index=False)

                elif parse[8] == 'asc':
                    if parse[10] == None:
                        print('/nQUERY ERROR: Please enter value(s) for BY condition.')
                    else:
                        bool = True
                        data = data.sort_values(by=parse[10], ascending=bool)
                        print_out = data.head(no_print).to_string(index=False)
                       
                else:
                    print("/nQUERY ERROR: Invalid ORDER or BY entry.)")

                # PARSE[12]: RETURN COLUMNS
                if parse[12] == 'all':
                    print_out = data.head(no_print).to_string(index=False)
                 
                elif parse[12].startswith('[') and parse[12].endswith(']'):
                    content_in_12 = parse[12][1:-1].replace(',', ', ').split(', ')
                    data = data[content_in_12]
                    print_out = data.head(no_print).to_string(index=False)
           
                else:
                    data = data[parse[12]]
                    print_out = data.head(no_print).to_string(index=False)

            #################### OPTION TO SAVE QUERY RESULTS ######################
            # Ask the user if they want to save the results to a CSV file
            save_to_csv = input("\nDo you want to save the results to a CSV file? (yes/no): ").lower()

            if save_to_csv == 'yes':
                # Get the desired filename from the user
                csv_filename = input("Enter the desired CSV filename (e.g., query_results.csv): ")

                # Save the results to the CSV file
                try:
                    query_result_df = pd.DataFrame(data)
                    query_result_df.to_csv(csv_filename, index=False)
                    saved_csv = (f"Results saved to {csv_filename} successfully.")
                except Exception as e:
                    print(f"\nError saving results to CSV: {e}")
            else:
                saved_csv = ("Results not saved to a CSV file.")

            #################### RETURN QUERY RESULT ######################
            print_line_2 = (f"\nRESULTS:")
            no_rows = (f"\nTotal number of rows after applying all query conditions: {len(data)}")
            note = (f"NOTE: Prints up to {no_print} results.")

            if parse[5] == 'count' or parse[5] == 'sum':
                query_result = [print_line_2, print_out, saved_csv]

            else:
                query_result = [print_line_2, print_out, no_rows, note, saved_csv]

            return query_result

        except Exception as e:
            print(f"Error executing the query: {e}")
            return []

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
        print("6. Query")
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
            print("\nData loaded successfully from the CSV file!")

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
                print(f"\nThe count for the record was updated successfully!")
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

        elif choice == '6':
            result = database.query(directory)
            if result is not None:
                for record in result:
                    print(record)
            else:
                print("\nError executing the query.")

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
