# Relational interactive command line interface (CLI)
#### Source dataset for demo: [Baby Names dataset](https://www.kaggle.com/datasets/kaggle/us-baby-names/)
<br>

## Initiate relational interactive command line interface
- To initiate CLI: `python src/csv_cli.py`<br>
- Enter file path when prompted (a new file will be created automatically if file does not exist)
  
    ```
    Enter the path to the database file: data/tests/test.csv
    ```
- Enter a number to begin command (detailed below)

## Available commands
1. Insert data (1 record in CSV table)<br>
    ```
    Enter your choice: 1
    Enter the name to add:
    Enter year of birth:
    Enter gender (M or F):
    
    New entry for <nmae> added successfully!
    ```
    - If the key of the record already exists, a prompt will confirm if the record should be replaced. **CONFIRM - okay to entry duplicate entries**
    - If confirmed, the whole record is added to the CSV database. Otherwise, the operation is cancelled and no data will be inserted. **CONFIRM this**<br><br>

2. Insert data (Batch upload existing CSV file)<br>
    - When prompted, enter path to existing CSV file to batch import.
        ```
        Enter your choice: 2
        Enter the path to the CSV file: data/external/baby_names.csv
        
        Data loaded successfully from the CSV file.
        ```
    - **CONFIRM - what happens with duplicate entries or when there is already existign data**.<br><br>

3. Delete existing entry <br>
    - When prompted enter the releveant key variables so the code can search for the appropriate record to delete.
    - **CONFIRM - what happens if there are duplicate records? Does the code delete both?**
   
        ```
        Enter your choice: 3
        Enter the name to add:
        Enter year of birth:
        Enter gender (M or F):
    
        Record for <name> deleted successfully!
        ```
        <br>
4. Edit existing entry<br>
    - When prompted enter the releveant key variables so the code can search for the appropriate record.
    - Then for each key variables, enter the updated value changes. If a change is not required, enter the same value as the original. **CONFIRM - maybe change this**
        ```
        Enter your choice: 4
        Enter name to edit:
        Enter year of birth:
        Enter gender (M or F):
        Enter the new name:
        Enter the new year of birth:
        Enter the new gender (M or F):
        
        Data deleted successfully.
        ```
    - If record does not exist in the database, the message will read "No record found for Veana in  with gender M." <br>
5. Display data<br>
    - Prints out current file content<br><br>
6. Exit<br>
    - Ends the CLI session
