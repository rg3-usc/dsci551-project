# Relational interactive command line interface (CLI)
#### Source dataset for demo: [Baby Names dataset](https://www.kaggle.com/datasets/kaggle/us-baby-names/)
<br>

## Initiate relational interactive command line interface
- To initiate CLI, depending on the approach and your system, use one of the following:
  - `python src/csv_cli.py`
  - `python3 csv_cli.py` <br>
- Enter file path when prompted (a new folder will be created automatically if folder does not exist). If you are already in the directory where you wish to save the folder, simply enter the folder name. If not, define your path including then include the folder name:
  
    ```
    Enter the directory where data chunks will be stored: data_chunks
    ```
    OR
    ```
    Enter the directory where data chunks will be stored: /Users/name/Documents/data_chunks
    ```

## Available commands
1. Insert new record <br>
    ```
    Enter your choice: 1
    Enter the name to add: <enter first name>
    Enter 4-digit year of birth: <enter 4-digit year of birth>
    Enter gender (M or F): <enter M or F>
    
    New entry for <name> added successfully!
    ```
    - Once successfull, the record is added to the appropriate CSV file.
    - If either name and/or gender is left blank operation will error and no data will be inserted.<br>

2. Batch load data from exsiting CSV<br>
    - When prompted, enter path to existing CSV file to batch import.
        ```
        Enter your choice: 2
        Enter CSV filepath: datapath/filename.csv
        
        Data loaded successfully from the CSV file!
        ```
   - Do not batch load the same CSV twice. Clear the data (Choice 7) if for some reason the same batch needs to be uploaded again.

3. Delete existing entry <br>
   - When prompted, enter the record to delete by indicating the name, year of birth and gender.

        ```
        Enter your choice: 3
        Enter the name to add: <enter first name>
        Enter 4-digit year of birth: <enter 4-digit year of birth>
        Enter gender (M or F): <enter M or F>

        Matching records for <name> in <year> with gender <M or F>:
        1. {'Id': '0', 'Name': 'name', 'Year': 'year', 'Gender': 'M or F', 'Count': '5'}

        Enter the number of records to delete (1-5 or 'all'):
   
        The record for <name> in <year> with gender <M or F> deleted successfully!
        ```
        - Once record is entered, a list will appear showing the record(s) found and a prompt will ask for the number of records to delete.
        - Select the allowable range.
          - If 'all' or max number in range is selected, the entire row will be deleted.
          - If greater than max value selected, error message will appear: Invalid input. Please enter a valid number.
          - 
          
5. Edit existing entry<br>
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
6. Display data<br>
    - Prints out current file content<br><br>
7. Exit<br>
    - Ends the CLI session
