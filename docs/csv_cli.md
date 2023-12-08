# Relational interactive command line interface (CLI)
#### Source dataset for demo: [Baby Names dataset](https://www.kaggle.com/datasets/kaggle/us-baby-names/)
<br>

## Initiate relational interactive command line interface
To initiate CLI, depending on the approach, use one of the following:
- `python src/csv_cli.py`
- `python3 csv_cli.py` <br>

Enter file path when prompted (a new folder will be created automatically if folder does not exist).
- If you are already in the directory where you wish to save the folder, simply enter the folder name.
  
    ```
    Enter the directory where data chunks will be stored: <folder name>
    ```
    
- If not, define your path including the folder name:
    ```
    Enter the directory where data chunks will be stored: /Users/name/Documents/<folder name>
    ```

## Available commands
1. Insert new record <br>
    - When prompted, enter the record to insert by indicating the name, year of birth, and gender.

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
   - A sample data file is provided in this Github repo: sample_data.csv

3. Delete existing entry <br>
   - When prompted, enter the record to delete by indicating the name, year of birth, and gender.

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
          - If value out of range, error message will appear: Invalid input. Please enter a valid number.
          
4. Update existing entry<br>
    - When prompted, enter the record to update by indicating the name, year of birth, and gender.
    - Only total count may be updated using this option. Use Insert or Delete for all other edits to the data.
        ```
        NOTE: Only total count may be updated using this option. Use Insert or Delete for all other edits to the data.
        Enter the name to add: <enter first name>
        Enter 4-digit year of birth: <enter 4-digit year of birth>
        Enter gender (M or F): <enter M or F>
        
        Matching records for <name> in <year> with gender <M or F>:
        1. {'Id': '1825048', 'Name': 'Name', 'Year': 'year', 'Gender': 'M or F', 'Count': '1'}
        
        Enter the new count for Rylund in 2014 with gender M: <enter number>
        
        Count updated successfully for <name> in <year> with gender <M or F>.
        
        The count for the record was updated successfully!
        ```
    - If record does not exist in the database, the message will read "No record found for <name> in <year> with gender <M or F>."
    - The value entered must be greater than 0. <br>
    
5. Display data<br>
    - Prints out list of available CSV files / data chunks.
    - Select list to view content.
      ```
      Enter your choice: 5
  
      Available CSV files:
      female_e.csv
      female_i.csv
      male_m.csv
  
      Enter the filename to display data (e.g., male_a.csv): <enter desired file>
      ```
6. Query<br>
    
    - Upon selection, a list of instruction will appear with examples on how to write the query.
    
      ```
      Enter your choice: 6
      
      INSTRUCTIONS: Follow example formats below exactly.
      
      SYNTAX: FIND <name> <gender> <year> CONDITION <aggregate> <value> ORDER <order> BY <col name> RETURN <col names>
            <name> a-z, a, [a,b,d,m], Kevin, or [Kevin,Sarah,Dave]
            <gender> M, F, Female, M-F or M/F
            <year> 2009, 2000-2009, or [2000,1999,2010]
            <aggregate> count, sum, top, bottom, or None
            <value> numeric (e.g., 10, 100)
            <order> desc, asc, [desc,asc] or None
            <col name> col1, col2, [col1,col2] or None
            <col name> col1, col2, [col1,col2] or all
      
      EXAMPLES:
            Find Riley M/F 1995-2000 CONDITION None None ORDER [asc,asc] BY [Gender,Count] RETURN all
            FIND [Mary,Ivy] F [2000,1983,2010,2020] CONDITION None None ORDER [asc,desc] BY [Name,Year] RETURN [Name,Year,Gender,Count]
            FIND Kevin M 2010-2014 CONDITION sum
            FIND e F 2014 CONDITION top 10 ORDER desc BY Count Return all
      
      EXAMPLE ERROR HANDLING:
            FIND Kevin lksjdfljs 2010-2014 CONDITION sum
            FIND Kevin M 200009 CONDITION CONDITION sum
      ```
      
    - Enter the query.
    - Prompt will appear to inquire if query results should be saved. If yes, enter the desired file name.

      ```
      ENTER YOUR QUERY: <enter query here>

      Do you want to save the results to a CSV file? (yes/no):
      Enter the desired CSV filename (e.g., query_results.csv): 
      ```   
   - Only the first 25 rows of the results of the query will print out.
   - Below is an example a query and its result:
      ```
      ENTER YOUR QUERY: Find Riley M/F 1995-2000 CONDITION None None ORDER [asc,asc] BY [Gender,Count] RETURN all
      
      Do you want to save the results to a CSV file? (yes/no): yes
      Enter the desired CSV filename (e.g., query_results.csv): test.csv

      RESULTS:
           Id  Name  Year Gender  Count
      1196926 Riley  1995      F    969
      1222967 Riley  1996      F   1148
      1249322 Riley  1997      F   1669
      1276269 Riley  1998      F   1877
      1304132 Riley  1999      F   2312
      1332669 Riley  2000      F   2552
      1212563 Riley  1995      M   1918
      1238754 Riley  1996      M   2393
      1265428 Riley  1997      M   2833
      1321059 Riley  1999      M   2970
      1292824 Riley  1998      M   3013
      1350299 Riley  2000      M   3420
      
      Total number of rows after applying all query conditions: 12
      NOTE: Prints up to 25 results.
      Results saved to test.csv successfully. 
      ```  
7. Clear Data<br>
    - Clears all csv files from the data path defined when the script was called.
    - Below is an example for results of the run.
      ```
      Enter your choice: 7
      File 'female_o.csv' removed.
      File 'female_m.csv' removed.
      File 'female_i.csv' removed.
      File 'male_l.csv' removed.
      File 'male_k.csv' removed.
      File 'male_p.csv' removed.
      File 'male_f.csv' removed.
      File 'male_r.csv' removed.
      File 'male_b.csv' removed.
      File 'female_r.csv' removed.
      File 'female_e.csv' removed.
      File 'female_s.csv' removed.
      File 'female_t.csv' removed.
      
      Data has been cleared.
      ``` 
8. Exit<br>
    - Exit from the script.


