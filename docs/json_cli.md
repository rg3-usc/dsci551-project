# Non-relational interactive command line interface (CLI)
#### Source dataset for demo: [Yelp Businesses dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json)
<br>

## Initiate non-relational interactive command line interface
- To initiate CLI: `python src/json_cli.py`<br>
- Enter file path when prompted (a new file will be created automatically if file does not exist)
- Additionally, when starting a new file, enter the primary key of the dataset. 
    ```
    Enter the path to the database file: data/tests/test.json
    Enter the primary key for the dataset: business_id
    ```
- If the file is not new, the primary key should have been saved from the initial session and it is not required to re-enter. Instead, the key value will be printed.
    ```
    Enter the path to the database file: data/tests/test.json
    Using primary key: business_id
    ```
- Enter a number to begin command (detailed below)
## Available commands
1. Insert data (1 record in JSON format)<br>
    ```
    Enter your choice: 1
    Enter the JSON data to insert: {"business_id":"Pns2l4eNsfO8kk83dixA6A","name":"Abby Rappoport, LAC, CMQ","address":"1616 Chapala St, Ste 2","city":"Santa Barbara","state":"CA","postal_code":"93101","latitude":34.4266787,"longitude":-119.7111968,"stars":5.0,"review_count":7,"is_open":0,"attributes":{"ByAppointmentOnly":"True"},"categories":"Doctors, Traditional Chinese Medicine, Naturopathic\/Holistic, Acupuncture, Health & Medical, Nutritionists","hours":null}
    
    Data inserted successfully.
    ```
    - If the key of the record already exists, a prompt will confirm if the record should be replaced. 
        - If confirmed, the whole record is replaced with the input. Otherwise, the operation is cancelled and no data will be inserted.<br><br>

2. Insert data (Batch insert of JSON file)<br>
    - When prompted, enter path to JSON file to batch import. (File should contain lines with each JSON record; NOT in an array)
        ```
        Enter your choice: 2
        Enter the path to the JSON file: data/external/yelp_academic_dataset_business.json
        
        Data inserted successfully.
        ```
    - If at least one key already exists, a one-time prompt will confirm if any and all duplicate keys should be replaced.
        - If yes, any instance of a repeated key will be replaced with the new record from the file. Otherwise, only new keys will be inserted and the keys that previously existed are skipped / not updated.<br><br>
        ```
        Enter your choice: 2
        Enter the path to the JSON file: data/external/yelp_academic_dataset_business.json
        
        At least one record whose primary key already exists in the database. Batch insert will replace the values of keys that already exist. Would you still like to proceed? (y/n): y
        Replacing values for key 'Pns2l4eNsfO8kk83dixA6A'.

        Data inserted successfully.
        ```

3. Update data (JSON format)<br>
    - When prompted enter the primary key that should be updated
    - Then enter (in JSON format) the updated field and value changes
        - Note: this will update only the specified fields and not the whole record. Any previous fields will be maintained.
        ```
        Enter your choice: 3
        Enter the key to update: Pns2l4eNsfO8kk83dixA6A
        Enter the new JSON value: {"name":"Test name change","address":"Test address change"}
        
        Data updated successfully.
        ```
        <br>
4. Delete data<br>
    - When prompted enter the primary key that should be deleted
        ```
        Enter your choice: 4
        Enter the key to delete: Pns2l4eNsfO8kk83dixA6A
        
        Data deleted successfully.
        ```
        <br>
5. Display data<br>
    - Prints out current file content<br><br>
6. Exit<br>
    - Ends the CLI session