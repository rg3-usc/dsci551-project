# Non-relational interactive command line interface (CLI)
#### Source dataset for demo: [Yelp Businesses dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset?select=yelp_academic_dataset_business.json)

## Initiate non-relational interactive command line interface
- Terminal command to initiate CLI: `python src/json_cli.py`<br>
- Enter file path when prompted (a new file will be created automatically if file does not exist)
- Additionally, when starting a new file, enter the primary key of the dataset. 
    ```
    Enter the path to the database file: data/tests/demo.json
    Enter the primary key for the dataset: business_id
    ```
- If the file is not new, the primary key should have been saved from the initial session and it is not required to re-enter. Instead, the key value will be printed.
    ```
    Enter the path to the database file: data/tests/demo.json
    Using primary key: business_id
    ```
- Enter a number to issue a command (detailed below)
## Available commands
1. Insert data (1 record in JSON format)<br>
    ```
    Enter your choice: 1
    Enter the JSON data to insert: {"business_id":"Pns2l4eNsfO8kk83dixA6A","name":"Abby Rappoport, LAC, CMQ","address":"1616 Chapala St, Ste 2","city":"Santa Barbara","state":"CA","postal_code":"93101","latitude":34.4266787,"longitude":-119.7111968,"stars":5.0,"review_count":7,"is_open":0,"attributes":{"ByAppointmentOnly":"True"},"categories":"Doctors, Traditional Chinese Medicine, Naturopathic\/Holistic, Acupuncture, Health & Medical, Nutritionists","hours":null}
    
    Data inserted successfully.
    ```
    - The primary key field must be included in the json entry to succesfully be inserted.
    - If the key of the record already exists, a prompt will confirm if the record should be replaced. 
        - If confirmed, the whole record is replaced with the input. Otherwise, the operation is cancelled and no data will be inserted.<br><br>

2. Insert data (Batch insert of JSON file)<br>
    - When prompted, enter path to JSON file to batch import. (File should contain lines with each JSON record; NOT in an array)
        ```
        Enter your choice: 2
        Enter the path to the JSON file: data/external/yelp_academic_dataset_business.json
        
        Data inserted successfully.
        ```
    - Any records missing the primary key field are skipped from being inserted.
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
        Enter the new JSON value: {"name":"Test name change","address":"Test address change","new_field":"new"}
        
        'Pns2l4eNsfO8kk83dixA6A' updated successfully.
        ```
4. Delete data<br>
    - When prompted enter the primary key that should be deleted
        ```
        Enter your choice: 4
        Enter the key to delete: Pns2l4eNsfO8kk83dixA6A
        
        Data deleted successfully.
        ```
        <br>
5. Display data<br>
    - Prints out (chunk-by-chunk) the current file content<br><br>
6. Query data<br>
    - When prompted enter query operation(s)
    - Enter '?help' to see documentation on syntax and examples of all available query commands<br>

    |Query Operation | Syntax | Example | Notes |
    | ----------- | ----------------- | -------- | ------- |
    |Show | `show <field(s)>`  | `show name stars` |  |
    |Filter (comparison) | `filter <field> <comparison condition>`  | `filter stars > 4` | < > <= >= = != |
    |Filter (substring matches) | `filter <field> contains <string or list>`  | `filter name contains 'Target'` <br>`filter state contains ['CA','AZ']` |  |
    |Filter (rows) | `filter rows <[range and/or list]>`  | `filter rows [1:100, 200]` |  |
    |Order | `order <field(s)>`  | `order -stars name` |asc by default; -<field> for desc |
    |Find (Count) | `find count [optional: by <group_field>]`  | `find count by state` |  |
    |Find (Aggregation) | `find <aggregation> <field> [optional: by <group_field>]`  | `find average stars by state` |averge, sum, min, max   |
    |Save Result | `save as <file_path> `  | `save as output.json` |  |
    |Join | `join with <file_path> by <field(s)>`  | `join with reviews.json by business_id` |  |
    
    - Multiple operations can be performed by separating operations with | 
        - Note: Query operations are performed sequentially so the order of the query commands are important.
            - `filter stars>=4 | filter rows [1:10]`: First records are filtered for where stars >=4 <u>THEN</u> first ten rows are filtered
    - Example of combined projecting, subsetting, and sorting query commands: <br>
    ```
    Enter your choice: 6
    Enter your custom query (enter '?help' for syntax and examples): show name state stars review_count | order -review_count | filter state contains ['CA','AZ'] | filter stars>=4 | filter rows [1:10]
    
    {"name": "Los Agaves", "state": "CA", "stars": 4.5, "review_count": 3834}
    {"name": "Brophy Bros - Santa Barbara", "state": "CA", "stars": 4.0, "review_count": 2940}
    {"name": "Boathouse at Hendry's Beach", "state": "CA", "stars": 4.0, "review_count": 2536}
    {"name": "Santa Barbara Shellfish Company", "state": "CA", "stars": 4.0, "review_count": 2404}
    {"name": "Prep & Pastry", "state": "AZ", "stars": 4.5, "review_count": 2126}
    {"name": "Mesa Verde", "state": "CA", "stars": 4.5, "review_count": 1796}
    {"name": "La Super-Rica Taqueria", "state": "CA", "stars": 4.0, "review_count": 1759}
    {"name": "McConnell's Fine Ice Creams", "state": "CA", "stars": 4.5, "review_count": 1537}
    {"name": "The Lark", "state": "CA", "stars": 4.0, "review_count": 1520}
    {"name": "The Palace Grill", "state": "CA", "stars": 4.5, "review_count": 1500}

    ```
    - Example of grouping, aggregating, saving and joining query commands: <br>
    ```
    Enter your choice: 6
    Enter your custom query (enter '?help' for syntax and examples): find count by state | order -count | save as data/tests/count_by_state
    
    Result saved successfully at: data/tests/count_by_state
    ```
    ```
    Enter your choice: 6
    Enter your custom query (enter '?help' for syntax and examples): find average stars by state | join with data/tests/count_by_state by state | order -average_stars 

    {"state": "MT", "average_stars": 5.0, "count": 1}
    {"state": "CO", "average_stars": 4.0, "count": 3}
    {"state": "XMS", "average_stars": 4.0, "count": 1}
    {"state": "HI", "average_stars": 4.25, "count": 2}
    {"state": "SD", "average_stars": 4.5, "count": 1}
    {"state": "UT", "average_stars": 4.5, "count": 1}
    {"state": "VT", "average_stars": 4.5, "count": 1}
    {"state": "DE", "average_stars": 3.3549668874172185, "count": 2265}
    {"state": "IL", "average_stars": 3.3696969696969696, "count": 2145}
    {"state": "AB", "average_stars": 3.447514803516957, "count": 5573}
    {"state": "NJ", "average_stars": 3.4591143392689783, "count": 8536}
    {"state": "WA", "average_stars": 3.5, "count": 2}
    {"state": "MO", "average_stars": 3.546091817098873, "count": 10913}
    {"state": "TN", "average_stars": 3.571499668214997, "count": 12056}
    {"state": "PA", "average_stars": 3.5730191838773173, "count": 34039}
    {"state": "IN", "average_stars": 3.5882457544234017, "count": 11247}
    {"state": "AZ", "average_stars": 3.5920096852300243, "count": 9912}
    {"state": "FL", "average_stars": 3.6109570831750855, "count": 26330}
    {"state": "LA", "average_stars": 3.679161628375655, "count": 9924}
    {"state": "ID", "average_stars": 3.7076337586747257, "count": 4467}
    {"state": "NV", "average_stars": 3.7368762151652626, "count": 7715}
    {"state": "CA", "average_stars": 3.9967326542379396, "count": 5203}
    {"state": "NC", "average_stars": 2.0, "count": 1}
    {"state": "MI", "average_stars": 2.5, "count": 1}
    {"state": "VI", "average_stars": 2.5, "count": 1}
    {"state": "TX", "average_stars": 2.875, "count": 4}
    {"state": "MA", "average_stars": 1.25, "count": 2}
    ```
7. Exit<br>
    - Ends the CLI session

##### Note: In line 9 of the script, `CHUNK_SIZE` is assigned a default value of 5000 which signifies that 5000 records are processed at a time when processing modifications or queries of the data. User may change this value to accomodate their memory usage needs.