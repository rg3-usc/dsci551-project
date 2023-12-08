# DSCI 551 Project: Designing database systems with custom query language
By: Veasna Tan & Richard Gallardo

## Project Overview
The project aims to design and implement database systems supporting both relational and non-relational data models. Each respective system includes a interactive command interface and supports a unique query language.

## I. Relational Database
#### Dataset source: [US Baby Names (Kaggle)](https://www.kaggle.com/datasets/kaggle/us-baby-names/)
### A. Usage
- Install dependencies: 
    - `pip install pandas`
- Use interactive CLI:
    - `python src/csv_cli.py`
    - Documentation / demo of all features of the CLI: [docs/csv_cli.md](docs/csv_cli.md)
### B. Features
- Data Model
    - Data stored as .csv tables across multiple files
- Schema
    - Headers: Id, Name, Year, Gender, Count
- Memory Handling
    - Data stored as tables in separate chunks categorized by gender and first letter of name (e.g. female_k.csv)
- Data Modification
    |Data Modification | Details | Processing Details |
    | ----------- | ----------------- | -------- | 
    |Insertion | Records can be inserted one-at-a-time or via a batch insertion from an existing list of names from a CSV file | Insertions are placed into chunks based on  gender and the first letter of each name. If the name and year combination does not exist, the new record is appended to the data, a unique Id is assigned, and the count is assigned as ‘1’. If the name combination exists within the CSV, the existing record will increase the count by +1.|
    |Update | Updating the counts of records is supported | To update the count of a record, the user must provide the name, year and gender. If there is a matching record, the user may enter the new count for the record. |
    |Delete | Deletion of a record is supported | To delete a record, the user must provide the name, year and gender. If there is a matching record, the user will be shown that record and prompted to either enter 'all' to delete all record or specify the number of those records they’d like to delete from the count. |
- Query Language <br>
The query system for this relational database uses the following syntax:<br> `FIND <name> <gender> <year> CONDITION <aggregate> <value> ORDER <order> BY <col name> RETURN <col names>`<br><br>
This syntax is designed for the query to be parsed in the below order which identifies the chunks it will need to process to fulfill the query:
    |Query Operation | Syntax | Memory Usage Notes |
    | ----------- | ----------------- | -------- | 
    |Filtering (subset criteria)<br>&<br>Joining |`FIND <name> <gender> <year>`| Defines the criteria for name, gender and year and creates a list for each. <br><br>For example, if `FIND [Dale,Beth,John] M [2000-2005]` the following lists are created:<br> alphabet = [‘b’, ‘d’, ‘j’]<br>gender = [‘male’]<br>years = [2000, 2001, 2002, 2003, 2004, 2005]<br><br> Using the defined list of criteria, a list of csv file chunks are identified to be processed. Filters are applied to each chunk of data and then joined/appended back together based on the criteria(s) needed.| 
    |Grouping / Aggregation| `CONDITION <aggregate> <value>` | The data may be aggregated by count, sum, top, bottom, or None with a numeric value entered (e.g., 10, 100) to further define aggregate criteria. |
    | Ordering (sort rows) | `ORDER <order> BY <col name>` | The data may be ordered as desc, asc, [desc,asc] or None. It can be ordered by \<col name> col1, col2, [col1,col2] or None.|
    |Projection (subset columns) | `RETURN <col names>` | Return select column names: col1, col2, [col1,col2] or 'all'|

## II. Non-Relational Database
#### Dataset source: [Yelp Dataset (Kaggle)](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset)
### A. Usage
- Install dependencies: 
    - `pip install json tabulate`
- Use interactive CLI:
    - `python src/json_cli.py`
    - Documentation / demo of all features of the CLI: [docs/json_cli.md](docs/json_cli.md)
### B. Features
- Data Model
    - Each dataset is stored as an individual JSON file with field-value pairs
- Schema
    - Flexible schema with the primary key field configured at initiation of the database
- Memory Handling
    - All database modifications and queries are processed in chunks. Modifications and queries are processed as external temporary file(s) before overwriting (modifications) or printing (queries)
    - Note: In line 9 of the [src/json_cli.py](src/json_cli.py), `CHUNK_SIZE` is assigned a default value of 5000 which signifies that 5000 records are processed at a time when processing modifications or queries of the data. Users may change this value to accomodate their memory usage needs.
- Data Modification

    |Data Modification | Details | Processing Details |
    | ----------- | ----------------- | -------- | 
    |Insertion | JSON records can be inserted one-at-a-time or via a batch insertion from an existing JSON source file.   | Insertions are performed by appending the record into the database and does not require reading the entire dataset into memory. <br><br> All primary keys are stored as a set (only primary keys, not the entire dataset). If the user is attempting to re-insert a previously inserted key, a prompt will confirm if the user wants to overwrite the data or skip the insertion. When overwriting, the previous record is deleted and the new record is appended.|
    |Update | Updates to specific fields and/or new field(s) insertions are supported  | Updates are performed by splitting up the dataset into chunks, updating the record's fields if the record exists in the chunk, then creating an external intermediate file that appends together the split chunks which replaces the prevous file. |
    |Delete | Deletion of a record is supported | Deletions are performed by splitting up the dataset into chunks, deleteing the record from the chunk that exists in, then creating an external intermediate file that appends together the split chunks which replaces the prevous file. |

- Query Language<br>
    - Multiple operations can be performed sequentially by separating each command with |
        - Each query operation is performed one-by-one and processed externally. An intermediate input file is used as the source data for the current query operation, and an intermediate output file is created as result of a query operation. This resulting output file would act as the input file for the next operation, if applicable.
        - e.g. `filter stars > 4.5 | filter state contains ['CA','NY'] | show name stars review_count | order review_count`
            - The intermediate input file is the unmodified dataset for the `filter stars > 4.5` query to process. The intermediate output result of this query is the input data for `filter state contains ['CA','NY']` query, and so on and so forth (sequentially) until the final output results of the `order review_count` is then printed.
    - Enter '?help' when prompted to enter a query in the CLI to view details in syntax and examples

    |Query Operation | Syntax | Processing Details |
    | ----------- | ----------------- | -------- | 
    |Projection (subset columns) | `show <field(s)>`  | Processed one chunk at a time to apply modifications and appended back together |
    |Filtering (subset rows) | `filter <field> <comparison operator* condition>`<br>*< > <= >= = !=<br><br>`filter <field> contains <'string' or ['list','of strings']>`<br><br>`filter rows <range and/or list (e.g. [1:100, 200])>`  | Processed one chunk at a time to apply modifications and appended back together |
    |Ordering (sort rows) | `order <field(s)>` <br> * asc by default; -\<field> for desc  | External sort: each unique value of the first sort field has its own intermediate file where all records get allocated to and then later appended as a fully sorted output. |
    |Grouping / Aggregation | `find count [optional: by <group_field>]`<br><br>`find <average, sum, min, or max> <field> [optional: by <group_field>]`  | Processed one chunk at a time. The results of each chunk are combined and grouped/aggregated once more for the final result.  |
    |Saving Query View | `save as <file_path>`  | Results of a query can be saved as a json file which may later be used to perform a join with |
    |Joining | `join with <file_path> by <field(s)>`  | External sort-merge-join: each unique value of the first join field will be partitioned into intermediate files (one from each data source). Each pair of intermediate files are joined to create joined, intermediate files which are appended into a final resulting output of joined records. |
