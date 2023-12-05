# DSCI 551 Project: Designing database systems with custom query language
By: Veasna Tan & Richard Gallardo

## Project Overview
The project aims to design and implement database systems supporting both relational and non-relational data models. Each respective system includes a interactive command interface and supports a unique query language.

## I. Relational Database
#### Dataset source: [US Baby Names (Kaggle)](https://www.kaggle.com/datasets/kaggle/us-baby-names/)
### A. Features
- Data Model
    - Data stored as .csv tables across multiple files
- Schema
    - Headers: Id, Name Year, Gender
- Storage / Memory Handling
    - Data stored as tables in separate chunks categorized by gender and first letter of name (e.g.: female_k.csv)
- Data Modification
    - \<placeholder>
- Query Commands
    - \<placeholder>
### B. Usage
- Install dependencies: 
    - `pip install <placeholder (if necessary)>`
- Use interactive CLI:
    - `python src/csv_cli.py`
    - Documentation / demo of all features of the CLI: [docs/csv_cli.md](docs/csv_cli.md)

## II. Non-Relational Database
#### Dataset source: [Yelp Dataset (Kaggle)](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset)
### A. Features
- Data Model
    - Each dataset is stored as an individual JSON file with field-value pairs
- Schema
    - Flexible schema with the primary key field configured at initiation of the database
- Memory Handling
    - All database modifications and queries are processed in chunks and are processed externally as temporary file(s) before overwriting (when modifying) or printing (for queries)
- Data Modification
    - Insertion: JSON records can be inserted one-at-a-time or via a batch insertion from an existing JSON source file. 
        - Insertions are performed by appending the record into the database and does not require reading the entire dataset into memory. 
        - All primary keys are stored as a set (only primary keys, not the eniter dataset). If the user is attempting to re-insert a previously inserted key, a prompt will confirm if the user wants to overwrite the data or skip the insertion.
    - Update: specified field updates or new field(s) insertions are supported
    - Delete: deletion of a record is supported
        - Updates and deletions are performed by splitting up the dataset into chunks, updating or deleting the record if it exists in the chunk, then creating an external intermediate file that appends together the split chunks back that replaces the prevous file.
- Query Commands<br>
    - Multiple operations can be performed sequentially by separating each command with |
        - e.g. `filter stars > 4.5 | filter state contains ['CA','NY'] | show name stars review_count | order review_count`
        - Each query operation is performed one-by-one and processed externally. An intermediate input file is used as the source data for the current query operation, and an intermediate output file is created as result of a query operation. This resulting output file would act as the input file for the next operation, if applicable.
    - Enter '?help' when prompted to enter a query in the CLI to view details in syntax and examples
    - Projection (subset columns):
        - Syntax: `show <field(s)>`
        - Processed one chunk at a time
    - Filtering (subset rows):
        - Syntax (comparison operators): `filter <field> <comparison condition>`
            - allowed operators:  < > <= >= = !=  
        - Syntax (substring matches): `filter <field> contains <'string' or ['list','of strings']>`
        - Syntax (row subset): `filter rows <range and/or list (e.g. [1:100, 200])>`
        - Processed one chunk at a time
    - Ordering (sort rows):
        - Syntax: `order <field(s)>`
            - ascending by default; `-<field>` for descending
        - External sort: each unique value of the first sort field has its own intermediate file where all records get allocated to and then later appended as a fully sorted output.
    - Grouping / Aggregation
        - Syntax (count): `find count [optional: by <group_field>]`
        - Syntax (aggregation): `find <aggregation> <field> [optional: by <group_field>]`
            - averge, sum, min, max
        - Processed one chunk a time. The results of each chunk are combined and grouped/aggregated once more for the final result. 
    - Save View
        - Syntax: `save as <file_path>`
        - Results of the query can be saved as a json file to be used to join
    - Join
        - Syntax: `join with <file_path> by <field(s)>`
        - External sort-merge-join: each unique value of the first join field will be partitioned into intermdiate files (one from each data source). Each pair of intermediate files are joined to create joined, intermediate files which are appended into a final resulting output of joined records.
### B. Usage
- Install dependencies: 
    - `pip install json tabulate`
- Use interactive CLI:
    - `python src/json_cli.py`
    - Documentation / demo of all features of the CLI: [docs/json_cli.md](docs/json_cli.md)