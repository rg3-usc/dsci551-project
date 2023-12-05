import json
import os
import re
import ast
from tabulate import tabulate

class KeyValueStore:
    PRIMARY_KEY_LOCATION = "_primary_key"
    CHUNK_SIZE = 5000

    def __init__(self, file_path):
        self.file_path = file_path
        self.primary_keys = set()  # Set to store primary keys
        if not os.path.exists(file_path):
            with open(file_path, 'w'):  # Create the file if it doesn't exist
                pass
        # Load the primary key from the database file
        self.load_primary_key()

    def load_primary_key(self):
        with open(self.file_path, 'r') as file:
            data = file.readline()
            if data:
                try:
                    first_line = json.loads(data)
                    stored_primary_key = first_line.get(self.PRIMARY_KEY_LOCATION)
                    self.primary_key = stored_primary_key if stored_primary_key else None
                except json.JSONDecodeError:
                    # If the file is not a valid JSON, treat it as a new file
                    self.primary_key = None
            else:
                self.primary_key = None
        # If primary key is not set, prompt the user and save it as the first line
        if not self.primary_key:
            self.primary_key = input("Enter the primary key for the dataset: ")
            self.save_primary_key()
        # Populate primary_keys set with existing keys
        self.populate_primary_keys()

    def populate_primary_keys(self):
        with open(self.file_path, 'r') as file:
            for line in file:
                if line.strip():
                    record = json.loads(line)
                    self.primary_keys.add(record.get(self.primary_key))

    def save_primary_key(self):
        with open(self.file_path, 'w') as file:
            primary_key_data = {self.PRIMARY_KEY_LOCATION: self.primary_key}
            file.write(json.dumps(primary_key_data) + '\n')

    def insert(self, data, force=False):
        key = data.get(self.primary_key)
        # Check if the key is None or missing
        if key is None:
            print("\nCannot insert data with a missing primary key.")
            return False
        if not force and key in self.primary_keys:
            # If the key exists and force is not set, prompt for confirmation to delete the old record
            confirmation = input(f"A record with the key '{key}' already exists. Do you want to replace it? (y/n): ").lower()
            if confirmation != 'y':
                print("\nInsert operation cancelled.")
                return False  # Insertion was not successful
            # Delete the old record
            self.delete(key)
        self.write_data(data)
        self.primary_keys.add(key)
        return True  # Insertion was successful

    def write_data(self, data):
        with open(self.file_path, 'a') as file:
            data_str = json.dumps(data) + '\n'
            file.write(data_str)

    def delete(self, key):
        success_flag = False
        temp_file_path = self.file_path + '_temp'
        with open(self.file_path, 'r') as input_file, open(temp_file_path, 'w') as temp_file:
            chunk = []
            for line in input_file:
                if line.strip():
                    record = json.loads(line)
                    if record.get(self.primary_key) == key:
                        # Skip this record (delete it)
                        success_flag = True
                        continue
                    chunk.append(record)
                    if len(chunk) >= self.CHUNK_SIZE:
                        # Write the chunk to the temporary file
                        temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
                        chunk = []
            # Write any remaining records in the last chunk to the temporary file
            if chunk: temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
        # Replace the original file with the temporary file
        os.replace(temp_file_path, self.file_path)
        if success_flag:
            # Update primary_keys set by removing the deleted key
            self.primary_keys.discard(key)
        return success_flag

    def read_data_chunked(self, read_primary_keys=False):
        if read_primary_keys:
            return list(self.primary_keys)
        with open(self.file_path, 'r') as file:
            chunk = []
            for line in file:
                if line.strip():
                    record = json.loads(line)
                    chunk.append(record)
                    if len(chunk) >= self.CHUNK_SIZE:
                        yield chunk
                        chunk = []
            if chunk:
                yield chunk  # Yield any remaining records in the last chunk

    def update(self, key, new_values):
        # Check if the key exists
        if key not in self.primary_keys:
            print(f"No record found with key '{key}'. Update operation cancelled.")
            return False
        temp_file_path = self.file_path + '_temp'
        record_updated = False
        with open(self.file_path, 'r') as input_file, open(temp_file_path, 'w') as temp_file:
            chunk = []
            for line in input_file:
                if line.strip():
                    record = json.loads(line)
                    if record.get(self.primary_key) == key:
                        # Update the record
                        record.update(new_values)
                        record_updated = True
                    chunk.append(record)
                    if len(chunk) >= self.CHUNK_SIZE:
                        # Write the chunk to the temporary file
                        temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
                        chunk = []
            # Write any remaining records in the last chunk to the temporary file
            temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
        # Replace the original file with the temporary file
        os.replace(temp_file_path, self.file_path)
        if record_updated:
            print(f"'{key}' updated successfully.")
        else:
            print(f"No record found with key '{key}'. Update operation cancelled.")
        return record_updated

    def batch_insert_from_file(self, json_file_path):
        try:
            existing_keys_detected = set()  # Set to track existing keys detected
            success_flag = False  # Flag to track if any insertion was successful
            confirmation_asked = False
            confirmation = 'y'
            with open(json_file_path, 'r') as file:
                for line in file:
                    try:
                        data = json.loads(line)
                        key = data.get(self.primary_key)
                        # Skip records that are missing the primary key field
                        if key is None:
                            print("Skipping record with a missing primary key.")
                            continue
                        if key in self.primary_keys and key not in existing_keys_detected:
                            # If the key already exists and this is the first encounter, prompt the user
                            if not confirmation_asked:
                                confirmation = input("\nAt least one record whose primary key already exists in the database. "
                                                    "Batch insert will replace the values of keys that already exist. "
                                                    "Would you still like to proceed? (y/n): ").lower()
                            confirmation_asked = True
                            if confirmation != 'y':
                                print(f"Skipping key '{key}' since it already exists.")
                                existing_keys_detected.add(key)
                            else:
                                self.delete(key)
                                print(f"Replacing values for key '{key}'.")
                        # Insert the key (whether it's a duplicate or a new key)
                        if key not in self.primary_keys or confirmation == 'y':
                            if self.insert(data, force=True):  # Use force=True to avoid confirmation prompt during batch insert
                                success_flag = True
                    except json.JSONDecodeError:
                        print("Invalid JSON format in file. Skipping line.")
            return success_flag
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return False

    def show_operation(self, fields, input_file, output_file):
        with open(input_file, 'r') as input_file, open(output_file, 'w') as temp_file:
            first_line = True
            chunk = []
            for line in input_file:
                if first_line and self.PRIMARY_KEY_LOCATION in line:
                    # Skip the first line if it contains the _primary_key row
                    first_line = False
                    continue
                if line.strip():
                    record = json.loads(line)
                    projected_record = {field: record.get(field) for field in fields}
                    chunk.append(projected_record)
                    if len(chunk) >= self.CHUNK_SIZE:
                        # Write the chunk to the temporary file
                        temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
                        chunk = []
            # Write any remaining records in the last chunk to the temporary file
            if chunk:
                temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')

    def parse_row_numbers(self, row_numbers):
        row_numbers_list = []
        for part in row_numbers.split(','):
            if ':' in part:
                start, end = map(int, part.split(':'))
                row_numbers_list.extend(range(start, end + 1))
            else:
                row_numbers_list.append(int(part))
        return row_numbers_list
    def filter_operation(self, condition, input_file, output_file):
        with open(input_file, 'r') as input_file, open(output_file, 'w') as temp_file:
            first_line = True
            chunk = []
            if condition.startswith("rows"):
                match = re.match(r'rows\s*\[([0-9:,]+)\]', condition)
                if not match:
                    raise ValueError(f'Invalid rows condition: {condition}')
                row_numbers = match.group(1)
                row_numbers_list = self.parse_row_numbers(row_numbers)
                # Filter rows based on line numbers
                line_number = 0
                for line in input_file:
                    line_number += 1
                    if first_line and self.PRIMARY_KEY_LOCATION in line:
                        # Skip the first line if it contains the _primary_key row
                        first_line = False
                        continue
                    if line_number in row_numbers_list:
                        chunk.append(line)
                    # Write the chunk to the temporary file after processing each line
                    temp_file.write(''.join(chunk))
                    chunk = []
                    
            else:
                match = re.match(r'(\w+)\s*([=><!]+)\s*(\'[^\']*\'|"[^"]*"|\w+)', condition)
                if not match:
                    match = re.match(r'(\w+)\s+contains\s+(.+)', condition)
                    if not match:
                        raise ValueError(f'Invalid condition: {condition}')
                    field, value = match.groups()
                    # Parse the provided value as a literal Python expression
                    try:
                        value = ast.literal_eval(value)
                        if not isinstance(value, (list, tuple)):
                            # If literal_eval doesn't raise an exception and the value is not a list or tuple,
                            # treat it as a single-item list to support both list and string values
                            value = [value]
                    except (SyntaxError, ValueError):
                        # If literal_eval fails, treat the value as a string
                        value = [value]
                    # Convert both the field value and the search values to lowercase for case-insensitive matching
                    condition = f"item.get('{field}').lower() in {list(map(lambda x: x.lower(), value))}"
                else:
                    field, op, value = match.groups()
                    if op == '=': op = '=='
                    condition = f"item.get('{field}') {op} {value}"
                # return filtered_data
                for line in input_file:
                    if first_line and self.PRIMARY_KEY_LOCATION in line:
                        # Skip the first line if it contains the _primary_key row
                        first_line = False
                        continue
                    if line.strip():
                        item = json.loads(line)
                        if eval(condition, {"item": item}):
                            chunk.append(item)
                        if len(chunk) >= self.CHUNK_SIZE:
                            # Write the chunk to the temporary file
                            temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')
                            chunk = []
                # Write any remaining records in the last chunk to the temporary file
                if chunk:
                    temp_file.write('\n'.join(map(json.dumps, chunk)) + '\n')

    def sort_operation(self, fields, input_file, output_file):
        # Create a list to store the paths of temporary chunk files
        chunk_files = []
        # Create a dictionary to store the open file handles for each unique value
        unique_value_files = {}
        with open(input_file, 'r') as input_file:
            while True:
                # Read lines until the chunk size or until the end of the file
                chunk = []
                for _ in range(self.CHUNK_SIZE):
                    line = input_file.readline()
                    if line:
                        chunk.append(json.loads(line))
                if not chunk:
                    break
                # Sort the chunk based on the specified fields
                is_descending = fields[0].startswith('-')
                sort_field = fields[0][1:] if is_descending else fields[0]
                sorted_chunk = sorted(chunk, key=lambda x: x.get(sort_field, ''), reverse=is_descending)
                # Write the sorted chunk to intermediate files based on the unique values of the first sort field
                for record in sorted_chunk:
                    first_sort_field_value = record.get(sort_field, '')
                    file_handle = unique_value_files.get(first_sort_field_value)
                    # If the file for the unique value is not open, create and open it
                    if file_handle is None:
                        intermediate_file_path = f"{output_file}_intermediate_{first_sort_field_value}.json"
                        file_handle = open(intermediate_file_path, 'a')  # Open for append
                        unique_value_files[first_sort_field_value] = file_handle
                        # Append the intermediate file path to chunk_files
                        chunk_files.append(intermediate_file_path)
                    # Write the record to the file
                    file_handle.write(json.dumps(record) + '\n')
        # Sort chunk_files based on the unique values of the first sort field
        chunk_files.sort(key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else x)
        if fields[0].startswith('-'):
            chunk_files.reverse()
        # Close all the intermediate files
        for file_handle in unique_value_files.values():
            file_handle.close()
        # Merge the sorted chunks into the final output
        self.merge_sorted_chunks(chunk_files, output_file, fields)
    def merge_sorted_chunks(self, chunk_files, output_file, fields):
        # Iterate through each intermediate file
        for file_path in chunk_files:
            # Sort the intermediate file by additional sort fields
            sorted_data = []
            with open(file_path, 'r') as opened_file:
                for line in opened_file:
                    record = json.loads(line)
                    sorted_data.append(record)
            # Perform the final sort on the sorted_data
            for field in reversed(fields[1:]):
                if field.startswith('-'):
                    field = field[1:]  # Remove the '-' for sorting
                    sorted_data = sorted(sorted_data, key=lambda item: item.get(field, ''), reverse=True)
                else:
                    sorted_data = sorted(sorted_data, key=lambda item: item.get(field, ''))
            # Append the sorted_data to the output file
            with open(output_file, 'a') as result_file:
                for record in sorted_data:
                    result_file.write(json.dumps(record) + '\n')
            # Clean up intermediate file (optional)
            os.remove(file_path)

    def count_operation(self, input_file, output_file, group_by=None):
        # Create a list to store the paths of temporary chunk files
        chunk_files = []
        # Read the input_file in chunks
        with open(input_file, 'r') as input_file:
            while True:
                # Read lines until the chunk size or until the end of the file
                chunk = []
                for _ in range(self.CHUNK_SIZE):
                    line = input_file.readline()
                    if line:
                        chunk.append(json.loads(line))
                if not chunk:
                    break
                # Perform count operation on the chunk
                if group_by:
                    def group(data, field):
                        grouped_data = {}
                        for item in data:
                            key = item[field]
                            grouped_data.setdefault(key, []).append(item)
                        return grouped_data
                    grouped_data = group(chunk, group_by)
                    count_results = []
                    for key, group in grouped_data.items():
                        count_result = len(group)
                        count_results.append({group_by: key, "count": count_result})
                    # Write the count results to a temporary file
                    chunk_file_path = f"{output_file}_chunk_{len(chunk_files)}.json"
                    with open(chunk_file_path, 'w') as chunk_file:
                        for record in count_results:
                            chunk_file.write(json.dumps(record) + '\n')
                    chunk_files.append(chunk_file_path)
                else:
                    count_result = len(chunk)
                    # Write the count result to a temporary file
                    chunk_file_path = f"{output_file}_chunk_{len(chunk_files)}.json"
                    with open(chunk_file_path, 'w') as chunk_file:
                        chunk_file.write(json.dumps({"count": count_result}) + '\n')
                    chunk_files.append(chunk_file_path)
        # Merge the count results from all chunks and do the final count calculation
        final_counts = {}  # Use a dictionary to accumulate counts based on the group_by key
        for chunk_file in chunk_files:
            with open(chunk_file, 'r') as chunk_file:
                for line in chunk_file:
                    record = json.loads(line)
                    key = record.get(group_by, 'total') if group_by else 'total'
                    final_counts[key] = final_counts.get(key, 0) + record["count"]
        # Write the final counts to the output file
        with open(output_file, 'w') as result_file:
            for key, count in final_counts.items():
                if group_by:
                    result_file.write(json.dumps({group_by: key, "count": count}) + '\n')
                else: 
                    result_file.write(json.dumps({"total_count": count}) + '\n')
        # Clean up temporary chunk files
        for chunk_file in chunk_files:
            os.remove(chunk_file)
    def aggregate_operation(self, input_file, output_file, field, aggregation, group_by=None):
        # Create a list to store the paths of temporary chunk files
        chunk_files = []
        # Read the input_file in chunks
        with open(input_file, 'r') as input_file:
            while True:
                # Read lines until the chunk size or until the end of the file
                chunk = []
                for _ in range(self.CHUNK_SIZE):
                    line = input_file.readline()
                    if line:
                        chunk.append(json.loads(line))
                if not chunk:
                    break
                # Perform aggregation operation on the chunk
                agg_result = None  # Initialize agg_result
                agg_field = aggregation + '_' + field
                if group_by:
                    def group(data, field):
                        grouped_data = {}
                        for item in data:
                            key = item[field]
                            grouped_data.setdefault(key, []).append(item)
                        return grouped_data
                    grouped_data = group(chunk, group_by)
                    agg_results = []
                    for key, group in grouped_data.items():
                        if aggregation == "sum":
                            agg_result = sum(item.get(field, 0) for item in group)
                        elif aggregation == "min":
                            agg_result = min((item.get(field, None) for item in group if field in item), default=None)
                        elif aggregation == "max":
                            agg_result = max((item.get(field, None) for item in group if field in item), default=None)
                        elif aggregation == "average":
                            valid_items = [item.get(field) for item in group if field in item]
                            agg_result = sum(valid_items) if valid_items else None # sum up until final step
                        agg_results.append({group_by: key, agg_field: agg_result, "count": len(group)})
                    # Write the aggregation results to a temporary file
                    chunk_file_path = f"{output_file}_chunk_{len(chunk_files)}.json"
                    with open(chunk_file_path, 'w') as chunk_file:
                        for record in agg_results:
                            chunk_file.write(json.dumps(record) + '\n')
                    chunk_files.append(chunk_file_path)
                else:
                    if aggregation == "sum":
                        agg_result = sum(item.get(field, 0) for item in chunk)
                    elif aggregation == "min":
                        agg_result = min((item.get(field, None) for item in chunk if field in item), default=None)
                    elif aggregation == "max":
                        agg_result = max((item.get(field, None) for item in chunk if field in item), default=None)
                    elif aggregation == "average":
                        valid_items = [item.get(field) for item in chunk if field in item]
                        agg_result = sum(valid_items) if valid_items else None # sum up until final step
                    # Write the aggregation result to a temporary file
                    chunk_file_path = f"{output_file}_chunk_{len(chunk_files)}.json"
                    with open(chunk_file_path, 'w') as chunk_file:
                        chunk_file.write(json.dumps({agg_field: agg_result, "count": len(chunk)}) + '\n')
                    chunk_files.append(chunk_file_path)
        # Merge the aggregation results from all chunks and do the final aggregation calculation
        final_aggregations = {}
        for chunk_file in chunk_files:
            with open(chunk_file, 'r') as chunk_file:
                for line in chunk_file:
                    record = json.loads(line)
                    key = record.get(group_by, 'total') if group_by else 'total'
                    final_aggregations[key] = final_aggregations.get(key, {"count": 0})
                    final_aggregations[key]["count"] += record["count"]
                    # Only consider the aggregation field
                    final_aggregations[key].setdefault(agg_field, []).append(record[agg_field])
        # Write the final aggregations to the output file
        with open(output_file, 'w') as result_file:
            for key, aggregations in final_aggregations.items():
                result = {group_by: key} if group_by else {}
                # Include the "count" field in the result
                if aggregation == "sum":
                    result[agg_field] = sum(aggregations[agg_field])
                elif aggregation == "average":
                    total_sum = sum(aggregations[agg_field])
                    total_count = aggregations["count"]
                    result[agg_field] = total_sum / total_count if total_count != 0 else 0  # Avoid division by zero
                elif aggregation == "min":
                    result[agg_field] = min(aggregations[agg_field])
                elif aggregation == "max":
                    result[agg_field] = max(aggregations[agg_field])
                result_file.write(json.dumps(result) + '\n')
        # Clean up temporary chunk files
        for chunk_file in chunk_files:
            os.remove(chunk_file)
    def save_result_as(self, input_file, file_path):
        try:
            with open(file_path, 'a') as file:
                # Write each record as a separate line
                with open(input_file, 'r') as result_file:
                    for i, line in enumerate(result_file):
                        record = json.loads(line)
                        file.write(json.dumps(record, separators=(',', ':')) + '\n')
            print(f"Result saved successfully at: {file_path}")
        except Exception as e:
            print(f"Error saving result: {e}")

    def join_datasets(self, input_file_path, output_file, other_file_path, specified_fields):
        # Create dictionaries to store intermediate file paths
        input_intermediate_files = {}
        other_intermediate_files = {}
        # Process the input file and create intermediate files
        with open(input_file_path, 'r') as input_file:
            for line in input_file:
                item = json.loads(line)
                key_value = tuple(item[field] for field in specified_fields)
                intermediate_file_path = f"{output_file}_input_intermediate_{key_value}.json"
                input_intermediate_files.setdefault(key_value, []).append(item)
                with open(intermediate_file_path, 'a') as intermediate_file:
                    intermediate_file.write(json.dumps(item) + '\n')
        # Process the external file and create intermediate files
        with open(other_file_path, 'r') as other_file:
            for line in other_file:
                item = json.loads(line)
                key_value = tuple(item[field] for field in specified_fields)
                intermediate_file_path = f"{output_file}_other_intermediate_{key_value}.json"
                other_intermediate_files.setdefault(key_value, []).append(item)
                with open(intermediate_file_path, 'a') as intermediate_file:
                    intermediate_file.write(json.dumps(item) + '\n')
        # Iterate through unique keys from both files and perform the join
        for common_key in set(input_intermediate_files.keys()) | set(other_intermediate_files.keys()):
            input_records = input_intermediate_files.get(common_key, [])
            other_records = other_intermediate_files.get(common_key, [])
            # Create a joined file for the common key
            joined_file_path = f"{output_file}_joined_intermediate_{common_key}.json"
            # Write joined records directly to the joined file
            with open(joined_file_path, 'w') as joined_file:
                for input_record in input_records:
                    for other_record in other_records:
                        joined_record = {**input_record, **other_record}
                        joined_file.write(json.dumps(joined_record) + '\n')
        # Merge intermediate files for joined records into the final output
        self.merge_joined_chunks(output_file, [f"{output_file}_joined_intermediate_{key}.json" for key in set(input_intermediate_files.keys()) | set(other_intermediate_files.keys())])
    def merge_joined_chunks(self, output_file, intermediate_files):
        # Concatenate intermediate files for joined records into the final output
        with open(output_file, 'w') as results_file:
            for intermediate_file_path in intermediate_files:
                with open(intermediate_file_path, 'r') as intermediate_file:
                    for line in intermediate_file:
                        results_file.write(line)
                # Clean up intermediate files for joined records (optional)
                os.remove(intermediate_file_path)

    def execute_query(self, query):
        temp_file_path = self.file_path + '_temp'
        input_file = temp_file_path + '_input'
        output_file = temp_file_path + '_output'
        error_messages = []
        # Create a duplicate of the database file as a temporary file
        with open(self.file_path, 'r') as db_file, open(input_file, 'w') as temp_input_file:
            first_line = True
            for line in db_file:
                if first_line and self.PRIMARY_KEY_LOCATION in line:
                    # Skip the first line if it contains the _primary_key row
                    first_line = False
                    continue
                temp_input_file.write(line)
        queries = query.strip().split('|')
        for query in queries:
            # Create an empty output file
            open(output_file, 'w').close()
            parts = query.strip().split()
            operation = parts[0]
            if operation == 'show':
                try:
                    fields = parts[1:]
                    self.show_operation(fields, input_file, output_file)
                    # Replace the input file with the output file for the next operation
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            elif operation == 'filter':
                try:
                    condition = ' '.join(parts[1:])
                    self.filter_operation(condition, input_file, output_file)
                    # Replace the input file with the output file for the next operation
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            elif operation == 'order':
                try:
                    fields = parts[1:]
                    self.sort_operation(fields, input_file, output_file)
                    # Replace the input file with the output file for the next operation
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            elif operation == 'find':
                try:
                    aggregation = parts[1]
                    if aggregation == 'count':
                        group_by = parts[3] if len(parts) > 2 and parts[2] == 'by' else None
                        self.count_operation(input_file, output_file, group_by)
                    else:  # handle find with aggregation other than count
                        field = parts[2]
                        group_by = parts[4] if len(parts) > 4 and parts[3] == 'by' else None
                        self.aggregate_operation(input_file, output_file, field, aggregation, group_by)
                    # Replace the input file with the output file for the next operation
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            elif operation == 'save' and parts[1] == 'as':
                try:
                    file_path = ' '.join(parts[2:])
                    self.save_result_as(input_file, file_path)
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            elif operation == 'join':
                try: 
                    other_file_index = parts.index('with') + 1
                    other_file_path = parts[other_file_index]
                    specified_fields = parts[4].split(',') if len(parts) > 4 and parts[3] == 'by' else None
                    if specified_fields:
                        self.join_datasets(input_file, output_file, other_file_path, specified_fields)
                    else:
                        print("\nError: The specified fields are not present in both datasets.")
                    if os.path.exists(output_file):
                        os.replace(output_file, input_file)
                except Exception as e:
                    error_messages.append(f"\nError during '{operation}' operation: {e}")
                    break
            else:
                error_messages.append(f"\nError: Invalid operation - {operation}")
                break
        if not error_messages:
            # Print the final result or do further processing
            with open(input_file, 'r') as result_file:
                for line in result_file:
                    if line.strip():
                        print(line.strip())
        for error_message in error_messages:
            print(error_message)
        # Cleanup temporary files
        os.remove(input_file)
        for file_name in os.listdir(os.path.dirname(temp_file_path)):
            if file_name.startswith(os.path.basename(temp_file_path)):
                file_path = os.path.join(os.path.dirname(temp_file_path), file_name)
                os.remove(file_path)

    def close(self):
        pass

def main():
    file_path = input("Enter the path to the database file: ")
    database = KeyValueStore(file_path)
    # Check if the primary key is already set
    if not database.primary_key:
        primary_key = input("Enter the primary key for the dataset: ")
        database.primary_key = primary_key
        database.save_primary_key()
    else:
        print(f"Using primary key: {database.primary_key}")

    while True:
        print("\nAvailable commands:")
        print("1. Insert data (1 record in JSON format)")
        print("2. Insert data (Batch insert of JSON file)")
        print("3. Update data (JSON format)")
        print("4. Delete data")
        print("5. Display data")
        print("6. Query data")
        print("7. Exit")
        choice = input("\nEnter your choice: ")

        if choice == '1':
            json_data = input("Enter the JSON data to insert: ")
            try:
                data_dict = json.loads(json_data)
            except json.JSONDecodeError:
                print("\nInvalid JSON format. Please try again.")
                continue
            if database.insert(data_dict):
                print("\nData inserted successfully.")

        elif choice == '2':
            json_file_path = input("Enter the path to the JSON file: ")
            if database.batch_insert_from_file(json_file_path):
                print("\nData inserted successfully.")

        elif choice == '3':
            key = input("Enter the key to update: ")
            json_data = input("Enter the new JSON value: ")
            try:
                new_value_dict = json.loads(json_data)
            except json.JSONDecodeError:
                print("\nInvalid JSON format. Please try again.")
                continue
            database.update(key, new_value_dict)

        elif choice == '4':
            key = input("Enter the key to delete: ")
            if database.delete(key):
                print("\nData deleted successfully.")
            else:
                print("\nNo data found for the given key.")

        elif choice == '5':
            print("\nDatabase contents:")
            for data_chunk in database.read_data_chunked():
                for item in data_chunk:
                    print(item)
        
        elif choice == '6':
            while True:
                query = input("Enter your custom query (enter '?help' for syntax and examples): ")
                if query == '?help': 
                    help_table = [
                        ["Show", "show <field(s)>", "show name stars",""],
                        ["Filter (comparison)", "filter <field> <comparison condition>", "filter stars > 4","< > <= >= = !="],
                        ["Filter (substring matches)", "filter <field> contains <string or list>", "filter name contains 'Target'\nfilter state contains ['CA','AZ']"],
                        ["Filter (rows)", "filter rows <[range and/or list]>", "filter rows [1:100, 200]"],
                        ["Order", "order <field(s)>", "order -stars name", "asc by default; -<field> for desc"],
                        ["Find (Count)", "find count [optional: by <group_field>]", "find count by state"],
                        ["Find (Aggregation)", "find <aggregation> <field> [optional: by <group_field>]", "find average stars by state","averge, sum, min, max"],
                        ["Save Result", "save as <file_path>", "save as output.json"],
                        ["Join", "join with <file_path> by <field(s)>", "join with reviews.json by business_id"]
                    ]
                    print(tabulate(help_table, headers=["Query Operation", "Syntax", "Example","Notes"], tablefmt="fancy_grid"))
                    print("\nNOTE: Multiple operations can be performed sequentially by separating with | ")
                    print("\te.g.\t filter stars > 4.5 | filter state contains ['CA','NY'] | show name stars review_count | order review_count\n")
                else:
                    database.execute_query(query)
                    break

        elif choice == '7':
            print("\nExiting the program.")
            database.close()
            break
        else:
            print("\nInvalid choice. Please try again.")

if __name__ == '__main__':
    main()
