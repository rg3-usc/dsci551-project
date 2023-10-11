import pandas as pd
import os

def insert_new_data(file_path, data_values, column_names):
    """
    Insert new data into a CSV file.

    Args:
        file_path (str): Path to the CSV file.
        data_values (list): List of values to be added as a new row.
        column_names (list): List of column names corresponding to data_values.
        
    Returns:
        pd.DataFrame: Updated DataFrame containing all the data.
    """
    # Create full path to the user directory
    full_path = os.path.expanduser(file_path)

    # Check if the file already exists
    if os.path.exists(full_path):
        # If the file exists, open the CSV to add new data
        data = pd.read_csv(full_path)
    else:
        # If the file does not exist, create a new DataFrame
        data = pd.DataFrame(columns=column_names)

    # Check if the number of column names matches the number of values
    if len(column_names) != len(data_values):
        raise ValueError("Number of column names must match the number of data values.")

    # Add new data to the DataFrame
    new_data = [data_values]  # Wrapping data_values in a list to create a single-row DataFrame
    new_row = pd.DataFrame(new_data, columns=column_names)
    data = pd.concat([data, new_row], ignore_index=True)

    # Add column to count each name added
    # Extract the name of the column to count occurrences from column_names list
    count_column_name = column_names[0]
    data['Count'] = data.groupby(count_column_name)[count_column_name].transform('count')

    # Save to CSV
    data.to_csv(full_path, index=True)

    return data

def delete_data(file_path, data_values, column_names):
    """
    Delete data from existing CSV file.

    Args:
        file_path (str): Path to the CSV file.
        data_values (list): List of values to be deleted.
        column_names (list): List of column names corresponding to data_values.
        
    Returns:
        pd.DataFrame: Updated DataFrame containing all the data.
    """
    
    # Create full path to the user directory
    full_path = os.path.expanduser(file_path)

    # Check if the file already exists
    if os.path.exists(full_path):
        # If the file exists, open the CSV and work with existing database
        data = pd.read_csv(full_path)

        # Initialize list to store individual conditions
        criteria_list = []

        # Create conditions for each pair of data_values and column_names
        for i in range(len(data_values)):
            criteria = (data[column_names[i]] == data_values[i])
            criteria_list.append(criteria)

        # Combine conditions using logical AND
        all_criteria = criteria_list[0]
        for criteria in criteria_list[1:]:
            all_criteria &= criteria

        # Remove the record from the existing dataframe
        data = data[~all_criteria]

        # Recount Count
        count_column_name = column_names[0]
        data['Count'] = data.groupby(count_column_name)[count_column_name].transform('count')

        # Save to CSV
        data.to_csv(full_path, index=True)
        
    else:
        # If the file does not exist, create a DataFrame to add new baby names
        return "Data file does not exist"

    return data

def edit_data(file_path, data_values, column_names, new_data_values):
    """
    Edit data from existing CSV file.

    Args:
        file_path (str): Path to the CSV file.
        data_values (list): List of values to identify the row to edit.
        column_names (list): List of column names corresponding to data_values.
        new_data_values (dict): Dictionary containing column names and new values.
        
    Returns:
        pd.DataFrame: Updated DataFrame containing all the data.
    """
    
    # Create full path to the user directory
    full_path = os.path.expanduser(file_path)

    # Check if the file already exists
    if os.path.exists(full_path):
        # If the file exists, open the CSV and work with existing database
        data = pd.read_csv(full_path)

        # Initialize list to store individual conditions
        criteria_list = []

        # Create conditions for each pair of data_values and column_names
        for i in range(len(data_values)):
            criteria = (data[column_names[i]] == data_values[i])
            criteria_list.append(criteria)

        # Combine conditions using logical AND
        all_criteria = criteria_list[0]
        for criteria in criteria_list[1:]:
            all_criteria &= criteria

        # Find the index for all_criteria and edit values as indicated by new_data_values
        row_index = data.index[all_criteria].tolist()

        # Check if only one row is found
        if len(row_index) == 1:
            # Update the row with new data values
            for column, value in new_data_values.items():
                data.at[row_index[0], column] = value
        else:
            print("Error: Multiple or no matching rows found for the given criteria.")

        # Save to CSV
        data.to_csv(full_path, index=True)

    else:
        # If the file does not exist, create a DataFrame to add new baby names
        return "Data file does not exist"

    return data
