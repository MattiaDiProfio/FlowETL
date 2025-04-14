import csv
import json
import numpy as np

def extract_list(collection, key):
    if isinstance(collection, list): 
        return (key, collection)
    if isinstance(collection, dict):
        for k, value in collection.items():
            result = extract_list(value, k)
            if result: 
                return result

def to_internal(filepath):

    internal_representation = [] 

    # extract the file type
    filetype = filepath.split(".")[-1]
    filename = filepath.split("\\")[-1]

    if filetype == 'csv':
        with open(filepath, 'r') as file:
            internal_representation = [ row for row in csv.reader(file)]
            return (None, internal_representation)
            
    elif filetype == 'json':

        try:
            with open(filepath, 'r') as file:
                dictionary = json.load(file)
        except json.decoder.JSONDecodeError as err:
            return (None, None)

        # extract the first list of objects
        l = extract_list(dictionary, key=None) 
        associated_key, objects = l if l else (None, None)

        if not objects:
            return (None, None)

        # construct a union of all attributes across objects
        attributes_union = list(set(attribute for object in objects for attribute in object.keys()))

        # set the attributes_union as the column headers of the internal representation
        internal_representation.append(attributes_union)

        for object in objects:

            # extend each object's attribute set to match the attributes_union
            extended_object_values = []
            for attribute in attributes_union:
                extended_object_values.append(object[attribute] if attribute in object else '_ext_')
            internal_representation.append(extended_object_values)

        return (associated_key, internal_representation)
    
    else:
        return (None, None)
    
def infer_cell_type(cell):
    if isinstance(cell, list) or isinstance(cell, dict): 
        return 'complex'
    try:
        float(cell)
        return 'number'
    except ValueError:
        return 'string'
    
def infer_schema(internal_representation):

    schema = {}
    headers = internal_representation[0]

    for column_indx, column_name in enumerate(headers):

        # store a local copy of the column
        column = [ row[column_indx] for row in internal_representation[1:] ]
        
        cell_types_frequency_count = {}
        cell_values_frequency_count = {}

        for cell in column:

            if cell == '' or cell is None: # do not consider empty cells
                continue

            # infer cell type and add to counter
            key_type = infer_cell_type(cell)
            cell_types_frequency_count[key_type] = cell_types_frequency_count.get(key_type, 0) + 1

            # add cell value to counter
            key_value = json.dumps(cell) if isinstance(cell, list) or isinstance(cell, dict) else cell # convert the cell contents to a string, so it is hashable
            cell_values_frequency_count[key_value] = cell_values_frequency_count.get(key_value, 0) + 1

        # utilise the frequency counts to infer the type for this particular column - refer to design doc for details
        inferred_type = 'string'
        if len(cell_values_frequency_count.keys()) == 2:
            inferred_type = 'boolean'
        else:
            recorded_types = list(cell_types_frequency_count.keys())
            if len(recorded_types) > 1:
                inferred_type = 'ambiguous'
            if len(recorded_types) == 1:
                inferred_type = recorded_types[0]

        schema[column_name] = inferred_type

    return schema

def compute_etl_metrics(internal_representation, schema):
    headers = internal_representation[0]
    row_count = len(internal_representation) - 1
    seen = set()
    for row in internal_representation:
        hashed_row = "".join([str(cell) for cell in row if cell is not None and cell != ''])
        seen.add(hashed_row)
    
    # compute the total number of numerical values
    total_numerical_values_count = 0
    total_numerical_outliers_count = 0
    for column_indx, column_name in enumerate(headers):
        if schema[column_name] == 'number': 

            # compute the count of numerical outliers
            column = [float(row[column_indx]) for row in internal_representation[1:] if row[column_indx] is not None and row[column_indx] != '']
            median = np.median(column)
            mad = np.median(np.abs(column - median))
            threshold = 3
            outlier_mask = np.abs(column - median) > (threshold * mad)
            total_numerical_outliers_count = len(np.where(outlier_mask)[0])
    
            # compute total numer of numerical values
            total_numerical_values_count += (len(internal_representation)-1)

    return { 
        "m" : round((sum([1 for row in internal_representation[1:] for cell in row if cell == '' or cell is None]) / (row_count * len(headers))), 3),
        "d" : round(abs((row_count - len(seen))) / row_count, 3),
        "o" : round( total_numerical_outliers_count / total_numerical_values_count ,3) if total_numerical_values_count > 0 else 0
    }

def compute_dq(internal_representation, schema):
    
    headers = internal_representation[0]
    row_count = (len(internal_representation)-1)
    total_cells = row_count * len(headers)

    # compute the ratio of missing values values 
    total_null_values = 0
    for row in internal_representation:
        for cell in row:
            if cell == "" or cell is None: total_null_values += 1

    # compute ratio of duplicate rows
    duplicate_rows = row_count - len(set(json.dumps(row) for row in internal_representation))

    # compute ratio of outliers across all cells
    total_numerical_outliers_count = 0
    total_numerical_values_count = 0
    for column_indx, column_name in enumerate(headers):
        column_type = schema[column_name]
        if column_type == 'number':
            # compute the count of numerical outliers
            column = [float(row[column_indx]) for row in internal_representation[1:] if row[column_indx] != '' and row[column_indx] is not None]
            median = np.median(column)
            mad = np.median(np.abs(column - median))
            threshold = 3
            outlier_mask = np.abs(column - median) > (threshold * mad)
            total_numerical_outliers_count = len(np.where(outlier_mask)[0])
    
            # compute total numer of numerical values
            total_numerical_values_count += (len(internal_representation)-1)

    missing_values_ratio = total_null_values / total_cells
    duplicate_rows_ratio = duplicate_rows / row_count
    outliers_percent = 0 if total_numerical_values_count == 0 else total_numerical_outliers_count / total_numerical_values_count

    final_dq = 1 - abs((missing_values_ratio + duplicate_rows_ratio + outliers_percent)/3)
    return final_dq
