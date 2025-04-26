import csv
import json
import itertools
# from thefuzz import fuzz
# from sentence_transformers import SentenceTransformer
import logging
import numpy as np
import ast
import traceback


def extract_code(text):
    """
    Method parses the response from the LLM inference task and returns a serialised python code block 
    """
    start = text.find("```python") + len("```python")
    end = text.find("```", start)
    code_string = text[start:end].strip() if start > len("```python") - 1 and end != -1 else None
    return ast.literal_eval(code_string) if code_string else None


def generate_plans():
    """
    Method generates all possible plan combinations according to the nodes and strategies supported
    """

    # define all possible options for the available transformation nodes
    missing_value_handlers = ['missingValues/impute', 'missingValues/drop.rows', 'missingValues/drop.columns'] 
    duplicate_values_handlers = ['duplicates']
    outlier_values_handlers = ['outliers/impute', 'outliers/drop']

    # compute all arrangements of transformation nodes 
    available_actions_permutations = itertools.permutations([ missing_value_handlers, duplicate_values_handlers, outlier_values_handlers ])
    
    # compute all possible plans
    for permutation in available_actions_permutations:
        for plan in itertools.product(*permutation):
            yield plan


def infer_cell_type(cell):
    """
    Method infers the FloeWTL type of a cell
    """
    if isinstance(cell, list) or isinstance(cell, dict): 
        return 'complex'
    try:
        float(cell)
        return 'number'
    except ValueError:
        return 'string'
    

def infer_schema(internal_representation):
    """
    Method infers the schema of the input internal representation
    """

    schema = {}
    headers = internal_representation[0]

    for column_indx, column_name in enumerate(headers):

        # store a local copy of the column 
        column = [ row[column_indx] for row in internal_representation[1:] ]
        
        # record the frequency of values and types computed for the current column
        cell_types_frequency_count = {}
        cell_values_frequency_count = {}

        for cell in column:

            # ignore empty cells or missing values
            if cell == '' or cell is None: 
                continue

            # infer cell type and add to counter
            key_type = infer_cell_type(cell)
            cell_types_frequency_count[key_type] = cell_types_frequency_count.get(key_type, 0) + 1

            # add cell value to counter
            key_value = json.dumps(cell) if isinstance(cell, list) or isinstance(cell, dict) else cell # convert the cell contents to a string, so it is hashable
            cell_values_frequency_count[key_value] = cell_values_frequency_count.get(key_value, 0) + 1

        # utilise the frequency counts to infer the type for this particular column 
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
    """
    Method calculates and returns the data wrangling issues detected on the input 
    internal representation. These include missing values, duplicated rows, and numerical outliers
    """

    headers = internal_representation[0]
    row_count = len(internal_representation) - 1

    # count the total number of missing values and the total number of cells in the IR
    missing_values_count = sum([1 for row in internal_representation[1:] for cell in row if cell == '' or cell is None])
    total_values = row_count * len(headers)

    # compute the ratio of missing values
    missing_values_percent = round((missing_values_count / total_values) * 100,3)    
    
    # compute count of unique rows (duplicates)
    seen = set()
    for row in internal_representation:
        cells = []
        for item in row:
            try: cells.append(str(item))
            except Exception: cells.append("NA")  
        hashed_row = "".join(cells)
        seen.add(hashed_row)
    
    # compute the ratio of duplicated rows
    duplicates_count = abs(row_count - len(seen))
    duplicate_rows_percent = round((duplicates_count / row_count) * 100, 3)

    # compute the total number of numerical values
    total_numerical_values_count = 0
    total_numerical_outliers_count = 0
    for column_indx, column_name in enumerate(headers):
        if schema[column_name] == 'number': 

            # compute the count of numerical outliers
            column = []
            for row in internal_representation[1:]:
                try: column.append(float(row[column_indx]))
                except (ValueError, TypeError): continue

            median = np.median(column)
            mad = np.median(np.abs(column - median))
            threshold = 3
            outlier_mask = np.abs(column - median) > (threshold * mad)
            total_numerical_outliers_count = len(np.where(outlier_mask)[0])
    
            # compute total numer of numerical values
            total_numerical_values_count += (len(internal_representation)-1)

    # compute the total number of numerical outliers
    outliers_percent = round((total_numerical_outliers_count / total_numerical_values_count) * 100, 3) if total_numerical_values_count > 0 else 0.0

    # compute the data quality of the IR
    ir_dq = round(compute_dq(internal_representation, schema), 3)

    return {
            'missing': {'missing_cells_percent': missing_values_percent},
            'outliers' : {'numerical_outliers_percent' : outliers_percent},
            'duplicates': {'duplicate_rows_percent' : duplicate_rows_percent},
            'dq': ir_dq
        }



def standardise_features(IR, mapping):
    """
    Method applies the source-target schema mapping to the internal representation
    """

    for x, y in mapping.items():

        if len(x) == 2:
            
            # multiple source columns are mapped to a single target column
            x1_column_name, x2_column_name = x
            x1_indx, x2_indx = IR[0].index(x1_column_name), IR[0].index(x2_column_name)

            IR[0][x1_indx] = y[0]

            # combine the values of the source columns into a string, used by the LLM during inference
            for i, row in enumerate(IR[1:], start=1):
                data_string = json.dumps(row[x1_indx]) + "|" + json.dumps(row[x2_indx]) 
                values = data_string.replace('"', '').split('|')
                merged_value = f"{values[0]}|{values[1]}"
                row[x1_indx] = merged_value

            for i, row in enumerate(IR):
                row.pop(x2_indx)

        else:
            
            if x == ():
                
                # the source column is created as a new column
                for t in y:
                    IR[0].append(t)

                # fill in the created column with a place holder value
                for row_indx in range(1, len(IR)):
                    IR[row_indx].append('_ext_')

            elif y == ():
                
                # the source column(s) are dropped by the mapping
                column_names_todrop_indices = [IR[0].index(s) for s in x]

                # remove the column values from the IR
                for i in range(len(IR)):
                    IR[i] = [IR[i][j] for j in range(len(IR[i])) if j not in column_names_todrop_indices]

            else:
                # the source column is simply renamed as the target column
                x, y = x[0], y[0] 
                column_indx = IR[0].index(x)
                IR[0][column_indx] = y

    return IR


def missing_value_handler(internal_representation, schema, strategy):
    """
    Method to detect and impute missing values within the IR
    """
    headers = internal_representation[0]

    for column_indx, column_name in enumerate(headers):
        column_type = schema[column_name] # get the column's type
        # impute the cell according to its column's type
        for row_indx in range(1, len(internal_representation)):
            cell = internal_representation[row_indx][column_indx]
            if cell is None or cell == '':
                if column_type == 'number': internal_representation[row_indx][column_indx] = str(0.0)
                else: internal_representation[row_indx][column_indx] = 'NA' # this covers both string and complex data types
 
    return internal_representation

def duplicate_values_handler(internal_representation):
    """
    Method to detect and handle duplicated rows within the IR    
    """

    unique_rows = []
    seen = set()
    for row in internal_representation:
        key = json.dumps(row) # serialise the row into a string
        if key not in seen:
            unique_rows.append(row)
            seen.add(key)
    return unique_rows


def outlier_handler(internal_representation, schema, strategy):
    """
    Method to detect and handle numerical outliers within the IR
    """

    processed_representation = [internal_representation[0]] + internal_representation[1:]
    threshold = 3

    if strategy not in ['impute', 'drop']:
        logging.error(f"Strategy '{strategy}' not supported for handling outlier values.")
        return internal_representation
    
    for column_indx, column_name in enumerate(processed_representation[0]):

        if schema[column_name] == 'number':
            
            column = [float(row[column_indx]) for row in processed_representation[1:]]

            median = np.median(column)
            mad = np.median(np.abs(column - median))

            outlier_mask = np.abs(column - median) > (threshold * mad)
            outlier_indices = np.where(outlier_mask)[0]

            if strategy == 'impute':

                for idx in outlier_indices:
                    processed_representation[idx + 1][column_indx] = float(median)
            
            elif strategy == 'drop':

                processed_representation = (
                    [processed_representation[0]] + 
                    [row for i, row in enumerate(processed_representation[1:]) 
                     if i not in outlier_indices]
                )

    return processed_representation


def compute_dq(internal_representation, schema):
    
    """
    Method compute_dq
    input : an internal representation and schema for the columns in the IR
    output : a data quality score between 0.0 and 1.0
    """

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
            column = []
            for row in internal_representation[1:]:
                try: column.append(float(row[column_indx]))
                except (ValueError, TypeError): continue
            median = np.median(column)
            mad = np.median(np.abs(column - median))
            threshold = 3
            outlier_mask = np.abs(column - median) > (threshold * mad)
            total_numerical_outliers_count = len(np.where(outlier_mask)[0])
    
            # compute total numer of numerical values
            total_numerical_values_count += (len(internal_representation)-1)

    missing_values_ratio = total_null_values / total_cells
    duplicate_rows_ratio = duplicate_rows / row_count
    outliers_percent = total_numerical_outliers_count / total_numerical_values_count if total_numerical_values_count > 0 else 0.0

    final_dq = 1 - abs((missing_values_ratio + duplicate_rows_ratio + outliers_percent)/3)
    return final_dq


def apply_airflow_plan(internal_representation, schema, plan):

    """
    method apply_airflow_plan
    inputs : internal_representation, plan
    outputs : internal representation after plan is applied, or None if the plan fails, target_headers is list of columns used to determine the schme amapping
    """

    try:
        current_ir = internal_representation
        # apply the plan to the (possibly changed) IR and schema

        for step in plan:
            action = step.split("/")

            if action[0] == "missingValues":
                strategy = action[1]
                current_ir = missing_value_handler(current_ir, schema, strategy)

            elif action[0] == "duplicates":
                current_ir = duplicate_values_handler(current_ir)

            elif action[0] == "outliers":
                strategy = action[1]
                current_ir = outlier_handler(current_ir, schema, strategy)

            else:
                logging.error(f"Action '{action}' not supported")
            
        return internal_representation

    except BaseException as e:
        return None
    

def apply_etl_plan(plan):

    """
    Method to apply a transformation plan to a source file
    """

    print("Starting ETL plan application...")

    # Step 1: Extract file path from the plan
    filepath = "input\\source\\" + plan[0]
    print(f"Filepath resolved: {filepath}")

    # Step 2: Extract the reconstruction key
    reconstruction_key = plan[1]['associated_key']
    print(f"Reconstruction key: {reconstruction_key}")

    # Step 3: Extract column mapping code
    mapping_code = plan[2]['standardiseFeatures']
    mapping_code = extract_code(mapping_code)
    print("Column mapping code extracted.")

    # Step 4: Read file and get internal representation (IR)
    ir = to_internal(filepath)[1]
    print(f"Internal representation loaded with {len(ir)} rows.")

    # Step 5: Apply column mapping to IR
    ir = standardise_features(ir, mapping_code)
    print("Standard feature mapping applied.")

    # Step 6: Infer schema
    schema = infer_schema(ir)
    print(f"Schema inferred: {schema}")

    # Step 7: Process intermediary steps (missing values, outliers, etc.)
    for step in plan[3:-1]:
        components = step.split("/")
        action = components[0]
        strategy = components[1] if len(components) == 2 else None
        print(f"Applying step: {action}, strategy: {strategy}")

        if action == "missingValues":
            ir = missing_value_handler(ir, schema, strategy)
            print("Missing values handled.")
            print()
            print(ir[:10])
            print()
        elif action == "duplicates":
            ir = duplicate_values_handler(ir)
            print("Duplicates removed.")
        elif action == "outliers":
            ir = outlier_handler(ir, schema, strategy)
            print("Outliers handled.")
        else:
            print(f"Unknown step: {step} — skipping.")

    # Step 8: Extract final transformation function
    def extract_function(text):
        start = text.find("```python") + len("```python")
        end = text.find("```", start)
        code_string = text[start:end].strip() if start > len("```python") - 1 and end != -1 else None
        return code_string

    llm_code = extract_function(plan[-1]["standardiseValues"])
    if not llm_code:
        print("No transformation code found in final step.")
        return (filepath, None, None)

    # Step 9: Compile and exec custom transformation function
    namespace = {}
    try:
        compiled_code = compile(llm_code, "<string>", "exec")
        exec(compiled_code, namespace)
        print(namespace.keys())
        transform_table = namespace['transform_table']
        print("transform_table = ", transform_table)
        print("Custom transformation function loaded.")
    except Exception as e:
        print("Failed to compile/load transformation function:")
        traceback.print_exc()  # This prints the full traceback
        return (filepath, None, None)

    # Step 10: Apply final transformation
    try:
        output = transform_table(ir)
        print("Final transformation applied.")
    except (BaseException, TypeError, ValueError) as e:
        print(f"Error applying final transformation: {e}")
        return (filepath, None, None)

    print("ETL plan completed successfully.")
    return (filepath, reconstruction_key, output)



def extract_list(collection, key):
    """
    Method to detect and return the first list found in a collection
    """
    if isinstance(collection, list): 
        return (key, collection)
    if isinstance(collection, dict):
        for k, value in collection.items():
            result = extract_list(value, k)
            if result: 
                return result


def to_internal(filepath):

    """
    Method to translate a file's contents into an internal representation
    """

    internal_representation = [] 

    # extract the file type
    filetype = filepath.split(".")[1]

    # when parsing json, this allows us to reconstruct the file. 
    # it essentially tells us which key the list of objects we will transform belongs to!

    if filetype == 'csv':
        with open(filepath, 'r', encoding='latin1') as file:
            internal_representation = [ row for row in csv.reader(file)]
        return (None, internal_representation)
            
    elif filetype == 'json':
        with open(filepath, 'r') as file:
            dictionary = json.load(file)

        # extract the first list of objects
        l = extract_list(dictionary, key=None) 
        associated_key, objects = l if l else (None, None)

        if not objects:
            logging.warning(f"File '{filepath}' does not contain a collection of objects.")
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
        logging.warning(f"File type '{filetype}' not supported.")
        return (None, None)
    




# ===========================================================


def compute_graphs(source, target, weight_threshold=0.5):
    pass
#     """
#     Method to compute the similarity graph between the source and target columns. NOTE that
#     this method is redundant and has been included purely to provide a detailed account
#     of the implementation steps
#     """

#     model = SentenceTransformer("all-MiniLM-L6-v2")

#     # embed the input and target headers
#     source_embeddings, target_embeddings = model.encode(source), model.encode(target)
#     source_attributes_embedding_map = { attribute : embedding for attribute, embedding in zip(source, source_embeddings) }
#     target_attributes_embedding_map = { attribute : embedding for attribute, embedding in zip(target, target_embeddings) }

#     X, Y = source, target
#     gX, gY = {}, {} # becomes the preference list for each attribute in X and Y (preference is the weight computed)

#     for v in Y: 
#         for u in X: 
#             sentence_similiarity = model.similarity(source_attributes_embedding_map[u], target_attributes_embedding_map[v])[0][0].item()
#             levenshtein_distance = fuzz.ratio(u, v)/100 
#             weight = round((levenshtein_distance + sentence_similiarity)/2, 3)

#             if weight > weight_threshold:
#                 # record the undirected edge between the nodes u and v
#                 gX[u] = gX[u] + [(v, weight)] if u in gX else [(v, weight)]
#                 gY[v] = gY[v] + [(u, weight)] if v in gY else [(u, weight)]

#     # sort each node's edge list by weight descending - required by the schema-matching algorithm
#     for node in gX: gX[node].sort(key=lambda edge : edge[1], reverse = True)
#     for node in gY: gY[node].sort(key=lambda edge : edge[1], reverse = True)
    
#     return gX, gY


def gale_shapley(source, target, diff=0.05):
    """
    Method to compute a mapping between the source and target columns. NOTE that
    this method is redundant and has been included purely to provide a detailed account
    of the implementation steps
    """

    X, Y = compute_graphs(source, target)

    unmatched_x = list(X.keys()) # all attributes from the input schemas start unmatched
    matched_attributes = {y: [] for y in Y}  
    # add special keys which map creation and deletion of attributes
    matched_attributes['CREATE'] = []
    matched_attributes['DROP'] = []
    rejected_matches = {x: [] for x in X} # record matches which get discarded during execution

    iterations_elapsed = 0

    while unmatched_x: 

        # extract first unmatched attribute from X set and all of its outgoing edges
        x = unmatched_x[0]
        x_outgoing_edges = X[x] 
        
        for y in x_outgoing_edges:
            edge_name = y[0] # y = (edge_name, edge_weight)
            if edge_name not in rejected_matches[x]:
                
                rejected_matches[x].append(edge_name) # record the proposed match as a rejection by default
                y_curr_x = len(matched_attributes[edge_name]) # get the number of attributes from X which have previously matched to this y attribute

                # the current y node belongs to no matches, so we initialise a match as (y -> x)
                if y_curr_x == 0:
                    matched_attributes[edge_name].append(x)
                    rejected_matches[x].remove(edge_name)
                    unmatched_x.remove(x)
                    break

                # the current y node is already part of a match with another x attribute (y -> x')
                elif y_curr_x == 1:
            
                    defender = matched_attributes[edge_name][0] # x' is the defender 
                    deferender_score = next(score for item, score in Y[edge_name] if item == defender) # obtain the edge weight (y -> x')
                    challenger_score = next(score for item, score in Y[y[0]] if item == x) # obtain the edge weight (y -> x)

                    if abs(challenger_score - deferender_score) / deferender_score <= diff: # the edge weight of (y -> x') is within 5% of the current edge (y -> x)
                       
                        # the two edges are unified into (y -> x and x')
                        matched_attributes[edge_name].append(x)
                        unmatched_x.remove(x)
                        rejected_matches[x].remove(edge_name)
                        break

                    else: # the weaker edge is discarded and replaced by the stronger one
                        if deferender_score > challenger_score:
                            break
                        else:
                            matched_attributes[edge_name].remove(defender)
                            matched_attributes[edge_name].append(x)
                            unmatched_x.remove(x)
                            unmatched_x.append(defender)
                            rejected_matches[defender].append(edge_name)
                            rejected_matches[x].remove(edge_name)
                            break

                else: # there is already an edge (y -> x' and z)

                    defender1, defender2 = matched_attributes[edge_name] 
                    defender1_score = next(score for item, score in Y[edge_name] if item == defender1) # obtain the edge weight (y -> x')
                    defender2_score = next(score for item, score in Y[edge_name] if item == defender2) # obtain the edge weight (y -> z)

                    challenger_score = next(score for item, score in Y[edge_name] if item == x) # obtain the edge weight (y -> x)

                    if defender1_score + defender2_score < challenger_score: # the challenger is more compatible with y than both the defenders combined
                        # the edge (y -> x' and z) is replaced by (y -> x)
                        matched_attributes[edge_name] = [x]
                        unmatched_x.remove(x)
                        unmatched_x.append(defender1)
                        unmatched_x.append(defender2)
                        rejected_matches[defender1].append(edge_name)
                        rejected_matches[defender2].append(edge_name)
                        rejected_matches[x].remove(edge_name)
                        break

                    else: # the challenger is rejected
                        break

        iterations_elapsed += 1

        iterations_threshold = 1000
        if iterations_elapsed > iterations_threshold:
            print("algorithm halted.")
            break
        
    # identify attributes from the Y set which are created
    for attribute in target:
        if attribute not in matched_attributes:
            matched_attributes['CREATE'].append(attribute)

    # swap the mapping order to be x -> y instead of y -> x
    output = {}
    for y, x in matched_attributes.items():
        if y == 'CREATE' or y == 'DROP': 
            output[y] = x
        else: 
            if x == '':
                if 'CREATE' in output: output['CREATE'] += f",{y}"
                else: output['CREATE'] = y
            else:
                output[",".join(x)] = y

    # identify attributes from the X set which are dropped
    temp = ''.join([ ''.join(entry) for entry in matched_attributes.values() ])
    for attribute in source:
        if attribute not in temp:
            output['DROP'].append(attribute)

    output['DROP'] = ",".join(list(set(output['DROP'] + unmatched_x if unmatched_x else [])))
    output['CREATE'] = ",".join(output['CREATE'])
    output.pop("", None)
    
    return output

