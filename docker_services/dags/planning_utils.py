import csv
import json
import itertools
from thefuzz import fuzz
from sentence_transformers import SentenceTransformer
import logging
import numpy as np

def generate_plans():
    
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

    missing_values_count = sum([1 for row in internal_representation[1:] for cell in row if cell == '' or cell is None])
    total_values = row_count * len(headers)

    # compute the ratio of missing values
    missing_values_percent = round((missing_values_count / total_values) * 100,3)    
    
    # compute count of unique rows (duplicates)
    seen = set()
    for row in internal_representation:
        hashed_row = "".join(row)
        seen.add(hashed_row)
    
    duplicates_count = abs(row_count - len(seen))
    duplicate_rows_percent = round((duplicates_count / row_count) * 100, 3)

    # compute the total number of numerical values
    total_numerical_values_count = 0
    total_numerical_outliers_count = 0
    for column_indx, column_name in enumerate(headers):
        if schema[column_name] == 'number': 

            # compute the count of numerical outliers
            column = [float(row[column_indx]) for row in internal_representation[1:]]
            median = np.median(column)
            mad = np.median(np.abs(column - median))
            threshold = 3
            outlier_mask = np.abs(column - median) > (threshold * mad)
            total_numerical_outliers_count = len(np.where(outlier_mask)[0])
    
            # compute total numer of numerical values
            total_numerical_values_count += (len(internal_representation)-1)

    outliers_percent = round((total_numerical_outliers_count / total_numerical_values_count) * 100, 3)

    # compute the dataquality of the IR
    ir_dq = round(compute_dq(internal_representation, schema), 3)

    return {
            'missing': {'missing_cells_percent': missing_values_percent},
            'outliers' : {'numerical_outliers_percent' : outliers_percent},
            'duplicates': {'duplicate_rows_percent' : duplicate_rows_percent},
            'dq': ir_dq
        }


def standardise_features(internal_representation, mapping):

    for x_attribute, y_attribute in mapping.items():

        source = x_attribute.split("+")

        if len(source) == 2:

            # need to merge multiple columns in the internal_representation and rename the resulting columns
            x1_column_name, x2_column_name = source[0], source[1]
            
            # locate the indices where these columns occur within the headers of the internal representation
            x1_indx, x2_indx = internal_representation[0].index(x1_column_name), internal_representation[0].index(x2_column_name)

            # rename the column x1_column_name to y_attribute
            internal_representation[0][x1_indx] = y_attribute

            # join column x2 onto column x1
            for row in internal_representation[1:]:
                row[x1_indx] = (json.dumps(row[x1_indx])[1:-1] + " " + json.dumps(row[x2_indx])[1:-1]).strip()
                
            # drop column x2 from the internal representation
            for row in internal_representation:
                row.pop(x2_indx)

        else:
            attribute = source[0]

            # check if the attribute is one of the special keywords
            if attribute == 'DROP':
                
                # extract the column names to be dropped
                column_names_todrop = y_attribute.split("+")

                if column_names_todrop[0] != '': # this occurs when we try to split an empty string, aka there is nothing to drop

                    # get the index of each column to be dropped within the headers of the internal representation
                    column_names_todrop_indices = [ internal_representation[0].index(name) for name in column_names_todrop ]

                    # remove each column index from each row in the internal representation
                    for i in range(len(internal_representation)):
                        internal_representation[i] = [ internal_representation[i][j] for j in range(len(internal_representation[i])) if j not in column_names_todrop_indices ]

            elif attribute == 'CREATE':
                
                # extract the column names to be created
                column_names_tocreate = y_attribute.split("+")

                if column_names_tocreate[0] != '': # this occurs when we try to split an empty string, aka there is nothing to create

                    # create a new column in the internal representation
                    for name in column_names_tocreate:

                        # extend the column headers
                        internal_representation[0].append(name)

                        # populate the new column
                        for row_indx in range(1, len(internal_representation)):
                            internal_representation[row_indx].append('_created_')

            else:
                # simply rename the column to whatever is the value of y_attribute
                column_indx = internal_representation[0].index(attribute)
                internal_representation[0][column_indx] = y_attribute

    return internal_representation

def compute_graphs(source, target, weight_threshold=0.5):

    model = SentenceTransformer("all-MiniLM-L6-v2")

    # embed the input and target headers
    source_embeddings, target_embeddings = model.encode(source), model.encode(target)
    source_attributes_embedding_map = { attribute : embedding for attribute, embedding in zip(source, source_embeddings) }
    target_attributes_embedding_map = { attribute : embedding for attribute, embedding in zip(target, target_embeddings) }

    X, Y = source, target
    gX, gY = {}, {} # becomes the preference list for each attribute in X and Y (preference is the weight computed)

    for v in Y: 
        for u in X: 
            sentence_similiarity = model.similarity(source_attributes_embedding_map[u], target_attributes_embedding_map[v])[0][0].item()
            levenshtein_distance = fuzz.ratio(u, v)/100 
            weight = round((levenshtein_distance + sentence_similiarity)/2, 3)

            if weight > weight_threshold:
                # record the undirected edge between the nodes u and v
                gX[u] = gX[u] + [(v, weight)] if u in gX else [(v, weight)]
                gY[v] = gY[v] + [(u, weight)] if v in gY else [(u, weight)]

    # sort each node's edge list by weight descending - required by the schema-matching algorithm
    for node in gX: gX[node].sort(key=lambda edge : edge[1], reverse = True)
    for node in gY: gY[node].sort(key=lambda edge : edge[1], reverse = True)
    
    return gX, gY

def gale_shapley(source, target, diff=0.05):

    X, Y = compute_graphs(source, target)

    unmatched_x = list(X.keys()) # all attributes from the input schemas start unmatched
    matched_attributes = {y: [] for y in Y}  
    # add special keys which map creation and deletion of attributes
    matched_attributes['CREATE'] = []
    matched_attributes['DROP'] = []
    rejected_matches = {x: [] for x in X} # record matches which get discarded during execution

    progress = [len(unmatched_x)] # record the progress of the algorithm, used to surpass stalling points

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

        progress.append(len(unmatched_x))
        if len(progress) > len(X.keys()) and progress[-(len(X.keys())-1)] == len(unmatched_x): # halt the algorithm when a stalling point is reached
            break
        
    # identify attributes from the Y set which are created
    for attribute in target:
        if attribute not in matched_attributes:
            matched_attributes['CREATE'].append(attribute)

    # swap the mapping order to be x -> y instead of y -> x
    output = {}
    for y, x in matched_attributes.items():
        if y == 'CREATE' or y == 'DROP': output[y] = x
        else: output['+'.join(x)] = y

    # identify attributes from the X set which are dropped
    temp = ''.join([ ''.join(entry) for entry in matched_attributes.values() ])
    for attribute in source:
        if attribute not in temp:
            output['DROP'].append(attribute)

    output['DROP'] = '+'.join(output['DROP'])
    output['CREATE'] = '+'.join(output['CREATE'])

    return output

def missing_value_handler(internal_representation, schema, strategy):
    headers = internal_representation[0]

    if strategy == 'impute':

        for column_indx, column_name in enumerate(headers):
            column_type = schema[column_name] # get the column's type
            # impute the cell according to its column's type
            for row_indx in range(1, len(internal_representation)):
                cell = internal_representation[row_indx][column_indx]
                if cell is None or cell == '':
                    if column_type == 'number': internal_representation[row_indx][column_indx] = 0.0
                    elif column_type == 'boolean': internal_representation[row_indx][column_indx] = False
                    elif column_type == 'ambiguous': 
                        internal_representation[row_indx][column_indx] = [] if isinstance(cell, list) else {}
                    else: internal_representation[row_indx][column_indx] = 'NA' # this covers both string and complex data types

    elif strategy == 'drop.rows':

        # store all indices of rows to be dropped from the internal representation
        null_rows_indices = []
        # drop all rows which contain a null value
        for row_indx in range(1, len(internal_representation)):
            for cell in internal_representation[row_indx]:
                if cell is None or cell == '':
                    row_offset = len(null_rows_indices) # compute offset to account for number of rows decreasing during drop operation
                    null_rows_indices.append(row_indx - row_offset)
                    break
        
        for index in null_rows_indices:
            internal_representation.pop(index)

    elif strategy == 'drop.columns': 

        null_columns_indices = []        
        for column_indx, column_name in enumerate(headers):

            # drop all columns which contain more than 50% null values
            null_cells_count = 0

            for row in internal_representation[1:]:
                if row[column_indx] == "" or row[column_indx] is None:
                    null_cells_count += 1
            
            # compute the ratio of null cells in the current column
            null_ratio = null_cells_count / (len(internal_representation)-1)
            if null_ratio >= 0.5:
                null_columns_indices.append(column_indx)
        
        # remove all flagged columns by filtering them out
        for row_indx, row in enumerate(internal_representation):
            internal_representation[row_indx] = [row[i] for i in range(len(row)) if i not in null_columns_indices]

    else:
        logging.error(f"Strategy '{strategy}' not supported for handling missing values.")

    return internal_representation

def duplicate_values_handler(internal_representation):
    unique_rows = []
    seen = set()
    for row in internal_representation:
        key = json.dumps(row)
        if key not in seen:
            unique_rows.append(row)
            seen.add(key)
    return unique_rows

"""
function outlier_handler
inputs : the internal representation, a strategy for handling outliers, a schema to lookup the column's type
output : the internal representation, with outlier values handled according to the strategy
NOTE that this method requires the missing_value_handler to be invoked first, otherwise null values will break the outlier handling process!
NOTE that outlier detection is only done on numerical columns. the detection strategy uses z-scores, and imputation is done using the median of the column
"""

def outlier_handler(internal_representation, schema, strategy):

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


"""
function compute_dq
input : an internal representation and schema for the columns in the IR
output : a DQ score between 0.0 and 1.0
"""
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
    duplicate_rows_ratio = duplicate_rows / total_cells
    outliers_percent = total_numerical_outliers_count / total_numerical_values_count

    final_dq = 1 - abs((missing_values_ratio + duplicate_rows_ratio + outliers_percent)/3)
    return final_dq


"""
function apply_plan
inputs : internal_representation, plan
outputs : internal representation after plan is applied, or None if the plan fails, target_headers is list of columns used to determine the schme amapping
"""
def apply_airflow_plan(internal_representation, schema, plan):
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

    # extract file name from plan
    filepath = "input\\source\\" + plan[0]

    # read file into internal representation and infer schema
    # NOTE - this repetitive work could be avoided by having the DAG publish the artifacts, and spark read them from kafka

    reconstruction_key = plan[1]['associated_key']

    # extract column mapping from plan
    mapping = plan[2]['standardiseFeatures']

    # apply the column mapping
    ir = standardise_features(to_internal(filepath)[1], mapping)

    # infer the schema from the internal representation
    schema = infer_schema(ir)

    # parse the intermediary steps of the plan
    for step in plan[3:-1]:

        components = step.split("/")
        action = components[0]
        if len(components) == 2:
            strategy = components[1]
        if action == "missingValues": ir = missing_value_handler(ir, schema, strategy)
        if action == "duplicates": ir = duplicate_values_handler(ir)
        if action == "outliers": ir = outlier_handler(ir, schema, strategy)

    # apply the final step of the plan, which is standardisation of values
    llm_code = plan[-1]["standardiseValues"]

    namespace = {}
    compiled_code = compile(llm_code, "<string>", "exec")
    exec(compiled_code, namespace)  # Execute in a separate namespace

    transform_table = namespace['transform_table']

    try:
        output = transform_table(ir)
    except (BaseException, TypeError, ValueError) as e:
        # the ir is not modified, we return whatever we have before the failed method!
        logging.error(f"Could not apply plan, encountered the following exception : {e}")
        return (filepath, None, None)

    return (filepath, reconstruction_key, output)



def extract_list(collection, key):
    if isinstance(collection, list): return (key, collection)
    if isinstance(collection, dict):
        for k, value in collection.items():
            result = extract_list(value, k)
            if result: 
                return result



def to_internal(filepath):

    internal_representation = [] 

    # extract the file type
    filetype = filepath.split(".")[1]

    # when parsing json, this allows us to reconstruct the file. 
    # it essentially tells us which key the list of objects we will transform belongs to!

    if filetype == 'csv':
        with open(filepath, 'r', encoding='utf=8-sig') as file:
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
    