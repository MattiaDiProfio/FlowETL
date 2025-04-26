import os
import csv
import json
import random
from bonoboutils import to_internal, infer_schema, compute_etl_metrics, compute_dq
 

def inject_missing_values(ir, schema):
    """
    Method converts 40% of the values within the input internal representation to missing

    :param ir: the internal represenation
    :param schema: the internal representation's schema
    :return ir: the polluted interal representation  
    """

    for i in range(1, len(ir)):
        for j in range(len(ir[i])):
            celltype = schema[ir[0][j]]
            if random.random() <= 0.4:
                ir[i][j] = "" if celltype == "string" else None
    return ir


def inject_duplicates(ir):
    """
    Method duplicates 20% of the rows within the input internal representation

    :param ir: the internal represenation
    :return ir: the polluted interal representation  
    """

    n = len(ir)
    for i in range(1, n):
        if random.random() <= 0.2:
            ir.append(ir[i])
    return ir


def inject_outliers(ir, schema):
    """
    Method converts 20% of the numerical values within the input internal representation to outliers

    :param ir: the internal represenation
    :param schema: the internal representation's schema
    :return ir: the polluted interal representation  
    """

    for i in range(1, len(ir)):
        for j in range(len(ir[i])):
            celltype = schema[ir[0][j]]
            if random.random() <= 0.2 and ir[i][j] and celltype == "number":
                ir[i][j] = str(float(ir[i][j]) * 100)
    return ir
  

def load(path, reconstruction_key, ir):
    """
    Method  converts the transformed internal representation back to its original file type and writes its contents to the output folder

    :param path: the source's file name and type
    :param reconstruction_key: the reconstruction key required if the original file type is of type JSON
    :param ir: the transformed internal representation
    :return: None
    """

    # extract the source's file name and file type
    filename = path.split("\\")[-1]
    file_ext = filename.split(".")[-1]

    if file_ext == "csv":

        # if the original file is structured, simply dumpt the the internal representation to a csv file
        with open("..\\datasetspolluted\\" + filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(ir) 
        
    elif file_ext == "json":

        # initialise empty list of transformed json objects, i.e., the contents of the internal represenation
        reconstructed_objs = [] 

        keys = ir[0] # extract the internal representation's column headers, corresponding to the union of keys of the json objects

        # iterate the internal representation's values and convert each row into a json object, discarding extended key-value pairs
        for row in ir[1:]:
            obj = {}
            for key, value in zip(keys, row):
                if value != '_ext_':
                    obj[key] = value
            reconstructed_objs.append(obj)

        # read over the json file, making sure to keep non-transformed attributes
        with open("../evaluation_datasets/source/json/" + path, 'r') as file:
            dictionary = json.load(file)

        if reconstruction_key and reconstruction_key in dictionary:
            dictionary[reconstruction_key] = reconstructed_objs
        
        with open("../datasetspolluted/" + filename, "w") as file:
            # edge case occurs when the source json is just a list of objects
            json.dump(dictionary, file, indent=4)

    else:
        print("Error occurred during the loading ")


if __name__ == "__main__":

    # extract filepaths for all evaluation datasets
    basedir, csvdir, jsondir = "../evaluation_datasets/source/", "csv/", "json/"
    csvfilepaths = [ f.name for f in os.scandir(basedir + csvdir) if f.is_file() ]
    jsonfilepaths = [ f.name for f in os.scandir(basedir + jsondir) if f.is_file() ]

    for path in csvfilepaths:

        # convert the file to an internal representation and infer its schema
        key, ir = to_internal(basedir + csvdir + path)
        schema = infer_schema(ir)

        # gather the pre-pollution data quality score and etl metrics
        dq1 = round(compute_dq(ir, schema),3)
        metrics1 = compute_etl_metrics(ir, schema)

        # artifically pollute the internal representation
        ir = inject_missing_values(ir, schema)
        ir = inject_outliers(ir, schema)
        ir = inject_duplicates(ir)

        # gather the post-pollution data quality score and etl metrics
        dq2 = round(compute_dq(ir, schema),3)
        metrics2 = compute_etl_metrics(ir, schema)

        # replace the original file with its polluted version
        load(path, key, ir)

        
    for path in jsonfilepaths:

        # convert the file to an internal representation and infer its schema
        key, ir = to_internal(basedir + jsondir + path)
        schema = infer_schema(ir)

        # gather the pre-pollution data quality score and etl metrics
        dq1 = round(compute_dq(ir, schema), 3)
        metrics1 = compute_etl_metrics(ir, schema)

        # artifically pollute the internal representation
        ir = inject_missing_values(ir, schema)
        ir = inject_outliers(ir, schema)
        ir = inject_duplicates(ir)

        # gather the post-pollution data quality score and etl metrics
        dq2 = round(compute_dq(ir, schema),3)
        metrics2 = compute_etl_metrics(ir, schema)

        # replace the original file with its polluted version
        load(path, key, ir)
        