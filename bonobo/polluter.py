import os
import csv
import json
import random
import numpy as np

from bonoboutils import to_internal, infer_schema, compute_etl_metrics, compute_dq
 
def inject_missing_values(ir, schema):
    for i in range(1, len(ir)):
        for j in range(len(ir[i])):
            celltype = schema[ir[0][j]]
            # nullify the current cell with a 10% chance
            if random.random() <= 0.4:
                ir[i][j] = "" if celltype == "string" else None
    return ir

def inject_duplicates(ir):
    n = len(ir)
    for i in range(1, n):
        if random.random() <= 0.2:
            # duplicate the current row with a 10% chance
            ir.append(ir[i])
    return ir

def inject_outliers(ir, schema):
    for i in range(1, len(ir)):
        for j in range(len(ir[i])):
            celltype = schema[ir[0][j]]
            # turn the current cell into an outlier with a 10% chance
            if random.random() <= 0.2 and ir[i][j] and celltype == "number":
                ir[i][j] = str(float(ir[i][j]) * 100)
    return ir

def load(path, reconstruction_key, ir):

    # extract the filename from path
    filename = path.split("\\")[-1]
    file_ext = filename.split(".")[-1]

    if file_ext == "csv":
        with open("..\\datasetspolluted\\" + filename, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(ir) 
        
    elif file_ext == "json":

        reconstructed_objs = [] # the list of transformed objects
        keys = ir[0]
        for row in ir[1:]:
            obj = {}
            for key, value in zip(keys, row):
                if value != '_ext_':
                    obj[key] = value
            reconstructed_objs.append(obj)

        # read over json file, making sure to keep non-transformed attributes
        with open("../datasets/source/json/" + path, 'r') as file:
            dictionary = json.load(file)

        if reconstruction_key and reconstruction_key in dictionary:
            dictionary[reconstruction_key] = reconstructed_objs

        with open("../datasetspolluted/" + filename, "w") as file:
            json.dump(dictionary, file, indent=4)

    else:
        print("error")

if __name__ == "__main__":

    # operate on the csv files first    
    basedir, csvdir, jsondir = "../datasets/source/", "csv/", "json/"
    csvfilepaths = [ f.name for f in os.scandir(basedir + csvdir) if f.is_file() ]
    jsonfilepaths = [ f.name for f in os.scandir(basedir + jsondir) if f.is_file() ]

    for path in csvfilepaths:
        # convert file contents to IR and infer schema
        key, ir = to_internal(basedir + csvdir + path)
        schema = infer_schema(ir)
        dq1 = round(compute_dq(ir, schema),3)
        metrics1 = compute_etl_metrics(ir, schema)
        # artifically inject missing values
        ir = inject_missing_values(ir, schema)
        ir = inject_outliers(ir, schema)
        ir = inject_duplicates(ir)
        dq2 = round(compute_dq(ir, schema),3)
        metrics2 = compute_etl_metrics(ir, schema)
        print(path, dq1, dq2, metrics1, metrics2)

        # write the polluted file in the source folder
        load(path, key, ir)
        
    for path in jsonfilepaths:
        # convert file contents to IR and infer schema
        key, ir = to_internal(basedir + jsondir + path)
        schema = infer_schema(ir)
        dq1 = round(compute_dq(ir, schema),3)
        metrics1 = compute_etl_metrics(ir, schema)
        # artifically inject missing values
        ir = inject_missing_values(ir, schema)
        ir = inject_outliers(ir, schema)
        ir = inject_duplicates(ir)
        dq2 = round(compute_dq(ir, schema),3)
        metrics2 = compute_etl_metrics(ir, schema)
        print(path, dq1, dq2, metrics1, metrics2)

        # write the polluted file in the source folder
        load(path, key, ir)
        