import json
import uuid
import os
import glob
import pandas as pd
import logging

def get_columns(ds):
    schemas_file_path = os.environ.setdefault('SCHEMA_FILE_PATH','data/retail_db/schemas.json')
    with open(schemas_file_path) as fp:
        schemas =   json.load(fp)
        #print(schemas)
    try:
        schema = schemas.get(ds)
        #print(schema)
        if not schema:
            raise KeyError
        cols = sorted(schema , key = lambda s : s['column_position'])
        columns = [col['column_name'] for col in cols]
        return columns
    except KeyError:
        raise
             
        
def process_file(src_base_dir,ds,tgt_base_dir):
    try:
        for file in glob.glob(f'{src_base_dir}/{ds}/part*'):
            df =pd.read_csv(file,names = get_columns(ds))
            os.makedirs(f'{tgt_base_dir}/{ds}',exist_ok=True)
            df.to_json(f'{tgt_base_dir}/{ds}/part-{str(uuid.uuid1())}.json',orient = 'records',lines=True)
            logging.info(f'Num of records Processed for {os.path.split(file)[1]} in {ds} is {df.shape}')
    except KeyError:
        raise

def main():      
    logging.basicConfig(filename = 'logs/ffc.log',
                    level = 'INFO', format = '%(levelname)s' '%(asctime)s' '%(message)s',
                    datefmt='%Y-%m-%d %I:%M:%S %p')
    src_base_dir = os.environ.get('SRC_BASE_DIR') 
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')  
    datasets = os.environ.get('DATASETS')
    logging.info("File format coversion started")
    if not datasets:
        for path in glob.glob(f'{src_base_dir}/*'):
            if os.path.isdir(path):
                process_file(src_base_dir,os.path.split(path)[1],tgt_base_dir)
    else :
        dirs = datasets.split(',')
        try:
            for ds in dirs:
                process_file(src_base_dir,ds,tgt_base_dir)
        except Exception as e:
            logging.error(f'file format conv for {ds} is not succesful')
            
    logging.info("Conversion is successful")
            # ds = os.path.split(path)[1]
            # for file in glob.glob(f'{path}/part*'):
            #     df =pd.read_csv(file,names = get_columns(ds))
            #     os.makedirs(f'{tgt_base_dir}/{ds}',exist_ok=True)
            #     df.to_json(f'{tgt_base_dir}/{ds}/part-{str(uuid.uuid1())}.json',orient = 'records',lines=True)
            #     print(f'Num of records Processed for {os.path.split(file)[1]} in {ds} is {df.shape}')




if __name__==  '__main__':
    main()