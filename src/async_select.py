import aiohttp
import asyncio
import json
import pandas as pd
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.algebra import translateQuery, translateAlgebra, pprintAlgebra

import sys
import os

sys.stdout = open(os.devnull, 'w')

FOLDER = "/data/bio2rdf-logs"

async def parseSPARQLquery(query):
    try:
        parsed = parseQuery(query);
        algebra = translateQuery(parsed);
        #pprintAlgebra(algebra)
        q = translateAlgebra(algebra);
        return q
    except:
        return ""

def progress(task):
    # report progress of the task
    print('.', end='')

async def process_all_queries():       
        data_folder = '/data/bio2rdf-logs'
        agg_pickle_file = f'{data_folder}/agg_logs.pkl' 
        df = pd.read_pickle(agg_pickle_file)
        #df = df[:5000].copy()
        queries = df.clean2.values.tolist()

        tasks = [parseSPARQLquery(query) for query in queries]
        for task in tasks:
            task.add_done_callback(progress)

        all_queries = await asyncio.gather(*tasks)

        return all_queries

def save_queries_to_json(queries, filename):
    with open(filename, "w") as file:
        json.dump(queries, file, indent=4)

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    select_queries = asyncio.run(process_all_queries())
    print(f"Total queries: {len(select_queries)}")
    #select_queries = list(dict.fromkeys(select_queries))

    save_queries_to_json(select_queries,f"{FOLDER}/async-queries.json")

    print(f"Total queries: {len(select_queries)}")
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")
