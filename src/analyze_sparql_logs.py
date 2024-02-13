import pandas as pd
import re
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.algebra import translateQuery, translateAlgebra, pprintAlgebra
from rdflib.plugins.sparql.parserutils import prettify_parsetree
import asyncio
import json


def readCSV(file):
    df = pd.read_csv(file, lineterminator='\n', dtype=str) 
    df.columns= ["query","domain","agent","timestamp"]
    return df

def filterNonSPARQL(text):
    keywords = ["select","insert","construct","ask"]
    t = text.lower()
    for keyword in keywords:
        if t.find(keyword) != -1:
            return t
    return ""

def removeHTTPparams(text):
    keywords = ["&format", "&timeout", "&debug", "&run", "&maxrows", "&infer", "&output", "&results", "&default-graph-uri"]

    # Regular expression to find any of the keywords in the query
    regex = '|'.join(re.escape(keyword) for keyword in keywords)

    # Remove the matched part of the query and everything after it
    result = re.sub(rf'({regex}).*', '', text)
    return result

def getSelectQuery(query):
    #query = "ASK WHERE { <http://nonsensical.com/1/1583904985165> ?p ?o}"
    #query = 'SELECT ?item ?info WHERE {?item rdfs:label ?info. FILTER(regex(?info, "muscular dystrophy", "i"))}'
    #query = 'ASK\nWHERE\n  { <http://nonsensical.com/1/1583905059047> <http://nonsensical.com/2/1583905059047> ?o}\n'
    #query = 'SELECT ?item ?info WHERE {?item rdfs:label ?info. FILTER(regex(?info, "muscular dystrophy", "i"))} &timeout=500000'
    #query = '\n\n prefix dv: <http://bio2rdf.org/drugbank_vocabulary:>\n prefix dct: <http://purl.org/dc/terms/>\n\n select distinct ?enzyme_name ?drug_name \n {\n ?s a dv:enzyme-relation .\n ?s dv:enzyme ?enzyme_name .\n ?s dv:drug ?drug_name .\n ?s dv:action dv:substrate .\n } \n'
    try:
        parsed = parseQuery(query)
        algebra = translateQuery(parsed)
        #pprintAlgebra(algebra)
        q = translateAlgebra(algebra);
        return q
    except:
        return ""
    return ""

def prepareCleanDF(bio2rdf_log_file):
    df = readCSV(bio2rdf_log_file)
    df.to_pickle(pickle_file)
    df = pd.read_pickle(pickle_file)
    
    df['clean1'] = df.apply(lambda row: filterNonSPARQL(row['query']), axis=1)
    df['clean2'] = df.apply(lambda row: removeHTTPparams(row['clean1']), axis=1)

    df2 = df.groupby('clean2',as_index=False).size()
    return df2

if __name__ == "__main__":
    data_folder = '/data/bio2rdf-logs'
    bio2rdf_log_file = f'{data_folder}/bio2rdf_sparql_logs_processed_01-2019_to_07-2021.csv'
    pickle_file = f'{data_folder}/logs.pkl'
    agg_pickle_file = f'{data_folder}/agg_logs.pkl'
    select_query_file = f'{data_folder}/async-queries.json'
    select_pkl_file = f'{data_folder}/select-queries.pkl'

    #df2 = prepareCleanDF(bio2rdf_log_file)
    #df2.to_pickle(agg_pickle_file)
    #df2 = pd.read_pickle(agg_pickle_file)

    with open(select_query_file, "r") as file:
        data = json.load(file)

    data = list(dict.fromkeys(data))
    df = pd.DataFrame(data, columns=['query'])
    df.to_pickle(f'{data_folder}/{select_pkl_file}')
    
    print('hello')