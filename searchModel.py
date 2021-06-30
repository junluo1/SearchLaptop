import pandas as pd
from fuzzywuzzy import fuzz
from elasticsearch import Elasticsearch, helpers
import csv
import requests
import json

'''
this function analyze the dataset and create a unique value list for every useful attribute, return the matrix of atrribute value list
'''


def analyzeData():
    # import dataset laptop_price
    docs = pd.read_csv('laptop_price.csv')

    # create unique value lists for every useful attribute
    companies = docs['Company'].unique()
    products = docs['Product'].unique()
    typenames = docs['TypeName'].unique()
    inches = docs['Inches'].unique()
    screens = docs['ScreenResolution'].unique()
    cpus = docs['Cpu'].unique()
    rams = docs['Ram'].unique()
    memories = docs['Memory'].unique()
    gpus = docs['Gpu'].unique()
    opsys = docs['OpSys'].unique()

    # create attribute unique value matrix
    attrs = [companies, products, typenames,
             inches, screens, cpus, rams, memories, gpus, opsys]
    return attrs


# create a list of attribute names maintained, so that we know which attribute we find out when iterating through the terms
attrsNames = ["Company", "Product", "TypeName",
              "Inches", "ScreenResolution", "Cpu", "Ram", "Memory", "Gpu", "OpSys"]

'''
 the Core function to analyze the query, takes 2 params, attribute unique value matrix and text query
 check what kind of attributes are covered in the query, query is a splited list of query words
 I use fuzz module to match the term with every list of values, keep the average score
 finally the attribute list that gets the highest score would mostly likely be the atrribute that this value matches
 return a dictionary that store the matched attribute as key and term as value for search use later
'''


def detectAttr(attrs, query):
    query = query.split()
    # the returned value
    searchQuery = {}
    # match every term in the query with the matrix of unique values
    for term in query:
        # interate through every atrribute value list except for weight and price which people often use adjectives
        attrScores = []
        for i in range(len(attrs)):
            # call the function matchTitle to see if the term is contained in the list
            avgScore = matchTitleScore(term, attrs[i])
            attrScores.append(avgScore)
        # print("attrScores: ", attrScores)
        # get the index of the most matched attribute
        idx = attrScores.index(max(attrScores))
        # store the term as value and attribute as key for later use when searching
        if attrsNames[idx] in searchQuery:
            searchQuery[attrsNames[idx]] += " " + term
        else:
            searchQuery[attrsNames[idx]] = term
    return searchQuery


'''
 function called by detectAttr to calculate the average score of a term is similar to values in the unique value list, add fuzziness to tolerant spelling error
 return the avarge score of fuzz match
'''


def matchTitleScore(term, attrs):
    scores = []
    for i in attrs:
        # return 100 if got a complete match
        if fuzz.token_set_ratio(term, i) == 100:
            return 100
        # otherwise calculate the average for scores
        scores.append(fuzz.token_set_ratio(term, i))
    return sum(scores)/len(scores)


'''
elasticsearch object class 
'''


class ElasticObj:
    def __init__(self, index_name, index_type):
        self.index_name = index_name
        self.index_type = index_type
        self.es = Elasticsearch(host='localhost', port=9200)

    '''
    main function that import data from csv file and adding documents to elastic index
    '''

    def importData(self, filename):
        with open(filename, encoding='ISO-8859-1') as f:
            reader = csv.DictReader(f)
            helpers.bulk(self.es, reader, index=self.index_name)

    '''
    function that handles search querys, do a most_fields search 
    '''

    def search(self, query, attr):
        # query doc, most fields search
        subLst = []

        lstQuery = []
        for key, val in attr.items():
            subDct = {}
            subDct[key] = val
            subLst.append(subDct)
        for item in subLst:
            dct = {}
            dct['match'] = item
            lstQuery.append(dct)

        doc = {
            "size": 30,
            "query": {
                "bool": {
                    "should": lstQuery
                }
            }}
        print(attr)
        print(doc)

        res = self.es.search(index=self.index_name, body=doc)
        return res


# run a analysis for the data, collect information for analyze query
attrs = analyzeData()
# check connection
requests.get('http://localhost:9200')
# create elastic search object and build index
obj = ElasticObj("my_index", "docs")
obj.importData('laptop_price.csv')
