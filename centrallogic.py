from datetime import datetime
from elasticsearch import Elasticsearch
import logging
import json
from pprint import pprint

ES_HOST = "stg-serverdependency001.phonepe.nb6"
es = Elasticsearch(hosts=[ES_HOST], )


# def search(index_name, query):
#     res = es.search(index=index_name, body=query)
#     return query

def lhsoutput(search):
    searchterm = search
    process_key_list =[]
    destination_list=[]
    index_dep_list =[]
    #indexterm = 'stg-nginx400.phonepe.nb6'
    
    logging.basicConfig(level=logging.ERROR)

    index_list = es.cat.indices(index='stg-*', h='index', s='index:desc').split()
    print(index_list)
    print()

    #Loop 1 to iternate indexes
    for index in index_list:
        res = es.get(index=index, id=1)
        #print("Doing for - "+index)
        #pprint(res['_source']['Dependency'])
        #print('--------------------------------------------------------------------------------------------------------------------------------------------')
        #print(); print()

        total_dependencies = len(res['_source']['Dependency'])                          #Count the no of dependencies in a particular index
        
        #Loop 2 to iternate dependencies
        for counter in range(total_dependencies):                                       #Iternating over Dependency at a time
            dependencies = res['_source']['Dependency'][counter]
            #print()
            #print("Dependency " + str(counter) +  "-"); pprint(dependencies)

            process_key= list(dependencies.keys())[0]                                   #To find out the process name&port
            #print("Process Name - " + process_key)
            
            destination_count = len(dependencies[process_key]['destination'])           #To find out the destination host
            
            #Loop 3 to go through all destinations
            for count in range(destination_count):
                destination = dependencies[process_key]['destination'][count]['DestAddr']

                #Code to remove clouddb names and remove the domain, keeping only the hostname
                dest_host = "-".join(destination.split("-",2)[:2])
                destination_hostname = dest_host.replace('.phonepe.nb6','')
                searchterm_hostname = searchterm.replace('.phonepe.nb6','')
                #print("Destination & search Term Name - " + destination_hostname + '  ' + searchterm_hostname)
                
                if searchterm_hostname == destination_hostname:
                    print("Dependency found in "+ index)
                    process_key_list.append(process_key)
                    destination_list.append(destination_hostname)
                    index_dep_list.append(index)
                
    print("Process - ",process_key_list); print("LHS - ",index_dep_list);print("RHS - ",destination_list)


    # dict = {
    #     'process' : process_key_list,
    #     'lhs' : index_dep_list,
    # }
    # print(dict)
    #return dict

    list_dict=[]
    lhs_dict={}
    print(len(process_key_list))
    for x in range(len(process_key_list)):
        lhs_dict['process']=process_key_list[x]
        lhs_dict['lsh']=index_dep_list[x]
        list_dict.append(lhs_dict.copy())

    final_dict={'lhs_dep':list_dict}
    print(final_dict)

    return final_dict

#if __name__ == '__main__':
#    lefthandside = lhsoutput('stg-nginx400.phonepe.nb6')

    #ELASTICSEARCH PYTHONIC WAY OF SEARCHING THE DATA
    # query_body = {
    #     "query": {
    #          "match": {
    #            "serviceName": "stg-traefik004.phonepe.nb6"
    #           }
    #      }
    # }
    # data=es.search(index=searchterm, body=query_body)
    # print(data)

    # for idx in idx_list:
    #     raw_data = es.indices.get_mapping(idx)
    #     print ("get_mapping() response type:", type(raw_data))

    #     # returns dict_keys() obj in Python 3
    #     mapping_keys = raw_data[ idx ]["mappings"].keys()
    #     print ("\n_mapping keys():", mapping_keys)

    #     # get the index's doc type
    #     doc_type = list(mapping_keys)[0]
    #     print ("doc_type:", doc_type)

    #     # get the schema by accessing index's _doc type attr
    #     schema = raw_data[ idx ]["mappings"][ doc_type ]["properties"]
    #     print (json.dumps(schema, indent=4))
    #     print ("\n# of fields in _mapping:", len(schema))
    #     print ("all fields:", list(schema.keys()) )
