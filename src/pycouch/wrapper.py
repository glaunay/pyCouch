import requests
import json, re
import copy as cp

DEFAULT_END_POINT='http://127.0.0.1:5984'

QUEUE_MAPPER = None

DEBUG_MODE = False

def setKeyMappingRules(ruleLitt) :
    global QUEUE_MAPPER
    QUEUE_MAPPER = {}
    for regExp in ruleLitt:
        QUEUE_MAPPER[regExp] = {
            "volName" : ruleLitt[regExp],
            "queue" : {}
        }


def putInQueue(key, val):
    global QUEUE_MAPPER

    for regExp, cQueue in QUEUE_MAPPER.items():
        if re.search(regExp, key):
            cQueue["queue"][key] = val
            return
    raise ValueError("${key} not found")

def resetQueue():
    for regExp, cQueue in QUEUE_MAPPER.items():
        cQueue["queue"] = {}

def setServerUrl(path):
    global DEFAULT_END_POINT
    DEFAULT_END_POINT = path

def lambdaFuse(old, new):
    for k in new:
        old[k] = new[k]
    return old

## MULTIPLE VOLUME BULK FN WRAPPER 
def volDocAdd(iterable, updateFunc=lambdaFuse):
    global QUEUE_MAPPER
    if not QUEUE_MAPPER:
        raise ValueError ("Please set volume mapping rules")
    
    if DEBUG_MODE:
        print("Dispatching", len(iterable), "items")

    data = []
    for k,v in list(iterable.items()):
        putInQueue(k,v)
    for regExp, cQueue in QUEUE_MAPPER.items():
        if not cQueue["queue"]:
            continue
        print("inserting ", regExp, str(len(cQueue["queue"])), "element(s) =>", cQueue["volName"])
        if DEBUG_MODE:
            print("DM",cQueue["queue"])
        
        data += bulkDocAdd(cQueue["queue"], updateFunc=updateFunc, target=cQueue["volName"])
    
    resetQueue()
    return data

        
## BULK FUNCTIONS

def bulkDocAdd(iterable, updateFunc=lambdaFuse, target=None): # iterable w/ key:value pairs, key is primary _id in DB and value is document to insert
    global DEFAULT_END_POINT
    
    if not target:
        raise ValueError ("No target db specified")
        
    ans = bulkRequestByKey(list(iterable.keys()), target)# param iterable msut have a keys method

    if DEBUG_MODE:
        print("bulkDocAdd prior key request ", ans.keys())
        print(ans)
    
    bulkInsertData = {"docs" : [] }

    for reqItem in ans['results']:       
        key = reqItem["id"]
        dataToPut = iterable[key]
        dataToPut['_id'] = key
        
        _datum = reqItem['docs'][0] # mandatory docs key, one value array guaranted
        if 'error' in _datum:
            if docNotFound(_datum["error"]):
                if DEBUG_MODE:
                    print("creating ", key, "document" )
            else:
                print ('Unexpected error here', _datum)
            
        elif 'ok' in _datum:
            dataToPut = updateFunc(_datum["ok"], iterable[key])
        else:
            print('unrecognized item packet format', str(reqItem))
            continue
        bulkInsertData["docs"].append(dataToPut)
        
    if DEBUG_MODE:
        print("about to bulk_doc that", str(bulkInsertData))
        
    r = requests.post(DEFAULT_END_POINT + '/' + target + '/_bulk_docs', json=bulkInsertData)
    return json.loads(r.text)

def bulkRequestByKey(keyIter, target, packetSize=2000):          
    data = {"results" : []}
    if DEBUG_MODE:
        print("bulkRequestByKey at", target)
    for i in range(0,len(keyIter), packetSize):
        j = i + packetSize if i + packetSize < len(keyIter) else len(keyIter)
        keyBag = keyIter[i:j]
        _data = _bulkRequestByKey(keyBag, target)
       # data["results"].append(_data["results"])
        data["results"] += _data["results"]
    #if DEBUG_MODE:
    #    print(data)
    return data

def _bulkRequestByKey(keyIter, target):
    req = {
        "docs" : [ {"id" : k } for k in keyIter ]
    }
    url = DEFAULT_END_POINT + '/' + target +'/_bulk_get'
    r = requests.post(url, json=req)
    data = json.loads(r.text) 
    if not 'results' in data:
        raise TypeError("Unsuccessful bulkRequest at", url)
    return data



## NON BULK FUNCTIONS
def couchPing():
    global DEFAULT_END_POINT
    data = ""
    try :
        r = requests.get(DEFAULT_END_POINT)
        try :
            data = json.loads(r.text)
        except:
            print('Cant decode ping')
            return False
    except:
        print("Cant connect to DB at: %s", DEFAULT_END_POINT)
        return False

    print("Connection established\n", data)
    return True

def couchPS():
    global DEFAULT_END_POINT
    r = requests.get(DEFAULT_END_POINT + '/_active_tasks')
    return json.loads(r.text)

def couchGetRequest(path):
    global DEFAULT_END_POINT
    r= requests.get(DEFAULT_END_POINT + '/' + path)
    resulttext = r.text
    return json.loads(resulttext)

def couchPutRequest(path, data):
    global DEFAULT_END_POINT
    r= requests.put(DEFAULT_END_POINT + '/' + path, json=data)
    resulttext = r.text
    return json.loads(resulttext)

def couchDbList():
    return couchGetRequest('_all_dbs')

def couchGenerateUUID():
    return couchGetRequest('_uuids')["uuids"][0]

def docNotFound(data):
    if "error" in data and "reason" in data:
        return data["error"] == "not_found" and data["reason"] == "missing"
    return False

def couchGetDoc(target, key):
    if not key:
        raise ValueError("Please specify a document key")
        
    MaybeDoc = couchGetRequest(str(target) + '/' + str(key))
    if docNotFound(MaybeDoc):
        return None
    return MaybeDoc

def couchPutDoc(target, key, data):
    MaybePut = couchPutRequest(target + '/' + key, data)
    return MaybePut

def couchAddDoc(data, target=None, key=None, updateFunc=lambdaFuse):
    if not target:
        raise ValueError("Please specify a database to target")

    key = couchGenerateUUID() if not key else key
    ans = couchGetDoc(target,key)
    
    dataToPut = data
    if not ans:
        if DEBUG_MODE:
            print ("Creating " + target + "/" + key)       
            print(ans)
    else :
        if DEBUG_MODE:
            print ("Updating " + target + "/" + key)
            print(ans)
        dataToPut = updateFunc(ans, data)
    
    ans = couchPutDoc(target, key, dataToPut)

    if DEBUG_MODE:
        print(ans)