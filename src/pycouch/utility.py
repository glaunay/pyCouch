import re, json, os, random
# Utility function to check the content of a previous insert log file


reFile=re.compile('^globing.+\/([^\/]+) #items [\d]+$')
reError=re.compile('^Error here ==> (.*)$')

'''
Returns pickle files along with words that failed insertion
{'Candidatus Moranella endobia PCIT GCF_000219175.1.p': ['AAGATATGGTCCACTTTGGGTGG', ...],
 'Cardinium endosymbiont cEper1 of Encarsia pergandiella GCF_000304455.1.p': ['AAGGTAAATATGTATGTGGGGGG', ...],
  ...
}
'''
def extractError(fileName):
    errorRecords = {}
    currRecord = None 
    with open(fileName, 'r') as f:
        for l in f:
            m = reFile.search(l)
            mErr = reError.search(l)
            if m:
                id = os.path.splitext(m.groups(1)[0])[0]
                errorRecords[id] = []
                currRecord = errorRecords[id] 
                #print(m.groups(1))
            elif mErr:
                d = json.loads(mErr.group(1).replace("'", "\""))
                currRecord.append(d["id"])
    return { x:d for x, d in errorRecords.items() if len(errorRecords[x]) > 0 }

def troubleGenerator(motif):
    if random.randint(1,3) == 1:
        return {'id': motif, 'error': 'unknown_error', 'reason': 'undefined'}
    return None