"""
    Build a database of fasta entries using file system directory architecture
    Usage:
        couchBuild.py <databaseName> [--url <urlLocation> --data <inputFolder>]
        couchBuild.py <location> [--nodes <volNumExp> --threads <nWorker> --size <size>]
    
    Options:
        -h --help                               Show this screen
        -u <urlLocation>, --url <urlLocation>   MultiFasta input file [default: http://localhost:5984]
        -v, --dinfo                             Display Database statistics
        -l, --linfo                             Display input Folder statistics
        -d <inputFolder>, --data <inputFolder>  Display Input file statistics
        -i <firstToIndex>, --min <firstToIndex> Index of the first file to build in database
        -j <lastToIndex>, --max <lastToIndex>   Index of the last file to build in database

"""
import sys, re, pickle, os
import copy, json


from docopt import docopt
import pycouch.wrapper as couchDB


# Convenience functions to split pickle input file
class GenomeData():
    def __init__(self, pFile):
        self.data = pickle.load( open( pFile, "rb" ) )
        
    def __getitem__(self, val):
        if isinstance(val, slice):
            return { k : copy.deepcopy(self.data[k]) for k in list(self.data.keys())[val] }

        k =  list(self.data.keys())[val]
        return { k : copy.deepcopy(self.data[k]) }

    def __len__(self):
        return len(self.data)
    
def getSubset(pFile, size=10):
    data = pickle.load( open( pFile, "rb" ) )
    subSet = {}
    i = 0
    for k in genomeData:
        if i == size:
            break
        subSet[k] = genomeData[k]
        i += 1
        
    print (len(data), len(subSet))
    return subSet





if __name__ == "__main__":
    arguments = docopt(__doc__, version='couchBuild 1.0')

    if arguments['--url']:
        couchDB.setServerUrl('http://localhost:1234')
    if not couchDB.couchPing():
        exit(1)
    targetDB='crispr_test'
batchSize = 10000
res = {}
    
for fName in fileNames[:1]:
    dataFile = rootDir + '/' + fName[0]
    c = GenomeData(dataFile)
    print("globing", dataFile, "#items", len(c))
    
    for i in range(0,len(c), batchSize):
        j = i + batchSize if i + batchSize < len(c) else len(c)
        print(i,':',j)
        d = c[i:j]
        r = couchDB.bulkDocAdd(d, target=targetDB)
        #res[str(i) + ':' + str(j)] = r
        for d in r:
            if not 'ok' in d:
                print ("Error here ==>", str(d))
