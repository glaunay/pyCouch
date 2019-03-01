"""
    Build a couchDB database of genome_ref pickled entries
    Usage:
        couchBuild.py <databaseName> [--url <urlLocation> --data <inputFolder> --size <batchSize> --min <firstToIndex> --max <lastToIndex> ]
        couchBuild.py <databaseName> [--url <urlLocation> --data <inputFolder> --dinfo --linfo ]

    Options:
        -h --help                               Show this screen
        -u <urlLocation>, --url <urlLocation>   DB URL end-point [default: http://localhost:5984]
        -v, --dinfo                             Display Database statistics
        -l, --linfo                             Display input Folder statistics
        -b <batchSize>, --size <batchSize>      Maximal number of entry to add at a time
        -d <inputFolder>, --data <inputFolder>  Input file statistics
        -i <firstToIndex>, --min <firstToIndex> Index of the first file to add to database
        -j <lastToIndex>, --max <lastToIndex>   Index of the last file to add to database

"""
import sys, re, pickle, os
import copy, json


from docopt import docopt
import pycouch.wrapper as couchDB


# Convenience functions to split the content of pickle input file
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
    for k in data:
        if i == size:
            break
        subSet[k] = data[k]
        i += 1
        
    print (len(data), len(subSet))
    return subSet





if __name__ == "__main__":
    arguments = docopt(__doc__, version='couchBuild 1.0')

    #print (arguments)

    if arguments['--url']:
        couchDB.setServerUrl(arguments['--url'])
    if not couchDB.couchPing():
        exit(1)

    batchSize = 10000 if not arguments['--size'] else  arguments['--size']
    #res = {}
    
    if arguments['--data']:
        rootDir = arguments['--data']
        fileNames = sorted([  (f, os.path.getsize(rootDir+'/'+f) ) for f in os.listdir(rootDir) ], key=lambda x:x[1])
        iMin = arguments['--min'] if arguments['--min'] else 0
        iMax = arguments['--max'] if arguments['--max'] else len(fileNames)
        if iMax > len(fileNames):
            iMax = len(fileNames)

        for fName in fileNames[iMin:iMax]:
            dataFile = rootDir + '/' + fName[0]
            c = GenomeData(dataFile)
            print("globing", dataFile, "#items", len(c))

            for i in range(0,len(c), batchSize):
                j = i + batchSize if i + batchSize < len(c) else len(c)
                #print(i,':',j)
                d = c[i:j]
                r = couchDB.bulkDocAdd(d, target=arguments['<databaseName>'])
        #res[str(i) + ':' + str(j)] = r
                for d in r:
                    if not 'ok' in d:
                        print ("Error here ==>", str(d))
