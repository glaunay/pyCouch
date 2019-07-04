"""
    Build a couchDB database of genome_ref pickled entries
    Usage:
        couchBuild.py <databaseName> [--url <urlLocation> --data <inputFolder> --size <batchSize> --min <firstToIndex> --max <lastToIndex> --fix <fixFile> --verbose]
        couchBuild.py [ --map <volumeMapperConf> --url <urlLocation> --data <inputFolder> --size <batchSize> --min <firstToIndex> --max <lastToIndex> --fix <fixFile> --verbose]
        couchBuild.py --list <inputFolder>

    Options:
        -h --help                                         Show this screen
        -u <urlLocation>, --url <urlLocation>             DB URL end-point [default: http://localhost:5984]
        -m <volumeMapperConf>, --map <volumeMapperConf>   DB volumes end-point mapper       
        -b <batchSize>, --size <batchSize>                Maximal number of entry to add at a time
        -d <inputFolder>, --data <inputFolder>            Input file statistics
        -i <firstToIndex>, --min <firstToIndex>           Index of the first file to add to database
        -j <lastToIndex>, --max <lastToIndex>             Index of the last file to add to database
        -x <fixFile>, --fix <fixFile>                     Path to the log file of a previous call
        -l <inputDir>, --list <inputDir>                  List content of file inputs
        -v, --verbose                                     Set verbose mode ON

"""
import sys, re, pickle, os
import copy, json


from docopt import docopt
import pycouch.wrapper as couchDB
from pycouch.utility import extractError

def getOrderFileList(path):
    rootDir = path
    return sorted([  (f, os.path.getsize(rootDir+'/'+f) ) for f in os.listdir(rootDir) ], key=lambda x:x[1])

def _lambda():
    return True


# Convenience functions to split the content of pickle input file
class GenomeData():
    def __init__(self, *args):
        if len(args) > 0:
            self.data = pickle.load( open(args[0], "rb" ) )
        
    def __getitem__(self, val):
        if isinstance(val, slice):
            return { k : copy.deepcopy(self.data[k]) for k in list(self.data.keys())[val] }

        k =  list(self.data.keys())[val]
        return { k : copy.deepcopy(self.data[k]) }

    def __len__(self):
        return len(self.data)
    
    def filter(self, lambdaF=_lambda):
        newGenomeData = GenomeData()
        newGenomeData.data = { k: v  for k,v in self.data.items()  if lambdaF(k,v) }
        return newGenomeData

if __name__ == "__main__":
    arguments = docopt(__doc__, version='couchBuild 1.0')
    print(arguments)
   
    couchDB.DEBUG_MODE = arguments['--verbose']
    print(couchDB.DEBUG_MODE)
    

    if arguments['--list']:
        fNames = getOrderFileList(arguments['--list'])
        for i,d in enumerate(fNames):
            print('[', i, ']', str(d)) 
        exit(0)

    if arguments['--url']:
        couchDB.setServerUrl(arguments['--url'])
    
    subSetToFix = None
    if arguments['--fix']:
        print("Extracting errors from", arguments['--fix'])
        subSetToFix = extractError(arguments['--fix'] )
        print(subSetToFix)
        #exit(1)

    def gDatumMotifMatchLambda(k,v):
        if (len( list(v.keys()) ) > 1):
            raise ValueError
        commonName = list(v.keys())[0]
        motif=k
        if not commonName + '.p' in subSetToFix: # Organism must be one for which list of failed word exist
            print(commonName + '.p', 'not in ', subSetToFix)
            raise ValueError
        
        return motif in subSetToFix[commonName + '.p'] # Is the motif part of the failed obne




    if not couchDB.couchPing():
        exit(1)

    batchSize = 10000 if not arguments['--size'] else int(arguments['--size'])

    if arguments['--map']:
        with open(arguments['--map'], 'r') as fp:
            couchDB.setKeyMappingRules( json.load(fp) )
            print("Loaded", len(couchDB.QUEUE_MAPPER), "volumes mapping rules" )


    if arguments['--data']:
        fileNames = getOrderFileList(arguments['--data'])
        iMin = int(arguments['--min']) if arguments['--min'] else 0
        iMax = int(arguments['--max']) if arguments['--max'] else len(fileNames)
        # Offset so that last idnex is added
        iMax += 1
        
        if iMax > len(fileNames):
            iMax = len(fileNames)

        for fName in fileNames[iMin:iMax]:
            if subSetToFix:
                if  fName[0] not in subSetToFix:
                    print(fName[0], " needs no fix")
                    continue
            dataFile = arguments['--data'] + '/' + fName[0]
            c = GenomeData(dataFile)
            print("globing", dataFile, "#items", len(c))
          
            if subSetToFix:
                x =len(c)
                c = c.filter(gDatumMotifMatchLambda)
                print(str(x), "->", str(len(c)), 'Early exiting')
              

            for i in range(0,len(c), batchSize):
                j = i + batchSize if i + batchSize < len(c) else len(c)
                #print(i,':',j)
                d = c[i:j]
                r = None
                if arguments['--map']:
                    r = couchDB.volDocAdd(d)
                else:
                    r = couchDB.bulkDocAdd(d, target=arguments['<databaseName>'])
                
                for rDatum in r:
                    if not 'ok' in rDatum:
                        print ("Error here ==>", str(rDatum))