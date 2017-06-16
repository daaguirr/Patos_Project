import gzip
import io
import os
import sys

import reverse_geocode


# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')


# Restore
def enablePrint():
    sys.stdout = sys.__stdout__


fileinput = gzip.open('data.tsv.gz', 'r')
fileoutput = gzip.open("dataReal.tsv.gz", "w+")

ind = 1
for line in io.TextIOWrapper(io.BufferedReader(fileinput), encoding='utf8', errors='ignore'):
    if ind % 10000 == 0:
        print "line :" + str(ind)
    values = line.split('\t')
    if len(values) < 61:
        pass
    else:
        values[60] = values[60][:-1]
        coors = (values[40], values[41]), (values[48], values[49]), (values[56], values[57])
        blockPrint()
        for i in coors:
            try:
                values.append(reverse_geocode.get(i)['country_code'])
            except:
                values.append(u'')
        enablePrint()
        # print len(values)
        fileoutput.write('\t'.join(values).encode('utf-8') + '\n')
    ind += 1
print ("finished")
