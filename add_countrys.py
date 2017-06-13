import gzip
import io

import reverse_geocode

fileinput = gzip.open('output.tsv.gz', 'r')
fileoutput = gzip.open("output2.tsv.gz", "w+")

ind = 1
for line in io.TextIOWrapper(io.BufferedReader(fileinput), encoding='utf8', errors='ignore'):
    if ind % 100000 == 0:
        print "line :" + str(ind)
    values = line.split('\t')
    coors = (values[40], values[41]), (values[48], values[49]), (values[56], values[57])
    for i in coors:
        try:
            values.append(reverse_geocode.get(i)['country_code'])
        except:
            values.append(u'')

    fileoutput.write('\t'.join(values).encode('utf-8') + '\n')
    ind += 1
print ("finished")
