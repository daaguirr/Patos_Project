import zipfile
import urllib.request
import os
import shutil
import sys
import gzip
import time

dir_path = os.path.dirname(os.path.realpath(__file__))
download_path = dir_path + "/download/"
output_path = dir_path + "/output/"

# IMPORTANTE PYTHON 3.X
if __name__ == '__main__':
    # Lectura
    args = sys.argv[1:]
    if len(args) != 2:
        print ('La cantidad de argumentos es 2 usted ingreso ' + str(len(args)))
        print ('uso <file> <max_lines>')
        sys.exit(1)
    if os.path.exists(download_path):
        shutil.rmtree(download_path)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    max_files = int(args[1])
    os.makedirs(download_path)
    os.makedirs(output_path)

    num_lines_tmp = sum(1 for line in open(args[0]))
    num_lines = min(num_lines_tmp, max_files)

    fileinput = open(args[0], 'r')
    start = time.time()
    current_size = 0
    for i, url in enumerate(fileinput):
        if i >= max_files:
            break
        print(url[:-1])
        urllib.request.urlretrieve(url[:-1], download_path + url.split('/')[-1][:-1])
        zip_ref = zipfile.ZipFile(download_path + url.split('/')[-1][:-1], 'r')
        zip_ref.extractall(output_path)
        zip_ref.close()
        print ("Download: " + "{0:.2f}".format(100.0 * (i + 1) / num_lines) + "%")

    shutil.rmtree(download_path)
    fileinput.close()

    files = [f for f in os.listdir(output_path)]
    fileoutput = gzip.open("output.tsv.gz", "w+")
    for i, f in enumerate(files):
        if i >= max_files:
            break
        csv = open(output_path + f, "r+")
        next(csv)
        for line in csv:
            fileoutput.write(line.encode('utf-8'))
        csv.close()
        print ("Merge: " + "{0:.2f}".format(100.0 * (i + 1) / min(len(files), max_files)) + "%")
    fileoutput.close()
    print ("listo")
