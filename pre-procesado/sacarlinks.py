import sys

if __name__ == '__main__':
    # Lectura
    args = sys.argv[1:]
    if len(args) != 1:
        print 'La cantidad de argumentos es 1 usted ingreso ' + str(len(args))
        sys.exit(1)
    fileinput = open(args[0], 'r')
    fileoutput = open('output_filtrados.txt', 'w')
    for line in fileinput:
        if line.find("export") != -1:
            fileoutput.write(line.split(" ")[2])

    print 'listo'
