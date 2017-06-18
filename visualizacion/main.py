#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 17 20:32:55 2017

@author: lucas
"""

'''
Recibimos un archivo part-000000 por cada categoria, es decir best5, worst5 y avg.
Cada linea de los archivos anteriores consisten en:
    (CODIGO_PAIS, TONO_AJUSTADO, VAL_CALCULADO)
    TONO_AJUSTADO = [1.0, 2.0]
    VAL_CALCULADO = [1.0, INF]

Es necesario transformarlos a CSV, donde el CODIGO_PAIS sea el primer valor y VAL_CALCULADO el segundo.
Ademas es necesario separar por el valor del TONO_AJUSTADO, donde:
    V_A \in [1.0, 1.25[ entonces se pinta de color ROJO
    V_A \in [1.25, 1.5[ entonces se pinta de color AMARILLO
    V_A \in [1.5, 1.75[ entonces se pinta de color VERDE
    V_A \in [1.75, 2.0[ entonces se pinta de color AZUL
    
Tomamos el archivo, lo transformamos a CSV, creando una columna por valor.
Luego leemos el CSV y se lo entregamos a folium para obtener las visualizaciones.
'''

import pandas as pd
import folium
import pycountry

country_code = {}
for country in pycountry.countries:
    country_code[country.alpha_2] = country.alpha_3
   


'''
file2csv transforms a file into a CSV document with a "\t" regex by default.
file_dir, or file directory
file_name, file name
out_path, the path where the output will be saved
regex, value to use as splitter for CSV.
'''
def file2csv(file_dir, file_name, out_path, regex = "\t"):
    print("Converting:\n" + file_dir + file_name + "\nOUT:\n" + out_path)
    in_file = open(file_dir + file_name, "r")
    
    out_file = open(out_path, "w")
    out_file.write("CODE_COUNTRY" + regex + "ADJ_TONE" + regex + "VAL")
    
    country_dict = {}
    
    line = in_file.readline()
    while(line != ""):
        (country, tone, val) = line.strip().split(",")
        if(country != "("):
            country = country[1:]
            try:
                country_dict[country][0] += float(tone)
                country_dict[country][1] += float(val[:-1])
            except(KeyError):
                country_dict[country] = (float(tone), float(val[:-1]))
        line = in_file.readline()
    for country in country_dict.keys():
        try:
            out_file.write("\n" + str(country_code[country]) + regex 
                       + str(country_dict[country][0]) + regex 
                       + str(country_dict[country][1]))
        except(KeyError):
            print("error")
        

IN_PATH = "/home/lucas/git/Patos_Project/big_results2/"
state_geo = r'/home/lucas/git/Patos_Project/utils/countries_light.json'

for elem in ["avg", "best5", "worst5"]:
    file2csv(IN_PATH + elem, "/part-00000", IN_PATH + elem + "/" + elem + ".csv", regex = ",")
    
    map_data = pd.read_csv(IN_PATH + elem + "/" + elem + ".csv")
    mapa = folium.Map(location=[48, -102], zoom_start=3)
    mapa.choropleth(geo_path=state_geo, data=map_data,
                columns=["CODE_COUNTRY", "VAL"],
                key_on='feature.id',
                fill_color='YlGn', fill_opacity=0.7, line_opacity=0.2,
                legend_name='Unemployment Rate (%)')
    mapa.save(elem + '.html')

    