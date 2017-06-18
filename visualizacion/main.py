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
   

def selectPlot(country, tone_val, val, dict_list):
    dict1 = dict_list[0]
    dict2 = dict_list[1]
    dict3 = dict_list[2]
    dict4 = dict_list[3]
    tone_val = float(tone_val)
    
    if(tone_val >= 1.0 and tone_val < 1.25):
        try:
            dict1[country] += val
        except(KeyError):
            dict1[country] = val
    elif(tone_val >= 1.25 and tone_val < 1.5):
        try:
            dict2[country] += val
        except(KeyError):
            dict2[country] = val
    elif(tone_val >= 1.5 and tone_val < 1.75):
        try:
            dict3[country] += val
        except(KeyError):
            dict3[country] = val
    elif(tone_val >= 1.75 and tone_val <= 2.0):
        try:
            dict4[country] += val
        except(KeyError):
            dict4[country] = val
    return [dict1, dict2, dict3, dict4]

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
    
    out_files = []
    for i in range(4):
        out_files.append(open(out_path + "_" + str(i) + ".csv", "w"))
        out_files[i].write("CODE_COUNTRY" + regex + "VAL")
    
    country_dict_list = [{}, {}, {}, {}]
    
    
    line = in_file.readline()
    while(line != ""):
        (country, tone, val) = line.strip().split(",")
        if(country != "("):
            country = country[1:]
            country_dict_list = selectPlot(country, tone, val[:-1], country_dict_list)
        line = in_file.readline()
    i = 0
    for country_dict in country_dict_list:    
        for country in country_dict.keys():
            try:
                out_files[i].write("\n" + str(country_code[country]) + regex 
                           + str(country_dict[country]))
            except(KeyError):
                print("Country code not in DB: " + str(country))
        i += 1
        

IN_PATH = "/home/lucas/git/Patos_Project/big_results2/"
state_geo = r'/home/lucas/git/Patos_Project/utils/countries_light.json'
colors = ["YlGn", "BuPu", "YlOrRd", "YlGnBu"]

for elem in ["avg", "best5", "worst5"]:
    file2csv(IN_PATH + elem, "/part-00000", IN_PATH + elem + "/" + elem, regex = ",")
    for i in range(4):
        map_data = pd.read_csv(IN_PATH + elem + "/" + elem + "_" + str(i) + ".csv")
        mapa = folium.Map(location=[48, -102], zoom_start=3)
        mapa.choropleth(geo_path=state_geo, data=map_data,
                    columns=["CODE_COUNTRY", "VAL"],
                    key_on='feature.id',
                    fill_color=colors[i], fill_opacity=0.7, line_opacity=0.2,
                    legend_name='Visualizacion del impacto de las noticias a nivel mundial')
        mapa.save("/home/lucas/git/Patos_Project/visualizacion/" + elem + "/_" + str(i) + '.html')

    