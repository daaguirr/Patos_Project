import folium
import pandas as pd
import json
import os
# import reverse_geocode

state_geo = r'utils/countries_light.json'

state_unemployment = r'utils/test.csv'

state_data = pd.read_csv(state_unemployment)

# Let Folium determine the scale
# mapa = folium.Map(location=[48, -102], zoom_start=3)
# mapa.choropleth(geo_path=state_geo, data=state_data,
#                 columns=['State', 'Unemployment'],
#                 key_on='feature.id',
#                 fill_color='YlGn', fill_opacity=0.7, line_opacity=0.2,
#                 legend_name='Unemployment Rate (%)')
# mapa.save('us_states.html')
mapa = folium.Map(location=[48, -102], zoom_start=3)
dir_path = os.path.dirname(os.path.realpath(__file__))
print dir_path[:-14]
filter1 = state_data[state_data.Unemployment > 1]
filter2 = state_data[state_data.Unemployment <= 1]

with open('utils/countries_light.json') as data_file:
    data = json.load(data_file)
bla = {u'type': u'FeatureCollection', u'features':
    filter(lambda x: filter1['State'].tolist().count(x['id']) > 0, data['features'])}
with open(dir_path+'/filter1.json', 'w') as fp:
    json.dump(bla, fp)
bla = {u'type': u'FeatureCollection', u'features':
    filter(lambda x: filter2['State'].tolist().count(x['id']) > 0, data['features'])}
with open(dir_path+'/filter2.json', 'w') as fp:
    json.dump(bla, fp)


mapa.choropleth(geo_path=dir_path+'/filter1.json', data=filter1,
                columns=['State', 'Unemployment'],
                key_on='feature.id',
                fill_color='YlGn', fill_opacity=0.7, line_opacity=0.2,
                legend_name='Unemployment Rate (%)')
mapa.choropleth(geo_path=dir_path+'/filter2.json', data=filter2,
                columns=['State', 'Unemployment'],
                key_on='feature.id',
                fill_color='BuPu', fill_opacity=0.7, line_opacity=0.2,
                legend_name='Unemployment Rate (%)')
mapa.save('test2.html')


os.remove(dir_path+'/filter1.json')
os.remove(dir_path+'/filter2.json')

# Uso de Libreria
# coordinates = (-37.81, 144.96), (31.76, 35.21)
# print reverse_geocode.search(coordinates)
# print reverse_geocode.get((-54, -2))

# hd
# map = folium.Map(location=[48, -102], zoom_start=3)
# map.choropleth(geo_path=state_geo, data=state_data,
#             columns=['State', 'Unemployment'],
#             key_on='feature.properties.ISO_A3',
#             fill_color='YlGn', fill_opacity=0.7, line_opacity=0.2,
#             legend_name='Unemployment Rate (%)')
# map.save('us_states.html')
