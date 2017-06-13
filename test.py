import folium
import pandas as pd
# import reverse_geocode

state_geo = r'countries_light.json'
state_unemployment = r'test.csv'

state_data = pd.read_csv(state_unemployment)

# Let Folium determine the scale
mapa = folium.Map(location=[48, -102], zoom_start=3)
mapa.choropleth(geo_path=state_geo, data=state_data,
                columns=['State', 'Unemployment'],
                key_on='feature.id',
                fill_color='YlGn', fill_opacity=0.7, line_opacity=0.2,
                legend_name='Unemployment Rate (%)')
mapa.save('us_states.html')


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
