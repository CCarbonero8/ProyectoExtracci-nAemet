from __future__ import print_function
import time
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint
import requests
import pandas as pd
import json


#Obtener api_key encriptada
api_key = json.load(open('api_key.json'))

# Configure API key authorization: api_key
configuration = swagger_client.Configuration()
configuration.api_key['api_key'] = api_key['api_key']

# create an instance of the API class
api_instance = swagger_client.ValoresClimatologicosApi(swagger_client.ApiClient(configuration))

#Estaciones y años en el estudio
idema_estaciones = ["3195","5783"]
desc_estaciones = ["MADRID","SEVILLA"]
rango_años = range(2008,2019)

# FUNCIONES SECUNDARIAS
# Obtener lista desc_estaciones
def getDescEstaciones(posicion):
    lista_desc_estaciones = []
    for i in range(12):
        lista_desc_estaciones.append(desc_estaciones[posicion])
    return(lista_desc_estaciones)

# Obtener lista idema_estaciones
def getIdemaEstaciones(posicion ):
    lista_idema_estaciones = []
    for i in range(12):
        lista_idema_estaciones.append(idema_estaciones[posicion])
    return (lista_idema_estaciones)

# PROGRAMA PRINCIPAL
for i in range(len(idema_estaciones)):
    for anio_actual in rango_años:
        anio_ini_str = str(anio_actual) # str | Año Inicial (AAAA)
        anio_fin_str = str(anio_actual) # str | Año Final (AAAA) | DEBE COINCIDIR CON AÑO INICIAL (ERROR DE AEMET)
        idema = idema_estaciones[i] # str | Indicativo climatológico de la Estación MA

        try:
            # Climatologías mensuales anuales.
            api_response_clima = api_instance.climatologas_mensuales_anuales_(anio_ini_str, anio_fin_str, idema)
        except ApiException as e:
            print("Exception when calling ValoresClimatologicosApi->climatologas_mensuales_anuales_: %s\n" % e)

        r = requests.get(api_response_clima.datos)
        data = r.json()
        if anio_actual == 2008 and i == 0:
            registros = pd.DataFrame(data)
            registros = registros.drop(
                ['indicativo', 'p_max', 'n_cub', 'hr', 'n_gra', 'n_fog', 'inso', 'q_max', 'nw_55', 'q_mar', 'q_med',
                 'tm_min', 'ta_max', 'ts_min', 'nt_30', 'nv_0050', 'n_des', 'w_racha', 'np_100', 'n_nub', 'p_sol', 'nw_91',
                 'np_001', 'ta_min', 'e', 'np_300', 'nv_1000', 'evap', 'n_llu', 'n_tor', 'w_med', 'nt_00', 'ti_max',
                 'n_nie', 'tm_max', 'nv_0100', 'q_min', 'np_010', 'w_rec', 'ts_20', 'ts_10', 'ts_50'
                 ], axis=1, errors='ignore') #ignoramos errores porque hay columnas que no están presentes en todos los años
            registros.drop(12, inplace=True)# eliminamos fila 13 porque contiene el total anual
            registros['idema']= getIdemaEstaciones(i) # añadimos columna provincia
            registros['provincia']= getDescEstaciones(i) # añadimos columna provincia
        else:
            nuevos_registros = pd.DataFrame(data)
            nuevos_registros = nuevos_registros.drop(
                ['indicativo', 'p_max', 'n_cub', 'glo', 'hr', 'n_gra', 'n_fog', 'inso', 'q_max', 'nw_55', 'q_mar', 'q_med',
                 'tm_min', 'ta_max', 'ts_min', 'nt_30', 'nv_0050', 'n_des', 'w_racha', 'np_100', 'n_nub', 'p_sol', 'nw_91',
                 'np_001', 'ta_min', 'e', 'np_300', 'nv_1000', 'evap', 'n_llu', 'n_tor', 'w_med', 'nt_00', 'ti_max',
                 'n_nie', 'tm_max', 'nv_0100', 'q_min', 'np_010', 'w_rec', 'ts_20', 'ts_10', 'ts_50'
                 ], axis=1, errors='ignore'
                                   '') #ignoramos errores porque hay columnas que no están presentes en todos los años
            nuevos_registros.drop(12, inplace=True)# eliminamos fila 13 porque contiene el total anual
            nuevos_registros['idema'] = getIdemaEstaciones(i)
            nuevos_registros['provincia'] = getDescEstaciones(i)
            registros = pd.concat([registros, nuevos_registros], ignore_index=True)
# Ordenamos columnas
registros = registros[['fecha','idema','provincia','p_mes','tm_mes']]
print(registros)
registros.to_csv('pruebaClimatologicasMensuales.csv', sep=";")
