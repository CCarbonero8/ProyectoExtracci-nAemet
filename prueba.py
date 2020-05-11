from __future__ import print_function
import swagger_client
from swagger_client.rest import ApiException
import requests
import time
import json
import pandas as pd

#Codigo para evitar ssl error: dh key too small
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += 'HIGH:!DH:!aNULL'
try:
    requests.packages.urllib3.contrib.pyopenssl.DEFAULT_SSL_CIPHER_LIST += 'HIGH:!DH:!aNULL'
except AttributeError:
    # no pyopenssl support used / needed / available
    pass

#Obtener api_key encriptada
api_key = json.load(open('api_key.json'))

API_TOKEN = api_key['api_key']
tooManyRequestError = 429

class TooManyRequestException(Exception):
    pass

def getDatosMeteo (anio, idema):
    headers = {'Authorization': 'Bearer ' + API_TOKEN}
    openDataApiUrl = "https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/anioini/"+anio+"/aniofin/"+anio+"/estacion/"+idema

    openDataResponse = requests.get(openDataApiUrl, headers=headers)
    openDataResponse = openDataResponse.json()

    if openDataResponse["estado"] == tooManyRequestError:
        raise TooManyRequestException()

    finalDataUrl = openDataResponse["datos"]
    finalDataResponse = requests.get(finalDataUrl)
    finalData = finalDataResponse.json()
    registros = pd.DataFrame(finalData)
    registros.drop(
        ['indicativo', 'p_max', 'n_cub', 'hr', 'n_gra', 'n_fog', 'inso', 'q_max', 'nw_55', 'q_mar', 'q_med',
         'tm_min', 'ta_max', 'ts_min', 'nt_30', 'nv_0050', 'n_des', 'w_racha', 'np_100', 'n_nub', 'p_sol', 'nw_91',
         'np_001', 'ta_min', 'e', 'np_300', 'nv_1000', 'evap', 'n_llu', 'n_tor', 'w_med', 'nt_00', 'ti_max',
         'n_nie', 'tm_max', 'nv_0100', 'q_min', 'np_010', 'w_rec', 'ts_20', 'ts_10', 'ts_50','glo'
         ], axis=1, errors='ignore', inplace=True)  # ignoramos errores porque hay columnas que no están presentes en todos los años
    registros.drop(12, inplace=True)  # eliminamos fila 13 porque contiene el total anual
    return registros
def getDatosEstacion (idema):
    headers = {'Authorization': 'Bearer ' + API_TOKEN}
    # usamos la siguiente url porque contiene datos de la estacion por idema
    openDataApiUrl = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/2020-05-03T00:00:00UTC/fechafin/2020-05-04T23:59:59UTC/estacion/"+idema
    openDataResponse = requests.get(openDataApiUrl, headers=headers)
    openDataResponse = openDataResponse.json()

    if openDataResponse["estado"] == tooManyRequestError:
        raise TooManyRequestException()

    finalDataUrl = openDataResponse["datos"]
    finalDataResponse = requests.get(finalDataUrl)
    finalData = finalDataResponse.json()
    registros = pd.DataFrame(finalData)
    registros.drop(['fecha','nombre','altitud','tmed','prec','tmin',
                   'horatmin','tmax','horatmax','dir','velmedia','racha',
                   'horaracha','sol','presMax','horaPresMax','presMin','horaPresMin'
                   ], axis=1, errors='ignore', inplace=True)
    registros = registros.drop([1])
    registros_aux = pd.DataFrame()
    for i in range(12):
        registros_aux = registros_aux.append(registros, ignore_index=True)
    return registros_aux

#PROGRAMA PRINCIPAL
datosMeteo = pd.DataFrame()
datosEstaciones = pd.DataFrame()
#Estaciones y años en el estudio
idema_estaciones = ["3195","5783"]
rango_años = range(2008,2019)
for i in range(len(idema_estaciones)):
    for anio_actual in rango_años:
        try:
            nuevosDatosMeteo = getDatosMeteo(str(anio_actual), idema_estaciones[i])
            datosMeteo = pd.concat([datosMeteo, nuevosDatosMeteo], ignore_index=True)
            nuevosDatosEstaciones = getDatosEstacion(idema_estaciones[i])
            datosEstaciones = pd.concat([datosEstaciones, nuevosDatosEstaciones], ignore_index=True)

        except TooManyRequestException as error:
            while time.sleep(10)
            print("Agotado limite")
datosFinales = pd.concat([datosEstaciones, datosMeteo], axis=1)
print(datosFinales)
datosFinales.to_csv('prueba.csv')
