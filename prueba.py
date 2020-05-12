from __future__ import print_function
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

def getDatos (anio, idema):
    headers = {'Authorization': 'Bearer ' + API_TOKEN}
    openDataApiUrlMeteo = "https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/anioini/"+anio+"/aniofin/"+anio+"/estacion/"+idema
    openDataApiUrlEstacion =  "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/2020-05-03T00:00:00UTC/fechafin/2020-05-04T23:59:59UTC/estacion/"+idema

    openDataResponseMeteo = requests.get(openDataApiUrlMeteo, headers=headers)
    openDataResponseMeteo = openDataResponseMeteo.json()
    openDataResponseEstacion = requests.get(openDataApiUrlEstacion, headers=headers)
    openDataResponseEstacion = openDataResponseEstacion.json()

    if (openDataResponseMeteo["estado"] or openDataResponseEstacion["estado"]) == tooManyRequestError:
        raise TooManyRequestException()
    finalDataUrlMeteo = openDataResponseMeteo["datos"]
    finalDataResponseMeteo = requests.get(finalDataUrlMeteo)
    print(finalDataResponseMeteo)
    finalDataMeteo = finalDataResponseMeteo.json()
    registrosMeteo = pd.DataFrame(finalDataMeteo)
    registrosMeteo.drop(
        ['indicativo', 'p_max', 'n_cub', 'hr', 'n_gra', 'n_fog', 'inso', 'q_max', 'nw_55', 'q_mar', 'q_med',
         'tm_min', 'ta_max', 'ts_min', 'nt_30', 'nv_0050', 'n_des', 'w_racha', 'np_100', 'n_nub', 'p_sol', 'nw_91',
         'np_001', 'ta_min', 'e', 'np_300', 'nv_1000', 'evap', 'n_llu', 'n_tor', 'w_med', 'nt_00', 'ti_max',
         'n_nie', 'tm_max', 'nv_0100', 'q_min', 'np_010', 'w_rec', 'ts_20', 'ts_10', 'ts_50','glo'
         ], axis=1, errors='ignore', inplace=True)  # ignoramos errores porque hay columnas que no están presentes en todos los años
    registrosMeteo.drop(12, inplace=True)  # eliminamos fila 13 porque contiene el total anual

    finalDataUrlEstacion = openDataResponseEstacion["datos"]
    finalDataResponseEstacion = requests.get(finalDataUrlEstacion)
    finalDataEstacion = finalDataResponseEstacion.json()
    registrosEstacion = pd.DataFrame(finalDataEstacion)
    registrosEstacion.drop(['fecha', 'nombre', 'altitud', 'tmed', 'prec', 'tmin',
                    'horatmin', 'tmax', 'horatmax', 'dir', 'velmedia', 'racha',
                    'horaracha', 'sol', 'presMax', 'horaPresMax', 'presMin', 'horaPresMin'
                    ], axis=1, errors='ignore', inplace=True)
    registrosEstacion = registrosEstacion.drop([1])
    registrosEstacionAux = pd.DataFrame()
    for i in range(12):
        registrosEstacionAux = registrosEstacionAux.append(registrosEstacion, ignore_index=True)

    registros = pd.concat([registrosEstacionAux, registrosMeteo], axis=1)
    return registros

#PROGRAMA PRINCIPAL
datos = pd.DataFrame()
#Estaciones y años en el estudio
idema_estaciones = ["3195","5783","6156X","5973"]
anio_actual = 2008
pos_estacion = 0
while pos_estacion < len(idema_estaciones):
    while anio_actual < 2020:
        try:
            nuevosDatos = getDatos(str(anio_actual), idema_estaciones[pos_estacion])
            datos = pd.concat([datos, nuevosDatos], ignore_index=True)
            print(datos)
        except TooManyRequestException as error:
            print("TooManyRequest: Minuto de penalizacion AEMET")
            time.sleep(60)
            print("Agotado limite")
            anio_actual = anio_actual - 1
        except json.decoder.JSONDecodeError as error:
            print("JSON Vacio por minuto penalizacion")
            time.sleep(60)
            print("agotado limite por json ")
            anio_actual = anio_actual - 1
        anio_actual = anio_actual + 1
    anio_actual = 2008
    pos_estacion = pos_estacion + 1

datos.to_csv('prueba.csv')
