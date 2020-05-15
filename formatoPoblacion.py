import pandas as pd
import csv

# data = pd.read_csv("Poblacion ProvinciaAnioMes.csv")
# #Establecemos nivel de mes aunque no lo tengamos
# data = data.loc[data.index.repeat(12)].reset_index(drop = True)
# data.to_csv("PoblacionFormateada.csv")
dataGeneral = pd.DataFrame()
dataPoblacion = pd.read_csv("PoblacionFormateada.csv")
dataClima = pd.read_csv("prueba.csv")
dataLlamadas016 = pd.read_csv("Llamadas016ProvAnoMes.csv")

dataGeneral = dataClima
dataGeneral["pob"] = dataPoblacion.pob
dataGeneral["llamadas016"] = dataLlamadas016.llamadas
dataGeneral.drop([0,1,2,3])
dataGeneral["llam/hab"] = dataGeneral.llamadas016 / dataGeneral.pob
dataClima.to_csv("DataGeneralLlamadas.csv", index=False)