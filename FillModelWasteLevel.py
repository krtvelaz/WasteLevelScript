import shutil as s
import pandas as pd
import json as js
from datetime import datetime, timezone
import requests as rq
import os
import pytz
from urllib3.exceptions import InsecureRequestWarning
rq.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Ubicacion de los Archivos de configuración
path = os.getcwd()+"/"

# Variables de Aumento
AumentoNormal = 0.003571429
AumentoAfan = 0.004464286
AumentoNormalNoc = 0.00125
AumentoAfanNoc = 0.0025
Momento = datetime.now(pytz.timezone("America/Bogota"))

# funcion para autenticarse en Orion


def Auth():
    url = 'https://medellinciudadinteligente.co/keycloak/auth/realms/fiware-server/protocol/openid-connect/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {
        'username': 'admin-user',
        'password': 'admin-user',
        'grant_type': 'password',
        'client_id': 'fiware-login'
    }
    response = rq.post(url, headers=headers, data=data, verify=False)
    return (response.json()['access_token'])


# funcion para alimentar modelo de WasteLevel
def EnviarPost(data: dict, token):
    url = "https://medellinciudadinteligente.co/orion/v2/op/update"
    headers = {
        'fiware-service': 'WasteLevel',
        'fiware-servicepath': '/hopu',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }
    response = rq.request("POST", url, headers=headers,
                          data=js.dumps(data), verify=False)
    return response

# Generar Valor siguiente para alimentar modelo


def ActualizarBinFillingLevel(binFillingLevel, windowStart, windowEnd, WeekDay1, WeekDay2):
    hoy = Momento
    Hora = hoy.hour
    Dia = hoy.isoweekday()
    Hora_TF = (Hora) >= windowStart and (Hora) < windowEnd
    Dia_TF = (Dia) == WeekDay1 and (Dia) == WeekDay2
    Aumento = 0
    if(Dia+1 == WeekDay1 or Dia+1 == WeekDay2):
        if (Hora > 17 and Hora >= 23) or (Hora >= 0 and Hora < 4):
            Aumento = AumentoAfanNoc
        else:
            Aumento = AumentoAfan
    else:
        if (Hora > 17 and Hora >= 23) or (Hora >= 0 and Hora < 4):
            Aumento = AumentoNormalNoc
        else:
            Aumento = AumentoNormal

    binFillingLevel = 0 if(Hora_TF and Dia_TF) else binFillingLevel+Aumento

    return binFillingLevel


# Formatear el Payload para enviar a Orion


def ActualizarArchivo(registro):
    ArchivoJson = open(path+'entities_waste.json')
    requestJson = js.load(ArchivoJson)
    ArchivoJson.close()

    requestJson['entities'][0]['id'] = "urn:ngsi:WasteContainer:"+registro.id
    requestJson['entities'][0]['binFillingLevel']['value'] = registro.binFillingLevel
    requestJson['entities'][0]['binFillingLevel']['metadata']['dateObserved']['value'] = registro.LastModifiedDate
    requestJson['entities'][0]['location']['value']['coordinates'][0] = registro.Longitud
    requestJson['entities'][0]['location']['value']['coordinates'][1] = registro.Latitud
    requestJson['entities'][0]['TimeInstant']['value'] = registro.LastModifiedDate
    requestJson['entities'][0]['time_index']['value'] = registro.LastModifiedDate

    return requestJson


# Reiniciar el valor de los dispostivos
def ReiniciarDispositivos():
    config = pd.read_json(path+"deviceWasteConfig.json")
    config.binFillingLevel = 0
    config.LastModifiedDate =  Momento.isoformat()[:-9]+'Z'
    config.to_json(path+"deviceWasteConfig.json", orient='records')


# Iniciar proceso de generación y envío de datos al Orion
def IniciarDispositivos():

    try:
        config = pd.read_json(path+"deviceWasteConfig.json")
        config.binFillingLevel = config.apply(lambda x: ActualizarBinFillingLevel(
            x.binFillingLevel, x.windowStart, x.windowEnd, x.WeekDay1, x.WeekDay2), axis=1)
        config.LastModifiedDate = Momento.isoformat()[:-9]+'Z'

        TOKEN = Auth()
        status_table = []
        status_code = 0
        print('cargando...')
        for registro in config.itertuples():
            datos = ActualizarArchivo(registro)
            respuesta = EnviarPost(datos, TOKEN)
            status_code = respuesta
            if(status_code != 204):
                TOKEN = Auth()
                respuesta = EnviarPost(datos, TOKEN)
                status_code = respuesta
                with open("Logs.txt", "a") as file:
                    file.write("{'id':" + str(registro.id) + ", 'status': " + str(status_code.status_code) + ", 'content': " + str(
                        status_code.content) + ",'lastModifiedDate': " + str(registro.LastModifiedDate) + " } \n")

            status_table.append(
                {'id': registro.id, 'status': status_code.status_code, 'content': status_code.content, 'lastModifiedDate': registro.LastModifiedDate})

            config.to_json(path+"deviceWasteConfig.json", orient='records')
    except:
        with open("Logs.txt", "a") as file:
            file.write(Momento+" {No se pudo Continuar con el proceso } \n")

