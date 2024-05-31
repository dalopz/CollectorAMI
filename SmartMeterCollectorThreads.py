#-----------------------------------------------------------------------------------------------------------

# Codigo que realiza la conexión a los AMI, la lectura de los datos y el envío de las variables
# para cada medidor directamente a CrateDB


#-----------------------------------------------------------------------------------------------------------
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import traceback
import time
import schedule
import requests
from gurux_serial import GXSerial
from gurux_net import GXNet
from gurux_dlms.enums import ObjectType
from gurux_dlms.objects.GXDLMSObjectCollection import GXDLMSObjectCollection
from GXSettings import GXSettings
from GXDLMSReader import GXDLMSReader


class sampleclient():
    @classmethod
    def main(cls, args):
        print("Inicializando conexión...")
        # Lista de medidores con su IP y nombre de entidad
        meters = [
            {"ip": "10.60.28.21", "name": "SM_B10_ARQ"},
            {"ip": "10.60.60.21", "name": "SM_B9_SFA1"},
            {"ip": "10.60.60.22", "name": "SM_B9_SFA2"},
            {"ip": "10.60.24.21", "name": "SM_B8_CPA"},
            {"ip": "10.60.24.22", "name": "SM_B8_AA"},
            {"ip": "10.60.24.23", "name": "SM_B8_LABS"},
            {"ip": "10.60.24.24", "name": "SM_ECOVILLA"},
            {"ip": "10.60.20.21", "name": "SM_B7_TAC"},
            {"ip": "10.60.20.22", "name": "SM_B7_CTIC"},
            {"ip": "10.60.8.21", "name": "SM_B4_PRIM"},
            {"ip": "10.60.12.21", "name": "SM_B5_BACH"},
            {"ip": "10.60.36.21", "name": "SM_B12_DERE"},
            {"ip": "10.60.63.21", "name": "SM_B18_PARQ"},
            {"ip": "10.60.52.21", "name": "SM_B17_POLI"},
            {"ip": "10.60.48.21", "name": "SM_B15_BIBL"},
            {"ip": "10.60.4.21", "name": "SM_B3_RECT"}
        ]
        
        # Funcion para ejecutar cada 30 segundos la lectura de los datos por medidor
        def job(meter):
            reader = None
            settings = GXSettings()
            try:
                # Actualizar los argumentos con la IP del medidor
                argumentos = ['main.py', '-r', 'sn', '-c', '32', '-s', '1', '-h', meter["ip"], '-p', '4059', '-P', '00000000', '-a', 'Low']
                ret = settings.getParameters(argumentos)
                if ret != 0:
                    return

                # Initialize connection settings.
                if not isinstance(settings.media, (GXSerial, GXNet)):
                    raise Exception("Unknown media type.")

                reader = GXDLMSReader(settings.client, settings.media, settings.trace, settings.invocationCounter)
                settings.media.open()
                
                if settings.readObjects:
                    read = False
                    reader.initializeConnection()

                    if settings.outputFile and os.path.exists(settings.outputFile):
                        try:
                            c = GXDLMSObjectCollection.load(settings.outputFile)
                            settings.client.objects.extend(c)
                            if settings.client.objects:
                                read = True
                        except Exception:
                            read = False
                    if not read:
                        reader.getAssociationView()
                    for k, v in settings.readObjects:
                        obj = settings.client.objects.findByLN(ObjectType.NONE, k)
                        if obj is None:
                            raise Exception("Unknown logical name:" + k)
                        val = reader.read(obj, v)
                        reader.showValue(v, val)
                    if settings.outputFile:
                        settings.client.objects.save(settings.outputFile)
                else:
                    reader.initialize_get_value_by_obis_code(settings.outputFile)

                    # Generar el payload
                    patch_payload = {
                        "activeenergyexport":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:2.8.0")
                        },
                        "activeenergyexportday":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:2.9.2")
                        },
                        "activeenergyimport":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:1.8.0")
                        },
                        "activeenergyimportday":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:1.9.2")
                        },
                        "activepower":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:16.7.0")
                        },
                        "activepower1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:36.7.0")
                        },
                        "activepower2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:56.7.0")
                        },
                        "activepower3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:76.7.0")
                        },
                        "frecuency":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:14.7.0")
                        },
                        "harmonicsi1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:31.7.126")
                        },
                        "harmonicsi2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:51.7.126")
                        },
                        "harmonicsi3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:71.7.126")
                        },
                        "harmonicsv1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:32.7.126")
                        },
                        "harmonicsv2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:52.7.126")
                        },
                        "harmonicsv3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:72.7.126")
                        },
                        "i1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:31.7.0")
                        },
                        "i1angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.4")
                        },
                        "i2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:51.7.0")
                        },
                        "i2angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.5")
                        },
                        "i3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:71.7.0")
                        },
                        "i3angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.6")
                        },
                        "in":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:91.7.0")
                        },
                        "powerfactor1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:33.7.0")
                        },
                        "powerfactor2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:53.7.0")
                        },
                        "powerfactor3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:73.7.0")
                        },
                        "reactiveenergyexport":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:4.8.0")
                        },
                        "reactiveenergyexportday":{
                            "type": "Number",
                            "value": 0 # Obiscode no sirve
                        },
                        "reactiveenergyimport":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:3.8.0")
                        },
                        "reactiveenergyimportday":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:3.9.2")
                        },
                        "reactivepower":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:131.7.0")
                        },
                        "reactivepower1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:151.7.0")
                        },
                        "reactivepower2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:171.7.0")
                        },
                        "reactivepower3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-4:191.7.0")
                        },
                        "relativethdcurrent":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:11.7.127")
                        },
                        "relativethdpower":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:15.7.127")
                        },
                        "relativethdvoltage":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-0:12.7.127")
                        },
                        "totalpowerfactor":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:13.7.0")
                        },
                        "v1":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:32.7.0")
                        },
                        "v1angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.0")
                        },
                        "v1_ps":{
                            "type": "Number",
                            "value": 0 # No existe obiscode en el registro de excel
                        },
                        "v2":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:52.7.0")
                        },
                        "v2angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.1")
                        },
                        "v2_ps":{
                            "type": "Number",
                            "value": 0 # No existe obiscode en el registro de excel
                        },
                        "v3":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:72.7.0")
                        },
                        "v3angle":{
                            "type": "Number",
                            "value": reader.get_value_by_obis_code("1-1:81.7.2")
                        },
                        "v3_ps":{
                            "type": "Number",
                            "value": 0 # No existe obiscode en el registro de excel
                        }
                    }

                    target_url = f"http://localhost:1026/v2/entities/{meter['name']}/attrs"
                    headers = {"Content-Type": "application/json"}
                    response = requests.patch(target_url, headers=headers, json=patch_payload)
                    print(f"Enviando variables a {target_url}- codigo respuesta: {response.status_code}")

            except Exception as e:
                print(f"Error leyendo medidor {meter['name']} at {meter['ip']}: {str(e)}")
                traceback.print_exc()
            finally:
                if reader:
                    reader.close()
                if settings.media:
                    settings.media.close()

        # Programar la lectura de cada medidor
        with ThreadPoolExecutor(max_workers=len(meters)) as executor:
            while True:
                # Ejecutar los trabajos
                futures = [executor.submit(job, meter) for meter in meters]

                # Esperar a que todos los trabajos se completen
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Job generated an exception: {e}")

                # Esperar 30 segundos antes de ejecutar nuevamente los trabajos
                print("Esperando 30 segundos para conectar nuevamente a los medidores")
                time.sleep(30)

if __name__ == "__main__":
    sampleclient.main(sys.argv)
