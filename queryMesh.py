#!/usr/bin/python3

from influxdb import InfluxDBClient
import json
from time import sleep
import re
from textwrap import wrap

def main():
    """Instantiate a connection to the InfluxDB."""
    host = 'IP'
    port = 8086
    user = 'admin'
    password = 'password'
    dbname = 'table'
    query = 'select time, value from device_frmpayload_data_dados where time > now() -5m'

    client = InfluxDBClient(host, port, user, password, dbname)

    print("Querying data: " + query)
    while(1):
        result = client.query(query)

        for point in result.get_points():
            jtopy=json.dumps(point)
            dict_json=json.loads(jtopy)
        
            dados = dict_json["value"]
            dadosInt = json.loads(dados)
        
            time = dict_json["time"]
            node = dadosInt["nd"]
            if ("ts" in dadosInt):
                proxyTimeStamp = dadosInt["ts"]
            else:
                proxyTimeStamp = 0
            if ("id" in dadosInt["dt"]):
                idMesh = str(dadosInt["dt"]["id"])
            else:
                idMesh = str("00")
            if ("sq" in dadosInt["dt"]):
                seq = dadosInt["dt"]["sq"]
            else:
                seq = 0
            if ("ct" in dadosInt["dt"]):
                count = dadosInt["dt"]["ct"]
            else:
                count = 0
            if ( "rt" in dadosInt["dt"]):
                rota = dadosInt["dt"]["rt"]
            else:
                rota = "65"
            rota = re.sub('[^A-Z0-9]+', '', rota)
            if ("tp" in  dadosInt["dt"]):
                temp =  dadosInt["dt"]["tp"]
            else:
                temp = 0
            if ("hd" in  dadosInt["dt"]):
                humi =  dadosInt["dt"]["hd"]
            else:
                humi = 0
            if ("bt" in dadosInt["dt"]):
                bat = dadosInt["dt"]["bt"]
            else:
                bat = 0
            if ("ts" in dadosInt["dt"]):
                nodeTimeStamp = dadosInt["dt"]["ts"]
            else:
                nodeTimeStamp = 0
            if ("nn" in dadosInt["dt"]):
                nearNodes = dadosInt["dt"]["nn"]
            else:
                nearNodes = 0

            data = [{
                    "measurement": "loraWanMeshNodes",
                    "time": time,
                    "tags": {
                        "name": node
                        },
                    "fields": {
                        "idMesh": idMesh,
                        "seq": float(seq),
                        "count": float(count),
                        "rota" : rota,
                        "temp": float(temp),
                        "humi": float(humi),
                        "bat": float(bat),
                        "proxyTS": float(proxyTimeStamp),
                        "nodeTS": float(nodeTimeStamp),
                        "nearNodes": float(nearNodes),
                        }
                    }]
            print(data)

            status = client.write_points(data)
            print('Status: ' + str(status))
            client.close()
            print()
        
        sleep(10.00)

if __name__ == '__main__':
    main()
