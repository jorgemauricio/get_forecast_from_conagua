# /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#    Script que permite insertar el pronóstico de CONAGUA a una base de datos
#    Author: Jorge Mauricio
#    Email: jorge.ernesto.mauricio@gmail.com
#    Date: Created on Thu Sep 28 08:38:15 2017
#    Version: 1.0
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""

# librerías
import pandas as pd
import numpy as np
import time
from time import gmtime, strftime
import pyodbc
import os
import ftplib
import math
from access import server, database, usr, pwd

def main():

    # día actual
    today = strftime("%Y-%m-%d")
    #today = "2019-01-29"

    """

    """
    # leer el archivo a insertar
    df = pd.read_json("/home/jorge/Documents/Research/get_forecast_from_conagua/data/{}/DailyForecast_MX".format(today))

    """
    # # # # # # # # # # # # # # # # # # #
    #              DATA TO SQL          #
    # # # # # # # # # # # # # # # # # # #
    """

    # datos de la conexión
    conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.3.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+usr+';PWD='+ pwd)
    cursor =  conn.cursor()

    for index, row in df.iterrows():
        """
        Columns:
        cc,desciel,dh,dirvienc,dirvieng,dloc,ides,idmun,lat,lon,ndia,nes,
            nmun,prec,probprec,raf,tmax,tmin,velvien
        """

        CC       = row["cc"]
        DESCIEL  = row["desciel"]
        DH       = row["dh"]
        DIRVIENC = row["dirvienc"]
        DIRVIENG = row["dirvieng"]
        DLOC     = row["dloc"]
        IDES     = row["ides"]
        IDMUN    = row["idmun"]
        LAT      = row["lat"]
        LON      = row["lon"]
        NDIA     = row["ndia"]
        NES      = row["nes"]
        NMUN     = row["nmun"]
        PREC     = row["prec"]
        PROBPREC = row["probprec"]
        RAF      = row["raf"]
        TMAX     = row["tmax"]
        TMIN     = row["tmin"]
        VELVIEN  = row["velvien"]

        # generar query
        query = """INSERT INTO Pronostico (cc, desciel, dh, dirvienc, dirvieng,
                                        dloc, ides, idmun, lat, lon, ndia, nes,
                                        nmun, prec, probprec, raf, tmax, tmin,
                                        velvien)
                                        VALUES ((?), (?), (?), (?), (?), (?),
                                        (?), (?), (?), (?), (?), (?), (?), (?),
                                        (?), (?), (?), (?), (?))"""

        # ejecutar insert
        print(query)
        cursor.execute(query, (CC, DESCIEL, DH, DIRVIENC, DIRVIENG, DLOC, IDES,
                              IDMUN, LAT, LON, NDIA, NES, NMUN, PREC, PROBPREC,
                              RAF, TMAX, TMIN, VELVIEN))
        cursor.commit()

    conn.close()
    print("OK...")

if __name__ == '__main__':
    main()
