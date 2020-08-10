# /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
####################################### # # # # # # # # # # # #
# Script que permite insertar el pronóstico de CONAGUA a una base de datos
# Author: Jorge Mauricio
# Email: jorge.ernesto.mauricio@gmail.com
# Date: Created on Thu Sep 28 08:38:15 2017
# Version: 1.0
####################################### # # # # # # # # # # # #
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
    df = pd.read_json("data/{}/DailyForecast_MX".format(today))

    """
    # # # # # # # # # # # # # # # # # # #
    #              DATA TO SQL
    # # # # # # # # # # # # # # # # # # #
    """

    # datos de la conexión
    conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.3.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+usr+';PWD='+ pwd)
    cursor =  conn.cursor()

    for index, row in df.iterrows():
        "cc,desciel,dh,dirvienc,dirvieng,dloc,ides,idmun,lat,lon,ndia,nes,nmun,prec,probprec,raf,tmax,tmin,velvien"

        CC       = row["cc"]
        DESCIEL  = row["desciel""]
        DH       = row["graupel"]
        DIRVIENC = row["hail"]
        DIRVIENG = row["rain"]
        DLOC     = row["tmax"]
        IDES     = row["tmin"]
        IDMUN    = row["temp"]
        LAT      = row["dewpoint"]
        LON      = row["rh"]
        NDIA     = row["u"]
        NES      = row["v"]
        NMUN     = row["rhmin"]
        PREC     = row["rhmax"]
        PROBPREC = row["tsoil010"]
        RAF      = row["tsoil1040"]
        TMAX     = row["soilw010"]
        TMIN     = row["soilw1040"]
        VELVIEN  = row["gust"]


        # generar query
        query = "INSERT INTO Pronostico (lats,lons,graupel,hail,rain,tmax,tmin,temp,dewpoint,rh,u,v,rhmin,rhmax,tsoil010,tsoil1040,soilw010,soilw1040,gust,epot,refmax,srh3000,Fecha,Pronostico, cprec, cvelv, ctmax, ctmin) VALUES ((?),(?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?), (?))"
        # ejecutar insert
        print(query)
        cursor.execute(query, (LATS, LONS, GRAUPEL, HAIL, RAIN, TMAX, TMIN, TEMP, DEWPOINT, RH, U,V, RHMIN, RHMAX, TSOIL010, TSOIL1040, SOILW010, SOILW1040, GUST, EPOT, REFMAX, SRH3000, FECHA, PRONOSTICO, CPREC, CVELV, CTMAX, CTMIN))
        cursor.commit()

    conn.close()
    print("OK...")



if __name__ == '__main__':
    main()
