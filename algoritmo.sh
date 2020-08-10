#!/bin/bash

# today
today="$(date '+%Y-%m-%d')"

# create directory
mkdir /home/jorge/Documents/Research/get_forecast_from_conagua/data/$today

# change directory
cd /home/jorge/Documents/Research/get_forecast_from_conagua/data/$today

# download daily values
wget ftp://ftp.conagua.gob.mx/pronosticoporciudades/DailyForecast_MX.gz

# download hourly values
wget ftp://ftp.conagua.gob.mx/pronosticoporciudadeshoraria/HourlyForecast_MX.gz

# gunzip daily values
gunzip DailyForecast_MX.gz

# unzip hourly values
gunzip HourlyForecast_MX.gz

# execute algoritmo.py
/home/jorge/anaconda3/bin/python3.7 /home/jorge/Documents/Research/get_forecast_from_conagua/algoritmo.py
