import os
import requests
import pandas as pd

# API endpoint URL
api_url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"


params = {
    "table": "ps",
    "format": "json",
    "query": "select pl_name, hostname, pl_orbper, pl_rade, pl_radj, pl_massj, disc_year, disc_telescope, disc_facility, disc_locale, pl_dens, pl_insol, pl_orbeccen, pl_orbincl, pl_eqt, st_teff, st_mass, st_rad, st_lum, st_age from ps",
                  

}

response = requests.request("GET", api_url, params=params)
# print(response.text)
exoplanets_data = response.json()
df = pd.DataFrame(exoplanets_data)
# print(df.size)
outname = 'exoplanet_PS_table_final_load.csv'
outdir = r'C:\Users\vikas\Downloads\codes'
# Create the directory if it doesn't exist
if not os.path.exists(outdir):
    os.mkdir(outdir)

path = os.path.join(outdir, outname)
# print(exoplanets_data)
df.to_csv(path, index = False)

