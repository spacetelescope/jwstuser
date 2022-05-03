from jwstuser import engdb

# Open a user-defined api_token.txt file containing the MAST API token:
api_token = open('api_token.txt','r').readline()[:-1]

EDB = engdb.EngineeringDatabase(mast_api_token = api_token)

# First, get Guide Star X-position over a pre-determined range of time:
mnemonic = 'SA_ZFGGSPOSX'
start = '2022-05-02T06:00:00'
end = '2022-05-02T13:30:00'

cen_x = EDB.timeseries(mnemonic, start, end)

# Now get Y-position:
mnemonic = 'SA_ZFGGSPOSY'

cen_y = EDB.timeseries(mnemonic, start, end)

# Plot both:
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('ticks')

plt.plot(cen_x.time, cen_x.value - np.median(cen_x.value), '.-', label = 'Guide Star Position_X', color = 'cornflowerblue')
plt.plot(cen_y.time, cen_y.value - np.median(cen_y.value), '.-', label = 'Guide Star Position_Y', color = 'tomato')

plt.xticks(fontsize = 16)
plt.yticks(fontsize = 16)

plt.title('FGS Centroid Position on May 2nd, 2022')
plt.xlabel('Time (UTC)', fontsize = 20)
plt.ylabel('Position (wrt to median)', fontsize = 20)
plt.legend(fontsize = 18)

plt.show()

# Now extract to an ascii file (first column time in MJD UTC, second column X-position, third column Y-position):
fout = open('fgs_centroids_hp14_may2-2022.dat', 'w')

#from astropy.time import Time

fout.write('# MJD (UTC; ISIM) \t Centroid X \t Centroid Y\n')
for i in range( len(cen_x.time) ):

    #t = Time( cen_x.time[i].isoformat(), format = 'isot' ) 

    fout.write('{0:.10f} \t {1:.7f} \t {2:.7f}\n'.format( cen_x.time_mjd[i], cen_x.value[i], cen_y.value[i] ))   

fout.close()
