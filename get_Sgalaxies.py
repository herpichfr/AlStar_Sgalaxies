import pandas as pd
import numpy as np
import splusdata
import os
import time
import logging
import threading

conn = splusdata.connect('herpich', 'Schlieck61!')









fields = pd.read_csv('Sgalaxies.csv')

for field in fields['NAME']:
    print('Starting ' + f'{field}')
    aperture = 'aper_6'
    try:
        query_f = f"""SELECT det.ID, det.ra, det.dec, u.u_{aperture}, j0378.j0378_{aperture}, j0395.j0395_{aperture}, j0410.j0410_{aperture}, j0430.j0430_{aperture}, g.g_{aperture}, j0515.j0515_{aperture}, r.r_{aperture}, j0660.j0660_{aperture}, i.i_{aperture}, j0861.j0861_{aperture}, z.z_{aperture}, u.e_u_{aperture}, j0378.e_j0378_{aperture}, j0395.e_j0395_{aperture}, j0410.e_j0410_{aperture}, j0430.e_j0430_{aperture}, g.e_g_{aperture}, j0515.e_j0515_{aperture}, r.e_r_{aperture}, j0660.e_j0660_{aperture}, i.e_i_{aperture}, j0861.e_j0861_{aperture}, z.e_z_{aperture}, pz.PDF_Means, pz.PDF_STDs
                      FROM idr3.detection_image as det 
                      JOIN idr3.u_band as u ON (u.ID = det.ID)
                      JOIN idr3.j0378_band as j0378 ON (j0378.ID = det.ID)
                      JOIN idr3.j0395_band as j0395 ON (j0395.ID = det.ID)
                      JOIN idr3.j0410_band as j0410 ON (j0410.ID = det.ID)
                      JOIN idr3.j0430_band as j0430 ON (j0430.ID = det.ID)
                      JOIN idr3.g_band as g ON (g.ID = det.ID)
                      JOIN idr3.j0515_band as j0515 ON (j0515.ID = det.ID)
                      JOIN idr3.r_band as r ON (r.ID = det.ID)
                      JOIN idr3.j0660_band as j0660 ON (j0660.ID = det.ID)
                      JOIN idr3.i_band as i ON (i.ID = det.ID)
                      JOIN idr3.j0861_band as j0861 ON (j0861.ID = det.ID)
                      JOIN idr3.z_band as z ON (z.ID = det.ID)
                      JOIN idr3_vacs.photoz_pdfs as pz ON (pz.ID = det.ID)
                      WHERE det.field = '{field}'"""

        table = conn.query(query_f)
        print('Got the table!')
        table.write(f'{field}.fits') ## para salvar tabela

        # ## Fazendo o cross-match com o 2MASS, GALEX e o unWISE.
        # os.system(f"""java -jar stilts.jar cdsskymatch in={value.field}.fits cdstable=II/246/out ra=RA dec=DEC radius=1 find=each blocksize=500000 ocmd='delcols "RAJ2000 DEJ2000 errHalfMaj errHalfMin errPosAng Qfl Rfl X MeasureJD"; addcol ID_2MASS "2MASS"; addcol Jmag_ab "Jmag+0.91"; addcol Hmag_ab "Hmag+1.39"; addcol Kmag_ab "Kmag+1.85"; addcol angDist_2MASS "angDist"; delcols "2MASS angDist Jmag Hmag Kmag"' out={value.field}_vac.fits""")

        # os.system(f"""java -jar stilts.jar cdsskymatch in={value.field}_vac.fits cdstable=II/335/galex_ais ra=RA dec=DEC radius=2 find=each blocksize=500000 ocmd='delcols "RAJ2000 DEJ2000 objid phID Cat RAfdeg DEfdeg FUVexp NUVexp GLON GLAT tile img sv r.fov Obs b E(B-V) Sp? chkf FUV.a e_FUV.a NUV.a e_NUV.a FUV.4 e_FUV.4 NUV.4 e_NUV.4 FUV.6 e_FUV.6 NUV.6 e_NUV.6 Fafl Nafl Fexf Nexf Fflux e_Fflux Nflux e_Nflux FXpos FYpos NXpos NYpos Fima Nima Fr Nr nS/G fS/G nell fell nPA e_nPA fPA e_fPA Fnr F3r Nar Narms Nbrms Far Farms Fbrms w_NUV w_FUV Prob Sep Nerr Ferr Ierr Nperr Fperr CV G N primid groupid Gd Nd primidd groupidd grouptot OName Size"; addcol ID_GALEX "name"; addcol angDist_GALEX "angDist"; delcols "name angDist"' out={value.field}_vac.fits""")

        # os.system(f"""java -jar stilts.jar cdsskymatch in={value.field}_vac.fits cdstable=II/363/unwise ra=RA dec=DEC radius=1 find=each blocksize=500000 ocmd='delcols "RAdeg DEdeg XposW1 XposW2 YposW1 YposW2 e_XposW1 e_XposW2 e_YposW1 e_YposW2 e_FW1 e_FW2 q_W1 q_W2 rchi2W1 rchi2W2 fFW1 fFW2 FW1lbs FW2lbs e_FW1lbs e_FW2lbs fwhmW1 fwhmW2 SpModW1 SpModW2 e_SpModW1 e_SpModW2 skyW1 skyW2 RAW1deg RAW2deg DEW1deg DEW2deg coaddID detIDW1 detIDW2 nmW1 nmW2 PrimW1 PrimW2 FlagsW1 FlagsW2 f_FlagsW1 f_FlagsW2 Prim"; addcol W1_ab "22.5 - 2.5*log10(FW1) + 2.699"; addcol W2_ab "22.5 - 2.5*log10(FW2) + 3.339"; addcol ID_unWISE "objID"; addcol angDist_unWISE "angDist"; delcols "objID angDist FW1 FW2"' out=Matched/{value.field}_vac.csv""")

        # os.system(f"""rm {value.field}.fits""")

        # os.system(f"""rm {value.field}_vac.fits""")

    except:
        print(f"Error on {field}")
        file = open('error.txt', 'a')
        file.write(f'Error on {field}\n')
        file.close()