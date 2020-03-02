from datetime import datetime
from bizdays import *

import pandas as pd
import numpy as np

import sys
sys.path.insert(1, '..\\..\\GenericClasses\\')

from Views import Views

Refdate = 20200227
dtRefdate = pd.to_datetime(str(Refdate), format="%Y%m%d")

tstView = Views(Refdate=Refdate)

Base_DF = tstView.DF_Mod_Share_Subscriptions[['Refdate'
    ,'Id_ShareMov'
    ,'Id_Shareholder'
    ,'Id_Entity'
    ,'Td_CloseAmount']]
Entities_DF = tstView.DF_Entities_AdmRebate
Shareholders_DF = tstView.DF_Shareholders_AdmRebate

Base_DF = Base_DF.merge(Entities_DF, how='left', on='Id_Entity')
Base_DF = Base_DF.merge(Shareholders_DF, how='left', on='Id_Shareholder')

Base_DF['Vl_Rebate'] = np.where(Base_DF['Id_Shareholders_Distribuidores_Rebate']==1,
    Base_DF['Td_CloseAmount'] * ((1 + Base_DF['Value']) ** (1 / 252) - 1) * Base_DF['Rebate_Amount'],
        np.where(Base_DF['Id_Shareholders_Distribuidores_Rebate']==2,
        Base_DF['Td_CloseAmount'] * ((1 + Base_DF['Rebate_Amount']) ** (1 / 252) - 1),
        np.nan))

Base_DF.rename(columns={'Rebate_Amount': 'Pct_Rebate',
    'Value': 'Tx_Adm',
    'Td_CloseAmount' :'Td_Amount'
    ,'Shareholder_Distribuidor': 'Distribuidor'}, inplace=True)

Base_DF[tstView.Shareholders_Distribuidores_Rebate_Amount_ColNames]


tstView.DF_Share_Operations.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Share_Operations.csv')
tstView.DF_Mod_Share_Subscriptions.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Share_Subscriptions.csv')
tstView.DF_Share_Redemptions.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Share_Redemptions.csv')
tstView.DF_Shareholder_Position.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Shareholder_Position.csv')
tstView.DF_Passive_Funds.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Passive_Funds.csv')

tstView.Full_Check_Shareholder_Position.to_csv(f'C:\\Users\\andre\\Documents\\Python\\ProcessNAV\\MainClasses\\Passivo\\CSV\\{Refdate}_DF_Full_Check_Shareholder_Position.csv')



