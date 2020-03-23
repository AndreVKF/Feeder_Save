from datetime import datetime
from bizdays import *

import pandas as pd
import numpy as np

import sys
sys.path.insert(1, '..\\..\\GenericClasses\\')

from Views import Views

Refdate = 20200310
dtRefdate = pd.to_datetime(str(Refdate), format="%Y%m%d")

tstView = Views(Refdate=Refdate)

DF_Base = tstView.DF_BaseShareMov


DF_Base['Net_Amount'] = np.where(DF_Base['Id_Shareholder_MovType']==1, np.round(DF_Base['Value']*DF_Base['Amount'], 2),
                                np.where(DF_Base['Id_Shareholder_MovType']==2, np.round(DF_Base['Amount'], 2),
                                np.where(DF_Base['Id_Shareholder_MovType']==3, np.round(DF_Base['Yd_Shareholder_ShareQtt'].abs()*DF_Base['Value']*-1, 2), np.NaN)))

DF_Base['Number_Shares'] = np.where(DF_Base['Id_Shareholder_MovType']==1, np.round(DF_Base['Amount'], 7),
                                np.where(DF_Base['Id_Shareholder_MovType']==2, np.round(DF_Base['Amount']/DF_Base['Value'], 7),
                                np.where(DF_Base['Id_Shareholder_MovType']==3, np.round(DF_Base['Yd_Shareholder_ShareQtt'].abs()*-1, 7), np.NaN)))


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



