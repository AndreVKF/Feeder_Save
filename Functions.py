import sys
import requests
import json
import pandas as pd
import numpy as np

from pandas.io.json import json_normalize
from API_BBG import *

from datetime import date, datetime
from time import gmtime, strftime
from bizdays import Calendar, load_holidays


# Function para passar response do API_BBG para DataFrame


def BBG_Json_to_DF(Request_type, BBG_response):

    # If BDP
    if Request_type == "BDP":
        return pd.read_json(BBG_response.json(), orient='index')

    # Elif BDH
    elif Request_type == "BDH":
        # Empty dataframe
        newDF = pd.DataFrame()
        # Serializa response
        data = eval(BBG_response.json())

        for key in data:
            df = pd.read_json(data[key], orient='index')
            df['Ticker'] = key

            df = df.set_index(keys=['Ticker'], drop=True)
            del df.index.name

            newDF = newDF.append(df, ignore_index=False, sort=True)

        return newDF

# Deleta historico de precos de acordo com o AssetGroup


def Delete_Price_Hist(Refdate, Price_Df, SQL_Server_Connection, Queries):

    Worked = True

    if not Price_Df.empty:

        # Seleciona Id_Product dos ativos
        Id_Prod = str(Price_Df['Id_Product'].tolist())
        Id_Prod = str.replace(Id_Prod, '[', '')
        Id_Prod = str.replace(Id_Prod, ']', '')

        # Query para deletar ativos
        Del_Query = Queries.Delete_Prod_Prices(Refdate, Id_Prod)

        # Deleta products
        Worked = SQL_Server_Connection.execQuery(query=Del_Query)

    return Worked


# Function para criar dataframe específico
# De acordo com o AssetGroup es pecífico
def Create_PriceDF(BBG_Req, Refdate, AssetGroup, SQL_Server_Connection, Queries):

    # Price Fields
    Prices_Columns = ['Date', 'Id_Product', 'Price', 'Value',
                      'DV01', 'Delta1$', 'Delta%', 'Gamma',
                      'Vega$', 'Theta$', 'Delta2$', 'lvol',
                      'Last_Update']

    Prices_DF = pd.DataFrame(np.nan, index=[0], columns=Prices_Columns)

    # Set BBG Fields for AssetGroup
    if AssetGroup == 'Equity':
        fields = ['PX_LAST']
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)
    elif AssetGroup == 'Bond':
        fields = ['PX_LAST', 'RISK_MID', 'PX_DIRTY_MID']
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)
    elif AssetGroup == 'FX Forwards':
        fields = ['PX_LAST']
        BBG_Req = 'BDP'
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)
    elif AssetGroup == 'FX Spot':
        fields = ['PX_LAST']
        query = Queries.getFX_SPOT(Refdate=Refdate)
    elif AssetGroup == 'Listed Opt':
        fields = ['PX_LAST', 'DELTA', 'GAMMA', 'VEGA', 'IVOL']
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)
        query_undl = Queries.getOpt_Underlying(refdate=Refdate)
    elif AssetGroup == 'Futures':
        fields = ['PX_LAST', 'CONTRACT_VALUE']
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)
    elif AssetGroup == 'Funds':
        fields = ['PX_LAST']
        query = Queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup=AssetGroup)

    # Pega tickers dos ativos com posição
    PM_Products = SQL_Server_Connection.getData(
        query=query)

    # Filtra posicoes sem BBGTicker Cadastrado
    PM_Products = PM_Products[PM_Products["BBGTicker"].isnull() == False]

    #### Se PM_Products for vazio ####
    #### Break function ####
    if PM_Products.empty:
        return PM_Products

    # Pega preços da BBG python
    PM_BBG = BBG_POST(bbg_request=BBG_Req, tickers=PM_Products['BBGTicker'].tolist(),
                      fields=fields, date_start=Refdate,
                      date_end=Refdate)

    # Formata DF
    PM_DF = (BBG_Json_to_DF(Request_type=BBG_Req,
                            BBG_response=PM_BBG)).reset_index()

    PM_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)
    PM_BBGPriceDF = pd.merge(PM_DF, PM_Products,
                             on='BBGTicker', how='left')

    #### Se PM_BBGPriceDF for vazio ####
    #### Break function ####
    if PM_BBGPriceDF.empty:
        return PM_BBGPriceDF

    ########## Excecoes BBG para Bonds ##########

    if AssetGroup == 'Bond':
        #### Pega precos dos Bonds que não possuem precificacao @BGN ####

        # Remove assets com NaN no PX_LAST
        PM_BBGPriceDF = PM_BBGPriceDF.dropna(axis=0, subset=['PX_LAST'])

        # Left Join DF com todos os produtos com a DF de precos
        BGN_DF = pd.merge(PM_Products, PM_DF, on='BBGTicker', how='left')
        NaN_DF = BGN_DF[BGN_DF['PX_LAST'].isnull()]

        if not NaN_DF.empty:

            # Get prices ex BGN
            PM_NaN_BBG = BBG_POST(bbg_request=BBG_Req, tickers=NaN_DF['BBGTicker_Alt'].tolist(),
                                  fields=fields, date_start=Refdate,
                                  date_end=Refdate)
            # Formata DF
            PM_NaN_DF = (BBG_Json_to_DF(Request_type=BBG_Req,
                                        BBG_response=PM_NaN_BBG)).reset_index()

            PM_NaN_DF.rename(columns={'index': 'BBGTicker_Alt'}, inplace=True)
            PM_NaN_BBGPriceDF = pd.merge(PM_NaN_DF, PM_Products,
                                         on='BBGTicker_Alt', how='left')

            PM_BBGPriceDF = pd.concat([PM_BBGPriceDF, PM_NaN_BBGPriceDF],
                                      sort=True).reset_index(drop=True)

        #### Pega valor do PRINCIPAL_FACTOR dos BONDS ####
        Principal_Factor_BBG = BBG_POST(bbg_request='BDP', tickers=PM_Products['BBGTicker'].tolist(),
                                        fields=['PRINCIPAL_FACTOR'], date_start=Refdate,
                                        date_end=Refdate)
        # Formata DF
        Principal_Factor_DF = (BBG_Json_to_DF(Request_type='BDP',
                                              BBG_response=Principal_Factor_BBG)).reset_index()
        Principal_Factor_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)

        PM_BBGPriceDF = pd.merge(PM_BBGPriceDF, Principal_Factor_DF,
                                 on='BBGTicker', how='left')

        #### Adjust YAS Risk dos Corporates Bonds ####
        # Calendario
        strRefdate = datetime.strptime(str(Refdate), '%Y%m%d').strftime('%Y-%m-%d')

        Calendar_DF = SQL_Server_Connection.getData(query='SELECT * FROM Feriados_USA ORDER BY Data')
        Calendar_List = Calendar_DF['Data'].tolist()
        Calendar_USA = Calendar(Calendar_List, ['Sunday', 'Saturday'], name='T+2')
        settleDt = Calendar_USA.offset(strRefdate, 2).strftime("%Y%m%d")

        overrides = {"YAS_YLD_FLAG": "15", "SETTLE_DT": settleDt}

        #### Pega valor do YAS_RISK Adjusted Value ####
        YAS_RISK_BBG = BBG_POST_Tst(bbg_request='BDP', tickers=PM_Products['BBGTicker'].tolist(),
                                        fields=['YAS_RISK'], date_start=Refdate,
                                        date_end=Refdate, overrides=overrides)

         # Formata DF
        YAS_RISK_DF = (BBG_Json_to_DF(Request_type='BDP',
                                              BBG_response=YAS_RISK_BBG)).reset_index()
        YAS_RISK_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)

        PM_BBGPriceDF = pd.merge(PM_BBGPriceDF, YAS_RISK_DF,
                                 on='BBGTicker', how='left')

    ########## Pega precos dos Underlying Securities cadastrados nas options/futures ##########
    elif AssetGroup == 'Listed Opt' or AssetGroup == 'Futures':
        # Underlying Products
        fields = ['PX_LAST', 'CONTRACT_VALUE']
        Under_Products = SQL_Server_Connection.getData(
            query=query)

        # Checa posicoes com Underlying presente
        Under_Products = Under_Products[Under_Products["Underlying"].isnull() == False]

        # Under Products empty
        if not Under_Products.empty:
            Under_BBG = BBG_POST(bbg_request=BBG_Req, tickers=Under_Products['Underlying'].tolist(),
                                 fields=fields, date_start=Refdate,
                                 date_end=Refdate)

            # Formata DF
            Under_DF = (BBG_Json_to_DF(Request_type=BBG_Req,
                                       BBG_response=Under_BBG)).reset_index()

            Under_DF.rename(columns={'index': 'Underlying', 'PX_LAST': 'Under_PX_LAST',
                                     'CONTRACT_VALUE': 'Under_CONTRACT_VALUE'}, inplace=True)

            if Under_DF.empty:
                # Se nao trouxe valores crio um DataFrame vazio
                Under_DF = pd.DataFrame(
                    columns=['Underlying', 'Under_PX_LAST', 'Under_CONTRACT_VALUE'])
                PM_BBGPriceDF = pd.merge(PM_BBGPriceDF, Under_DF,
                                         on='Underlying', how='left')

            else:
                PM_BBGPriceDF = pd.merge(PM_BBGPriceDF, Under_DF,
                                         on='Underlying', how='left')

        # Cria um DataFrame vazio to append
        else:

            # Se nao trouxe valores crio um DataFrame vazio para dar merge
            Under_DF = pd.DataFrame(
                columns=['Underlying', 'Under_PX_LAST', 'Under_CONTRACT_VALUE'])
            PM_BBGPriceDF = pd.merge(PM_BBGPriceDF, Under_DF,
                                     on='Underlying', how='left')

    ########## FX SPOT Prices via BDP para Refdate<TdDate ##########
    elif AssetGroup == 'FX Spot' and BBG_Req == 'BDH':
        # Get SPOT  Products que nao sejam [PTAX, CASADO, BMFXTWO]
        query = Queries.getBDHF_FX(Refdate=Refdate)

        FX_BDH_Prod = SQL_Server_Connection.getData(query=query)

        FX_BDH_BBG = BBG_POST(bbg_request='BDP', tickers=FX_BDH_Prod['BBGTicker'].tolist(),
                              fields=fields, date_start=Refdate,
                              date_end=Refdate)

        # Formata DF
        FX_BDH_DF = (BBG_Json_to_DF(Request_type='BDP',
                                    BBG_response=FX_BDH_BBG)).reset_index()

        FX_BDH_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)
        FX_PM_BDH_DF = pd.merge(FX_BDH_DF, FX_BDH_Prod,
                                on='BBGTicker', how='left')

        # Concat Data Frames
        PM_BBGPriceDF = pd.concat([PM_BBGPriceDF, FX_PM_BDH_DF],
                                  sort=True).reset_index(drop=True)

    ########## Drop duplicates in case there are ##########

    PM_BBGPriceDF = PM_BBGPriceDF.drop_duplicates(subset=['Id_Product'])

    # Final Price DataFrame
    ########################### Equities ###########################
    if AssetGroup == 'Equity':
        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': PM_BBGPriceDF['Id_Product'],
            'Price': PM_BBGPriceDF['PX_LAST'],
            'Value': PM_BBGPriceDF['PX_LAST'],
            'DV01': np.nan,
            'Delta1$': PM_BBGPriceDF['PX_LAST'],
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': PM_BBGPriceDF['PX_LAST'],
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    ########################### Bond ###########################
    elif AssetGroup == 'Bond':
        # Adiciona colunas em PM_BBGPriceDF
        PM_BBGPriceDF['Value'] = np.where(PM_BBGPriceDF['Instrument'] == 'Letras Financeiras do Tesouro (LFT)',
                                          PM_BBGPriceDF['PX_LAST'], PM_BBGPriceDF['PX_DIRTY_MID']/100)

        # Marretada CSAN 8 1/4 11/29/49
        PM_BBGPriceDF['DV01'] = np.where(
            PM_BBGPriceDF['Instrument'] == 'Letras Financeiras do Tesouro (LFT)', np.nan,
            np.where(PM_BBGPriceDF['Id_Product'] == 1288, 0.15/10000, PM_BBGPriceDF['YAS_RISK']/10000))

        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': PM_BBGPriceDF['Id_Product'],
            'Price': PM_BBGPriceDF['PX_LAST'] * PM_BBGPriceDF['PRINCIPAL_FACTOR'],
            'Value': PM_BBGPriceDF['Value'] * PM_BBGPriceDF['PRINCIPAL_FACTOR'],
            'DV01': PM_BBGPriceDF['DV01'] * PM_BBGPriceDF['PRINCIPAL_FACTOR'],
            'Delta1$': np.nan,
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': PM_BBGPriceDF['Value'] * PM_BBGPriceDF['PRINCIPAL_FACTOR'],
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    ########################### Futures ###########################
    elif AssetGroup == 'Futures':
        # Adiciona colunas em PM_BBGPriceDF
        PM_BBGPriceDF['Days_To_Valuation'] = (pd.to_datetime(
            PM_BBGPriceDF['Valuation']) - datetime.strptime(str(Refdate), "%Y%m%d")).dt.days

        PM_BBGPriceDF['Mult'] = np.where(PM_BBGPriceDF['Instrument']
                                         == 'BMF Cupom Cambial (EV)', 2.00, 1.00)

        PM_BBGPriceDF['Vl'] = (PM_BBGPriceDF['CONTRACT_VALUE'] * PM_BBGPriceDF['Mult'])

        PM_BBGPriceDF['DV01'] = np.where(PM_BBGPriceDF['Instrument'] == 'BMF US Dollar Fut (UC)', np.nan,
                                         np.where(PM_BBGPriceDF['Instrument'] == 'US Treasury Note Futures Contract (OI)', 9 * PM_BBGPriceDF['CONTRACT_VALUE'] / 10000,
                                                  np.where(PM_BBGPriceDF['Instrument'] == 'BMF Cupom Cambial (EV)', 50000/(1+(PM_BBGPriceDF['PX_LAST']/100)*PM_BBGPriceDF['Days_To_Valuation']/360) - 50000/(1+((PM_BBGPriceDF['PX_LAST']+0.01)/100)*PM_BBGPriceDF['Days_To_Valuation']/360), np.nan)))

        PM_BBGPriceDF['Delta1$'] = np.where(PM_BBGPriceDF['Instrument'] == 'BMF Cupom Cambial (EV)', (-1 * PM_BBGPriceDF['Under_CONTRACT_VALUE']),
                                            np.where(PM_BBGPriceDF['Instrument'] == 'US Treasury Note Futures Contract (OI)', np.nan,
                                                     PM_BBGPriceDF['CONTRACT_VALUE']))

        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': PM_BBGPriceDF['Id_Product'],
            'Price': PM_BBGPriceDF['PX_LAST'],
            'Value': PM_BBGPriceDF['Vl'],
            'DV01': PM_BBGPriceDF['DV01'],
            'Delta1$': PM_BBGPriceDF['Delta1$'],
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': np.nan,
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    ########################### FX ###########################
    elif AssetGroup == 'FX Spot' or AssetGroup == 'FX Forwards':
        # Adiciona colunas em PM_BBGPriceDF

        # FX Spot
        if AssetGroup == 'FX Spot':
            PM_BBGPriceDF['FX Value'] = np.where(
                PM_BBGPriceDF['IsInv'] == True, PM_BBGPriceDF['PX_LAST'], (1/PM_BBGPriceDF['PX_LAST']))

            Prices_DF = pd.DataFrame({
                'Date': str(Refdate),
                'Id_Product': PM_BBGPriceDF['Id_Product'],
                'Price': PM_BBGPriceDF['PX_LAST'],
                'Value': PM_BBGPriceDF['FX Value'],
                'DV01': np.nan,
                'Delta1$': PM_BBGPriceDF['FX Value'],
                'Delta%': np.nan,
                'Gamma$': np.nan,
                'Vega$': np.nan,
                'Theta$': np.nan,
                'Delta2$': np.nan,
                'Ivol': np.nan,
                'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        # FX Forwards
        elif AssetGroup == 'FX Forwards':
            Prices_DF = pd.DataFrame({
                'Date': str(Refdate),
                'Id_Product': PM_BBGPriceDF['Id_Product'],
                'Price': PM_BBGPriceDF['PX_LAST'],
                'Value': PM_BBGPriceDF['PX_LAST'],
                'DV01': np.nan,
                'Delta1$': PM_BBGPriceDF['PX_LAST'],
                'Delta%': np.nan,
                'Gamma$': np.nan,
                'Vega$': np.nan,
                'Theta$': np.nan,
                'Delta2$': np.nan,
                'Ivol': np.nan,
                'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

    ########################### Options ###########################
    elif AssetGroup == 'Listed Opt':

        # Condition p/ Equity Index Opt; Equity Volatility Index Opt; Equity Opt
        PM_BBGPriceDF['Vl'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['PX_LAST'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST'])

        PM_BBGPriceDF['Delta1$'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['Under_PX_LAST']*PM_BBGPriceDF['DELTA'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['DELTA'])

        PM_BBGPriceDF['Delta%'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['DELTA'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['DELTA'])

        PM_BBGPriceDF['Gamma$'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['GAMMA'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['GAMMA'])

        PM_BBGPriceDF['Vega$'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['VEGA'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['VEGA'])

        PM_BBGPriceDF['Vega$'] = np.where(
            AssetGroup == 'Equity Index Spot Options'
            or AssetGroup == 'Equity Volatility Index Option'
            or AssetGroup == 'Equity Option',
            PM_BBGPriceDF['Opt_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['VEGA'],
            PM_BBGPriceDF['Fut_Val_Pt']*PM_BBGPriceDF['PX_LAST']*PM_BBGPriceDF['VEGA'])

        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': PM_BBGPriceDF['Id_Product'],
            'Price': PM_BBGPriceDF['PX_LAST'],
            'Value': PM_BBGPriceDF['Vl'],
            'DV01': np.nan,
            'Delta1$': PM_BBGPriceDF['Delta1$'],
            'Delta%': PM_BBGPriceDF['Delta%'],
            'Gamma$': PM_BBGPriceDF['Gamma$'],
            'Vega$': PM_BBGPriceDF['Vega$'],
            'Theta$': np.nan,
            'Delta2$': PM_BBGPriceDF['Vl'],
            'Ivol': PM_BBGPriceDF['IVOL']/100.00,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    ########################### Funds ###########################
    elif AssetGroup == 'Funds':
        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': PM_BBGPriceDF['Id_Product'],
            'Price': PM_BBGPriceDF['PX_LAST'],
            'Value': PM_BBGPriceDF['PX_LAST'],
            'DV01': np.nan,
            'Delta1$': PM_BBGPriceDF['PX_LAST'],
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': PM_BBGPriceDF['PX_LAST'],
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    return Prices_DF

# Function para salvar precos dos ativos de preco "1"


def Create_PriceUmDF(Refdate, SQL_Server_Connection, Queries):
    # Query
    # products
    # Inst.Id_CashFlow in (7, 10) OR AssetGroup.name='Cash'

    query = Queries.getPos_PriceUmDF(refdate=Refdate)
    # Get Products
    PM_Products = SQL_Server_Connection.getData(query=query)

    Prices_DF = pd.DataFrame({
        'Date': str(Refdate),
        'Id_Product': PM_Products['Id_Product'],
        'Price': 1.00,
        'Value': 1.00,
        'DV01': np.nan,
        'Delta1$': 1.00,
        'Delta%': np.nan,
        'Gamma$': np.nan,
        'Vega$': np.nan,
        'Theta$': np.nan,
        'Delta2$': 1.00,
        'Ivol': np.nan,
        'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

    return Prices_DF

# Function para salvar precos dos ativos por Instrument


def Create_CRAPriceDF(Refdate, SQL_Server_Connection, Queries, CRA_Calculator):

    # Class para cálculo dos CRAs
    # CRA_Calculator = CRA_Calculator(Refdate, SQL_Server_Connection, Queries)

    # Get CRA position
    CRA_query = Queries.getPos_by_Instrument(
        Refdate=Refdate, Instrument="Certificado de Recebíveis do Agronegócio (CRA)")
    CRA_DF = SQL_Server_Connection.getData(query=CRA_query)

    # Check se possui posição de CRAs
    if CRA_DF.empty:
        return CRA_DF

    else:
        CRA_DF['PX'] = CRA_DF.apply(CRA_Calculator.CRA_Price, axis=1)
        CRA_DF['Amort'] = CRA_DF.apply(CRA_Calculator.CRA_TotalAmort, axis=1)
        CRA_DF['Price'] = CRA_DF['PX'] - CRA_DF['Amort']

        # Price Fields
        Prices_Columns = ['Date', 'Id_Product', 'Price', 'Value',
                          'DV01', 'Delta1$', 'Delta%', 'Gamma',
                          'Vega$', 'Theta$', 'Delta2$', 'lvol',
                          'Last_Update']

        Prices_DF = pd.DataFrame(np.nan, index=[0], columns=Prices_Columns)

        Prices_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': CRA_DF['Id_Product'],
            'Price': CRA_DF['Price'],
            'Value': CRA_DF['Price'],
            'DV01': np.nan,
            'Delta1$': CRA_DF['Price'],
            'Delta%': np.nan,
            'Gamma$': np.nan,
            'Vega$': np.nan,
            'Theta$': np.nan,
            'Delta2$': CRA_DF['Price'],
            'Ivol': np.nan,
            'Last_Update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        return Prices_DF

# Check se possui precos para todos os ativos


def Try_ExecProcessNAV(Refdate, Log_DirSrc, SQL_Server_Connection, Queries):

    # Variaveis
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    # Missing price DF
    query = Queries.getPos_PriceMiss(refdate=Refdate)
    Missing_Px = SQL_Server_Connection.getData(query=query)

    # Abre log
    file = open(Log_DirSrc, "a+")

    # Se nao faltaram precos roda Procedure para calcular Position
    if Missing_Px.empty:
        # Tenta executar ProcessSharePosition
        try:
            query_exec = f"EXEC ProcessSharePosition '{Refdate}'"
            SQL_Server_Connection.execQuery(query=query_exec)
            file.write(f"Refdate:{Refdate} Executed ProcessSharePosition Time:{Time_Now}\n")
        except:
            file.write(f"Refdate:{Refdate} Failure to Exec ProcessSharePosition Time:{Time_Now}\n")
            file.close()
            return 0

        # Tenta executar ProcessDBV3
        try:
            query_exec = f"EXEC ProcessDBV3 '{Refdate}'"
            SQL_Server_Connection.execQuery(query=query_exec)
            file.write(f"Refdate:{Refdate} Executed ProcessDBV3 Time:{Time_Now}\n")
        except:
            file.write(f"Refdate:{Refdate} Failure to Exec ProcessDBV3 Time:{Time_Now}\n")
            file.close()
            return 0

        # Tenta executar SaveProfitTableT
        try:
            query_exec = f"EXEC SaveProfitTableT '{Refdate}'"
            SQL_Server_Connection.execQuery(query=query_exec)
            file.write(f"Refdate:{Refdate} Executed SaveProfitTableT Time:{Time_Now}\n")
        except:
            file.write(f"Refdate:{Refdate} Failure to Exec SaveProfitTableT Time:{Time_Now}\n")
            file.close()
            return 0

    else:
        file.write(f"Refdate:{Refdate} Missing Px For:{str(Missing_Px['Name'].tolist())} Time:{Time_Now}\n")

    # Close File Log
    file.close()
    return 1

# Function para executar Bonds data Procedures


def Exec_PositionBondsData(Refdate, Log_DirSrc, SQL_Server_Connection, Queries):

    # Get query
    query = Queries.Exec_PositionBonds(Refdate)

    # Variaveis
    Time_Now = datetime.now().strftime("%Y%m%d %H:%M:%S")

    # Abre log
    file = open(Log_DirSrc, "a+")

    # Try Save Position Bonds
    try:
        SQL_Server_Connection.execQuery(query=query)
        file.write(f"Refdate:{Refdate} Executed Save_PositionBonds Time:{Time_Now}\n")
    except:
        file.write(f"Refdate:{Refdate} Failure to Execute Save_PositionBonds Time:{Time_Now}\n")

    # Class Based Functions

    # Class calculator para pegar o preco dos CRAs


class CRA_Calculator():

    # Constructor
    def __init__(self, Refdate, SQL_Server_Connection, Queries):
        self.Refdate = Refdate
        self.SQL_Server_Connection = SQL_Server_Connection
        self.Queries = Queries

        # CRAs Count

    # Function para pegar valor total da amortizado
    def CRA_TotalAmort(self, Row):

        Refdate = self.Refdate
        SQL_Server_Connection = self.SQL_Server_Connection
        Queries = self.Queries

        # Get Total Amortization
        Total_Amort_Query = Queries.getTotalAmort_CRA(Refdate=Refdate, Id_Product=Row['Id_Product'])
        Total_Amort = SQL_Server_Connection.getValue(query=Total_Amort_Query)

        return float(Total_Amort)

        # Function para calcular o price CRA

    def CRA_Price(self, Row):

        Refdate = self.Refdate
        SQL_Server_Connection = self.SQL_Server_Connection
        Queries = self.Queries

        # Str_Refdate
        Str_Refdate = str(Refdate)[0:4] + "-" + \
            str(Refdate)[4:6] + "-" + str(Refdate)[6:8]

        # Feriados
        query_Feriados = Queries.getHolidays(Feriados="BMF")
        DF_Feriados = SQL_Server_Connection.getData(query=query_Feriados)

        Feriados = Calendar(holidays=DF_Feriados['Data'].tolist(), weekdays=['Sunday', 'Saturday'])
        Dias_Uteis = Feriados.bizdays(Row['Info'], Str_Refdate)

        # Taxa
        Taxa = Row['Underlying']

        # Get Index CDI Acumulado
        CDI_Index_Refdate_Query = Queries.getIndex_CDIAcum(Refdate=Refdate)
        CDI_Index_Refdate = SQL_Server_Connection.getValue(
            query=CDI_Index_Refdate_Query, vlType="float")

        CDI_Index_IniDate_Query = Queries.getIndex_CDIAcum(Refdate=Row['Info'])
        CDI_Index_IniDate = SQL_Server_Connection.getValue(
            query=CDI_Index_IniDate_Query, vlType="float")

        # Calculated Price
        CRA_Px = 1000 * CDI_Index_Refdate/CDI_Index_IniDate * pow(1 + float(Taxa), Dias_Uteis/252)

        return CRA_Px
