from bizdays import Calendar
from datetime import datetime

from API_BBG import *
from Functions import *

import numpy as np
import pandas as pd
# Classe para atualizar coupons dos bonds


class Bond_Coupons():
    # Construtor
    def __init__(self, SQL_Server_Connection, Queries):
        self.sql = SQL_Server_Connection
        self.queries = Queries

    # Function para passar datas

    # Function para transformar timestamp para data
    def PREV_CPN_DT_to_Strdate(self, row):

        # Calendario
        Calendar_DF = self.sql.getData(query='SELECT * FROM Feriados_USA ORDER BY Data')
        Calendar_List = Calendar_DF['Data'].tolist()

        Calendar_USA = Calendar(Calendar_List, ['Sunday', 'Saturday'])

        dt_stamp = datetime.fromtimestamp(row['PREV_CPN_DT']/1000)
        dt_str = dt_stamp.strftime('%Y-%m-%d')
        cpn_date = Calendar_USA.offset(dt_str, -1)

        return cpn_date

    # Pega lista dos Bonds
    def getBond_List(self, Refdate):
        Bonds_Query = self.queries.getPos_by_AssetGroup(refdate=Refdate, assetGroup='Bond')
        Bonds_DF = self.sql.getData(query=Bonds_Query)

        return Bonds_DF

    # Pega Rating dos Bonds
    def getBond_Ratings(self, Bonds_DF, Refdate):

        # Variaveis BBG
        fields = ['BB_COMPOSITE']

        # fields = ['BB_COMPOSITE',
        #           'RTG_MOODY',
        #           'RTG_FITCH',
        #           'RTG_SP']

        # Lista Tickers
        Bonds_Tickers = Bonds_DF['BBGTicker'].tolist()

        # Monta com os Ratings BBG_Composite
        Bonds_Tickers = BBG_POST(bbg_request='BDP', tickers=Bonds_Tickers, fields=fields)
        Bonds_Rat_DF = (BBG_Json_to_DF(Request_type='BDP',
                                       BBG_response=Bonds_Tickers).reset_index()).dropna()
        Bonds_Rat_DF.rename(columns={'index': 'BBGTicker', fields[0]: 'Rating'}, inplace=True)

        Bonds_Rat_DF = pd.merge(Bonds_Rat_DF, Bonds_DF,
                                on='BBGTicker', how='left')
        Bonds_Rat_DF["Id_Rating_Agency"] = 2

        Bonds_Rat_DF = Bonds_Rat_DF[["BBGTicker", "Rating",
                                     "Id_Product", "Product", "Id_Rating_Agency"]]

        # Checa se todos os Bonds possuem BBG_Ratings
        Bonds_Rat_NA = Bonds_Rat_DF[(Bonds_Rat_DF["Rating"] == "NR")
                                    | (Bonds_Rat_DF["Rating"] == "WR")
                                    | (Bonds_Rat_DF["Rating"] == "WD")]

        Rating_Flag = Bonds_Rat_NA.empty

        # Delete Ratings NR e WR do DataFrame principal
        Bonds_Rat_DF.drop(Bonds_Rat_NA.index, inplace=True)

        # Variaves do Loop
        fields = ['RTG_MOODY',
                  'RTG_FITCH',
                  'RTG_SP']
        ind = 0
        # ind = 1
        # Baixa Rating para os ativos que nao possuem BB_COMPOSITE Rating
        while not Rating_Flag and ind <= 2:
            fields_BBG = fields[ind]

            # Rating Secundario DataFrame
            Bonds_Tickers = BBG_POST(
                bbg_request='BDP', tickers=Bonds_Rat_NA["BBGTicker"].tolist(), fields=[fields_BBG])
            Bonds_Rat_Sec = (BBG_Json_to_DF(Request_type='BDP',
                                            BBG_response=Bonds_Tickers).reset_index()).dropna()

            if not Bonds_Rat_Sec.empty:
                Bonds_Rat_Sec.rename(columns={'index': 'BBGTicker', fields_BBG: 'Rating'}, inplace=True)
                Bonds_Rat_Sec["Id_Rating_Agency"] = ind + 1

                Bonds_Rat_Sec_DF = pd.merge(Bonds_Rat_NA[['BBGTicker', 'Id_Product', 'Product']], Bonds_Rat_Sec,
                                            on='BBGTicker', how='left')

                # Adjust Ratings
                Bonds_Rat_Sec_DF['Rating'] = np.where(Bonds_Rat_Sec_DF["Rating"] == "NR", np.nan,
                                                      np.where(Bonds_Rat_Sec_DF["Rating"] == "WR", np.nan,
                                                               np.where(Bonds_Rat_Sec_DF["Rating"] == "WD", np.nan, Bonds_Rat_Sec_DF['Rating'])))

                # Update Bonds Rating NA e check flag
                Bonds_Rat_NA = pd.merge(Bonds_Rat_NA[['BBGTicker', 'Id_Product', 'Product']], Bonds_Rat_Sec,
                                        on='BBGTicker', how='left')
                Bonds_Rat_NA = Bonds_Rat_NA[Bonds_Rat_NA['Rating'].isnull()]
                if Bonds_Rat_NA.empty:
                    Rating_Flag = True

                # Update Rating Bonds DF
                Bonds_Rat_Upd_DF = Bonds_Rat_Sec_DF.dropna(axis=0, subset=["Rating"])

                if not Bonds_Rat_Upd_DF.empty:
                    Update_DF = Bonds_Rat_Upd_DF[["BBGTicker", "Rating",
                                                  "Id_Product", "Product", "Id_Rating_Agency"]]
                    Bonds_Rat_DF = pd.concat([Bonds_Rat_DF, Update_DF])

            ind += 1

        # Monta DataFrame
        Bonds_Rat_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': Bonds_Rat_DF['Id_Product'],
            'Product': Bonds_Rat_DF['Product'],
            'Rating': Bonds_Rat_DF['Rating'],
            'Id_Rating_Agency': Bonds_Rat_DF['Id_Rating_Agency']
        })

        return Bonds_Rat_DF

    # Pega tabela de Ratings Octante
    def getRatings_Group(self, Bonds_BBG_Ratings):
        # Get Octante Rating DF
        query = self.queries.getOctante_Ratings()
        rating_DF = self.sql.getData(query=query)

        # Merge Rating
        Bonds_RatGrp_DF = pd.merge(Bonds_BBG_Ratings, rating_DF,
                                   on=['Id_Rating_Agency', 'Rating'], how='left')

        # Monta DataFrame Return
        Bonds_Oct_Ratings = pd.DataFrame({
            'Date': Bonds_RatGrp_DF['Date'],
            'Id_Product': Bonds_RatGrp_DF['Id_Product'],
            'BB_COMPOSITE': Bonds_RatGrp_DF['Group_Scale']
        })

        return Bonds_Oct_Ratings

    # Pega bonds que pagam coupon na data
    def getBond_Coupons(self, Bonds_DF, Refdate):

        # Variaveis BBG
        fields = ['PREV_CPN_DT',
                  'CPN',
                  'CPN_FREQ']

        # Lista de tickers
        Bonds_Tickers = Bonds_DF['BBGTicker'].tolist()

        # Monta DF com pagamentos dos coupons
        Bonds_Tickers = BBG_POST(bbg_request='BDP', tickers=Bonds_Tickers, fields=fields)
        Bonds_Cp_DF = (BBG_Json_to_DF(Request_type='BDP',
                                      BBG_response=Bonds_Tickers).reset_index()).dropna()
        Bonds_Cp_DF.rename(columns={'index': 'BBGTicker'}, inplace=True)

        # Date adjustments
        Bonds_Cp_DF['PREV_CPN_DT'] = Bonds_Cp_DF.apply(self.PREV_CPN_DT_to_Strdate, axis=1)

        # Check today payment
        Bonds_Cp_DF['CPN_DT'] = np.where(
            Bonds_Cp_DF['PREV_CPN_DT'] == datetime.strptime(str(Refdate), '%Y%m%d').date(), True, False)

        # Check cpn value
        Bonds_Cp_DF['CPN_VALUE'] = Bonds_Cp_DF['CPN']/Bonds_Cp_DF['CPN_FREQ']/100

        # Deixei 2 para testar
        # Voltar Teste Bonds_Cp_DF['CPN_FREQ'] == 1
        Bonds_Cp_DF = Bonds_Cp_DF[Bonds_Cp_DF['CPN_DT'] == True]

        return Bonds_Cp_DF

    # Monta dataframe para ser inserido no BD
    # Recebe um DataFrame com a posição dos Bonds;
    # Recebe um Dataframe com o conjunto de Bonds que pagam coupon na data Refdate;
    def createAssetEvent_CouponsDF(self, Bonds_DF, Bonds_TdCp, Refdate):
        Merge_BondsCp = pd.merge(Bonds_TdCp, Bonds_DF, on='BBGTicker', how='left')

        AssetEvents_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Product': Merge_BondsCp['Id_Product'],
            'Id_AssetEventType': 1,
            'Value': Merge_BondsCp['CPN_VALUE'],
            'Id_Ccy': Merge_BondsCp['Id_Currency']
        })

        return AssetEvents_DF

    # Insere coupons na tabela Bond Ratings
    # Recebe um df no formato da tabela no BD
    def insertBond_Ratings(self, Bonds_Oct_Ratings):
        # Checa se dataframe está vazio
        if not Bonds_Oct_Ratings.empty:
            try:
                self.sql.insertDataFrame(tableDB='BondsRating', df=Bonds_Oct_Ratings)
                print('Bond Ratings atualizados.')
            except:
                print('Erro ao inserir Bond Ratings.')
        else:
            print('Não existem Ratings a serem inseridos.')

    # Insere coupons na tabela Asset Events
    # Recebe um df no formato da tabela no BD
    def insertAssetEvents_Coupons(self, AssetEvents_DF):
        # Checa se dataframe está vazio
        if not AssetEvents_DF.empty:
            try:
                self.sql.insertDataFrame(tableDB='AssetEvents', df=AssetEvents_DF)
                print('Coupons atualizados.')
            except:
                print('Erro ao inserir coupons.')
        else:
            print('Não existem coupons a serem inseridos.')

    # Deleta coupons na tabela Asset Events
    # Recebe um df no formato da tabela no BD e uma data Refdate
    def deleteAssetEvents_Coupons(self, AssetEvents_DF, Refdate):
        # Prod List
        Id_Prod_List = str(AssetEvents_DF['Id_Product'].tolist())
        Id_Prod_List = str.replace(Id_Prod_List, '[', '')
        Id_Prod_List = str.replace(Id_Prod_List, ']', '')

        # Checa se dataframe está vazio
        if not AssetEvents_DF.empty:
            try:
                self.sql.execQuery(query=self.queries.Delete_Coupons_AssetEvents(
                    Refdate=Refdate, Id_Products=Id_Prod_List))
            except:
                print('Erro ao remover coupons do dia.')

    # Deleta coupons na tabela Asset Events
    # Recebe um df no formato da tabela no BD e uma data Refdate
    def deleteBonds_Ratings(self, Bonds_Oct_Ratings, Refdate):
        # Prod List
        Id_Prod_List = str(Bonds_Oct_Ratings['Id_Product'].tolist())
        Id_Prod_List = str.replace(Id_Prod_List, '[', '')
        Id_Prod_List = str.replace(Id_Prod_List, ']', '')

        # Checa se dataframe está vazio
        if not Bonds_Oct_Ratings.empty:
            try:
                self.sql.execQuery(query=self.queries.Delete_Bonds_Ratings(
                    Refdate=Refdate, Id_Products=Id_Prod_List))
            except:
                print('Erro ao remover coupons do dia.')
