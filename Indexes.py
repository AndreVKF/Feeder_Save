from API_BBG import *
from Functions import *


class Indexes():

    # Construtor
    def __init__(self, SQL_Server_Connector, Queries):
        self.sql = SQL_Server_Connector
        self.queries = Queries

        self.DF_Index = SQL_Server_Connector.getData(query=self.queries.getRiskfactors())
        # self.DF_Index = SQL_Server_Connection.getData(query=self.queries.getRiskfactors())

    # Function para pegars os Index do dia

    def Index_Prices(self, Refdate, BBG_Req):

        fields = ['PX_LAST']

        # Pega preços dos Indexes da BBG python
        Indexes_BBG = BBG_POST(bbg_request=BBG_Req, tickers=self.DF_Index['RiskTicker'].tolist(),
                               fields=fields, date_start=Refdate,
                               date_end=Refdate)

        # Request to Data Frame
        Indexes_Px = (BBG_Json_to_DF(Request_type=BBG_Req,
                                     BBG_response=Indexes_BBG)).reset_index()

        Indexes_Px.rename(columns={'index': 'RiskTicker'}, inplace=True)

        Indexes = pd.merge(Indexes_Px, self.DF_Index,
                           on='RiskTicker', how='left')

        Indexes_DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Riskfactor': Indexes['Id'],
            'RiskTicker': Indexes['RiskTicker'],
            'PX_LAST': Indexes['PX_LAST']
        })

        print('Index prices carregados !')

        return Indexes_DF

    # Function para salvar indexes
    # Recebe uma data e um DataFrame pra salvar na tabela IndexesValue

    def Index_Save(self, Refdate, Indexes_Prices):
        # Variaveis locais
        DF = pd.DataFrame({
            'Date': str(Refdate),
            'Id_Riskfactor': Indexes_Prices['Id_Riskfactor'],
            'Value': Indexes_Prices['PX_LAST']
        })

        # Insere no DB
        try:
            self.sql.insertDataFrame(tableDB='IndexesValue', df=DF)
            print('IndexesValue de %s atualizado com sucesso!' % Refdate)
        except:
            print('Erro ao inserer Indexes no BD.')

    # Deleta histórico do dia
    # Recebe uma data e um DataFrame pra deletar histoico
    def Index_Delete(self, Refdate, Indexes_Prices):
        # Variaveis locais
        Id_Riskfactor = str(Indexes_Prices['Id_Riskfactor'].tolist())
        Id_Riskfactor = str.replace(Id_Riskfactor, '[', '')
        Id_Riskfactor = str.replace(Id_Riskfactor, ']', '')

        Del_Query = self.queries.Delete_Index_Date(Refdate=Refdate, Id_Riskfactor=Id_Riskfactor)

        # Insere no DB
        try:
            self.sql.execQuery(query=Del_Query)
            print('IndexesValue de %s deletados com sucesso!' % Refdate)
        except:
            print('Erro ao inserer Indexes no BD.')
