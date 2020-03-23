from datetime import *

class PassivoQueries():

    def __init__(self, Refdate=int(date.today().strftime("%Y%m%d"))):
        super().__init__()
        # Main Arguments
        self.Refdate = Refdate

    def QTdShareMov(self):

        query = f"""
            DECLARE @Refdate AS DATE
            DECLARE @YdDate AS DATE

            SET @Refdate = '{self.Refdate}'
            SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;


            SELECT
                Share_Mov.Id AS Id_ShareMov
                ,Shareholders.Id AS Shareholder_Id
                ,Shareholders.Name AS Shareholder_Name
                ,Entities.Id AS Id_Entity
                ,Entities.Name AS Entity_Name
                ,Products.Id AS Id_Product
                ,Products.Name AS Product_Name
                ,Products.Id_Currency AS Id_Currency
                ,Share_Mov.Request_Date 
                ,Share_Mov.Quote_Date
                ,Share_Mov.EffQuote_Date
                ,Share_Mov.EffectiveFin_Date
                ,Shareholder_OperationType.Name AS Movimentação
                ,Shareholder_MovType.Name AS Instrumento_Movimentação
                ,Shareholders_Distribuidores.Name
                ,Prices.Value AS ShareValue

            FROM 
                Share_Mov
                
            -- Joins
            LEFT JOIN Shareholders ON Share_Mov.Id_Shareholder=Shareholders.Id
            LEFT JOIN Entities ON Share_Mov.Id_Entity=Entities.Id
            LEFT JOIN Entities_PassiveInfo ON Entities.Id=Entities_PassiveInfo.Id_Entity
            LEFT JOIN Products ON Entities.Id_Product=Products.Id
            LEFT JOIN Prices ON Prices.Id_Product=Products.Id
            LEFT JOIN Shareholder_OperationType ON Share_Mov.Id_Shareholder_OperationType=Shareholder_OperationType.id
            LEFT JOIN Shareholder_MovType ON Share_Mov.Id_Shareholder_MovType=Shareholder_MovType.id
            LEFT JOIN Shareholders_Distribuidores ON Share_Mov.Id_Distribuidor=Shareholders_Distribuidores.Id
            WHERE
                Prices.Date = Share_Mov.EffQuote_Date
                AND Share_Mov.Quote_Date = @Refdate
                AND @Refdate>=Entities.Inception_Date AND (@Refdate<=Entities.Liquidation_Date OR Entities.Liquidation_Date IS NULL)"""

        return query

    def QShareMov_byRefdate(self):

        query = f"""
            SELECT * FROM Share_Mov WHERE EffQuote_Date='{self.Refdate}'
            """

        return query

    def QShareholders(self):

        query = f"""
        SELECT * FROM Shareholders
        """
    
        return query

    def QShareMov_BaseQuery(self):

        query = f"""
        DECLARE @Refdate DATE
        DECLARE @YdDate DATE

        SET @Refdate='{self.Refdate}'
        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;

        SELECT
            Share_Mov.Id AS Id_Share_Mov
            ,Shareholders.Id AS Id_Shareholder
            ,Shareholders.Name AS Shareholder
            ,Share_Mov.Id_Entity AS Id_Entity
            ,Share_Mov.Request_Date AS Request_Date
            ,Share_Mov.Quote_Date AS Quote_Date
            ,Share_Mov.EffectiveFin_Date AS EffectiveFin_Date
            ,Share_Mov.EffQuote_Date AS EffQuote_Date
            ,Entities.Name AS Entity
            ,Products.Name AS Product
            ,Products.Id_Currency AS Id_Currency
            ,Prices.Value AS Value
            ,Share_Mov.Id_Shareholder_MovType AS Id_Shareholder_MovType
            ,Products.Id AS Id_Product
            ,ISNULL(Share_Mov.Amount, 0) AS Amount
            ,Shareholders_Distribuidores.Name AS Shareholders_Distribuidores_Name
            ,Shareholders.Info1 AS Cod_Itau
            ,Shareholders.Cod_Cotista_Distribuidor AS Cod_Cotista_Distribuidor
            ,Shareholder_OperationType.Id AS Id_OperationType
            ,Shareholder_OperationType.Name AS OperationType
            ,Shareholder_MovType.Id AS Id_MovType
            ,Shareholder_MovType.Name AS MovType
            ,ISNULL(Yd_Shareholder_Position.Td_CloseShareQtt,0) AS Yd_Shareholder_ShareQtt
            

        FROM 
        /********************** JOIN **********************/
            Share_Mov 
        LEFT JOIN Shareholders ON Share_Mov.Id_Shareholder=Shareholders.Id
        LEFT JOIN Entities ON Share_Mov.Id_Entity=Entities.Id
        LEFT JOIN Shareholder_OperationType ON Share_Mov.Id_Shareholder_OperationType=Shareholder_OperationType.Id
        LEFT JOIN Shareholder_MovType ON Share_Mov.Id_Shareholder_MovType=Shareholder_MovType.Id
        LEFT JOIN Shareholders_Distribuidores ON Share_Mov.Id_Distribuidor=Shareholders_Distribuidores.Id
        LEFT JOIN Shareholder_Class ON Shareholders.Id_Shareholder_Class=Shareholder_Class.Id
        LEFT JOIN Products ON Entities.Id_Product=Products.Id
        LEFT JOIN Riskfactor ON Entities.Id_RiskfactorBenchmark=Riskfactor.Id
        LEFT JOIN Prices ON Prices.Id_Product=Entities.Id_Product
            AND Prices.Date=Share_Mov.EffQuote_Date

        -- Get previous Day Position
        LEFT JOIN Shareholder_Position AS Yd_Shareholder_Position ON Yd_Shareholder_Position.Refdate=@YdDate
            AND Products.Id=Yd_Shareholder_Position.Id_Product
            AND Shareholders.Id=Yd_Shareholder_Position.Id_Shareholder
        WHERE 
            EffQuote_Date=@Refdate
            AND @Refdate>=Entities.Inception_Date AND (@Refdate<=Entities.Liquidation_Date OR Entities.Liquidation_Date IS NULL)"""

        return query


    def QOpenShareSubscriptions(self):

        query = f"""
        declare @Refdate DATE

        SET @Refdate='{self.Refdate}'

        SELECT
                @Refdate AS Refdate
                ,Id_ShareMov AS Id_ShareMov
                ,Shareholder_Id AS Id_Shareholder
                ,Id_Entity AS Id_Entity
                ,Share_Operations.Id_Product AS Id_Product
                ,Entities.Id_PFeeType AS Id_PFeeType
                ,Entities.Id_RiskfactorBenchmark AS Id_RiskFactorBenchmark
                ,Prices.Price AS Td_Price
                ,Prices.Value AS Td_Value
                ,Quote_Date AS Quote_Date
                ,ShareValue AS Open_ShareValue
                ,EffQuote_date AS EffQuote_Date
                ,Net_Amount AS Subscription_Amount
                ,Number_Shares AS Number_Shares

        FROM
            Share_Operations
        LEFT JOIN Prices ON @Refdate=Prices.Date AND Share_Operations.Id_Product=Prices.Id_Product
        LEFT JOIN Entities ON Share_Operations.Id_Entity=Entities.Id
        WHERE
            -- Get Subscriptions em aberto 
            (Close_Date >= @Refdate OR Close_Date IS NULL)
            -- Get apenas aplicações
            AND Share_Operations.Number_Shares > 0
            -- Operações de Aplicação
            AND Movimentação='Aplicação'
            -- Pega dados de acordo com a data
            AND Quote_Date<=@Refdate

            AND @Refdate>=Entities.Inception_Date AND (@Refdate<=Entities.Liquidation_Date OR Entities.Liquidation_Date IS NULL)"""

        return query

    def QShare_RedemptionsUpToRefdate(self):

        query = f"""
        DECLARE @Refdate DATE

        SET @Refdate='{self.Refdate}'

        SELECT
            *
        FROM
            Share_Redemptions
        WHERE
            Refdate<@Refdate"""

        return query

    def QShare_PFeeControl(self):

        query = f"""
            DECLARE @Refdate DATE

            SET @Refdate='{self.Refdate}'

            SELECT
                *
            FROM
                Share_Subscriptions_PFeeControl
            WHERE
                PFee_PaymentDate<=@Refdate"""

        return query

    def QIndexesValue_PFee(self):

        query = f"""
        SELECT
            Date
            ,Id_RiskFactor
            ,Value 
        FROM 
            IndexesValue 
        WHERE 
            Id_RiskFactor IN (SELECT DISTINCT Id_RiskfactorBenchmark FROM Entities WHERE Id_RiskfactorBenchmark IS NOT NULL) 
        ORDER BY 
            Id_RiskFactor, Date"""

        return query

    def QTd_Redemptions(self):

        query = f"""
        DECLARE @Refdate AS DATE
        SET @Refdate = '{self.Refdate}'

        SELECT
           *
        FROM
            Share_Operations
        WHERE
        -- Operações cuja Refdate=@Refdate e sejam Resgates
            Quote_Date=@Refdate
            AND (Number_Shares<0 AND Movimentação IN ('Resgate', 'Resgate Total', 'Come Cotas'))"""

        return query

    def QComeCotas_DF(self):

        query = f"""
        DECLARE @Refdate DATE
        SET @Refdate = '{self.Refdate}'

        SELECT 
            @RefDate AS Refdate
            ,Id_ShareMov AS Id_ShareMov
            ,Share_Mov.Tag AS Id_CloseShareMov
            ,Share_Mov.Id_Shareholder AS Id_Shareholder
            ,Share_Operations.Id_Product AS Id_Product
            ,Share_Operations.Request_Date AS Request_Date
            ,Share_Operations.ShareValue AS Td_Value
            ,Share_Operations.Net_Amount AS Closed_Amount
            ,Share_Operations.Number_Shares AS Closed_Shares
        FROM 
            Share_Operations
        LEFT JOIN Share_Mov ON Share_Operations.Id_ShareMov=Share_Mov.Id
        WHERE
            Share_Operations.Quote_Date=@Refdate
            AND Movimentação IN ('Come Cotas')"""

        return query

    def QShare_Operations_History_DF(self):

        query = f"""
        DECLARE @Refdate DATE
        DECLARE @YdDate DATE

        SET @Refdate='{self.Refdate}'
        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;

        SELECT

            -- // Dados do Investimento // --

            @Refdate AS Refdate
            ,Share_Operations.Shareholder_Id AS Id_Shareholder
            ,Share_Operations.Shareholder_Name AS Shareholder
            ,Share_Operations.Id_Product AS Id_Product
            ,Share_Operations.Product_name AS Product
            ,Share_Operations.Number_Shares AS Number_Shares
            ,Share_Operations.Net_Amount AS Net_Amount
            ,Share_Operations.Movimentação AS Mov_Type
            ,Share_Operations.EffQuote_date AS EffQuote_date
            ,Share_Operations.ShareValue AS Quote_Value
                        
        FROM
            Share_Operations
            LEFT JOIN Entities ON Entities.Id=Share_Operations.Id_Entity
        WHERE
            Share_Operations.EffQuote_date <= @Refdate
            AND @Refdate>=Entities.Inception_Date AND (@Refdate<=Entities.Liquidation_Date OR Entities.Liquidation_Date IS NULL)
        ORDER BY
            EffQuote_date
        """

        return query

    def QEntities_AdmRebate(self):

        query = f"""
        DECLARE @Refdate DATE
        SET @Refdate='{self.Refdate}'

        SELECT
            Entities.Id AS Id_Entity
            ,Entities.Name AS Entity
            ,Entities.Id_Product AS Id_Product
            ,Entities.Id_PFeeAdmType AS Id_PFeeAdmType
            ,PFeeAdmType.Value AS Value
        FROM
            Entities
        LEFT JOIN PFeeAdmType ON Entities.Id_PFeeAdmType=PFeeAdmType.Id
        WHERE
            Inception_Date<=@Refdate
            AND (Liquidation_Date>=@Refdate OR Liquidation_Date IS NULL)
        """

        return query

    def QShareholders_AdmRebate(self):

        query = f"""
        SELECT
            Shareholders.Id AS Id_Shareholder
            ,Shareholders.Name AS Shareholder
            ,Shareholders_Distribuidores.Id AS Id_Shareholders_Distribuidores
            ,Shareholders_Distribuidores.Name AS Shareholder_Distribuidor
            ,Shareholders_Distribuidores_Rebate.Id AS Id_Shareholders_Distribuidores_Rebate
            ,Shareholders_Distribuidores_Rebate.Amount AS Rebate_Amount
            ,Shareholders_Distribuidores_Rebate.Comment AS Comment
        FROM
            Shareholders
        LEFT JOIN Shareholders_Distribuidores ON Shareholders.Id_Shareholders_Distribuidores=Shareholders_Distribuidores.Id
        LEFT JOIN Shareholders_Distribuidores_Rebate ON Shareholders_Distribuidores.Id_RebateType=Shareholders_Distribuidores_Rebate.Id
        """

        return query

    def QEntityShare_Value(self):

        query = f"""
        DECLARE @Refdate DATE
        DECLARE @YdDate DATE

        SET @Refdate='{self.Refdate}'
        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;

        SELECT 
            Id AS Id_Entity
            ,Name AS Entity
            ,@Refdate AS Refdate
            ,Entities.Id_Product AS Id_Product
            ,Entities.IdCcy AS Id_Currency
            ,TdPrices.Price AS TdPrice
            ,TdPrices.Value AS TdValue
            ,YdPrices.Price AS YdPrice
            ,YdPrices.Value AS YdValue
        FROM 
            Entities
        LEFT JOIN Prices AS TdPrices ON TdPrices.Id_Product=Entities.Id_Product
            AND TdPrices.Date=@Refdate
        LEFT JOIN Prices AS YdPrices ON YdPrices.Id_Product=Entities.Id_Product
            AND YdPrices.Date=@YdDate
        WHERE
            Inception_Date<=@Refdate
            AND (Liquidation_Date>=@Refdate OR Liquidation_Date IS NULL)

        """

        return query

    def QEntityShare_Value_History(self):

        query = f"""
        SELECT 
            Date AS Refdate
            ,Id_Product AS Id_Product
            ,Price AS Price
            ,Value AS Value
        FROM
            Prices
        WHERE
            Id_Product IN (SELECT DISTINCT Id_Product FROM Entities WHERE Id_Product IS NOT NULL)
            AND Date<='{self.Refdate}'
        ORDER BY
            Date
            ,Id_Product
        """

        return query

    def QBenchmark_Value(self):

        query = f"""
        SELECT 
            Date AS Refdate
            ,Id_RiskFactor AS Id_RiskFactorBenchmark
            ,Value AS Value
        FROM 
            IndexesValue 
        WHERE
            Id_RiskFactor IN (SELECT Id_RiskfactorBenchmark FROM Entities)
            AND Date<='{self.Refdate}'
        ORDER BY
            Date
            ,Id_RiskFactor
        """

        return query

    def QShare_ProdDescription(self):

        query = f"""
        SELECT 
            Id AS Id_Entity
            ,Name AS Entity
            ,IdCcy AS Id_Currency
            ,Id_Product AS Id_Product
        FROM 
            Entities
        """

        return query

    def QFeriados_BRA(self):

        query = f"""
        SELECT
            *
        FROM
            Feriados_BRA
        ORDER BY
            Data
        """

        return query

    ################################# UPDATES #################################

    def UpShareOperations_ResetCloseDate(self):
        
        query = f"""
        UPDATE
            Share_Operations
        SET
            Close_Date=NULL
            ,Closed=NULL
        WHERE
            Id_ShareMov IN
        (SELECT 
            Id_ShareMov 
        FROM 
            Share_Operations
        WHERE
            Close_Date='{self.Refdate}')
        """

        return query

    ################################# DELETE #################################

    def DelShare_Operations(self):

        query = f"""
        DELETE FROM Share_Operations WHERE Quote_Date='{self.Refdate}'
        """
        return query

    def DelShare_Subscriptions(self):

        query = f"""
        DELETE FROM Share_Subscriptions WHERE Refdate='{self.Refdate}'
        """
        return query

    def DelShare_Redemptions(self):

        query = f"""
        DELETE FROM Share_Redemptions WHERE Refdate='{self.Refdate}'
        """
        return query

    def DelShareholder_Position(self):

        query = f"""
        DELETE FROM Shareholder_Position WHERE Refdate='{self.Refdate}'
        """
        return query
    
    def DelPassive_Funds(self):

        query = f"""
        DELETE FROM Passive_Funds WHERE Refdate='{self.Refdate}'
        """

        return query

    def DelShareholders_Distribuidores_Rebate_Amount(self):

        query = f"""
        DELETE FROM Shareholders_Distribuidores_Rebate_Amount WHERE Refdate='{self.Refdate}'
        """

        return query