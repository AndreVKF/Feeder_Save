#### Queries para puxar dados dos ativos ###


class Queries():

    # # Construtor
    # def __init__(self, refdate):
    #     self.refdate = refdate

    # Put quotes
    def putQuotes(self, texto):
        return "'" + texto + "'"

    # Get Octante Rating Scale
    def getOctante_Ratings(self):
        query = """
                SELECT
                    *
                FROM
                    Ratings
                """
        return query

    # Get Riskfactors and Riscktickers
    def getRiskfactors(self):
        query = """
                SELECT
                    Id,
                    Name,
                	RiskTicker
                FROM
                	RiskFactor
                WHERE
                	IsIndex=1
                ORDER BY
                	Name
                """
        return query

    # Exec Save Position Bonds
    def Exec_PositionBonds(self, Refdate):
        query = f"""
                EXEC SavePosition_Bonds {self.putQuotes(str(Refdate))}
                """
        return query

    # Delete Indexes Value da data Refdate
    def Delete_Index_Date(self, Refdate, Id_Riskfactor):
        query = """
                DELETE FROM IndexesValue
                WHERE Date=%s AND Id_Riskfactor IN (%s)
                """ % (self.putQuotes(str(Refdate)), Id_Riskfactor)

        return query

    # Get query para deletar historico dos ativos
    def Delete_Prod_Prices(self, Refdate, Id_Products):

        query = """
                DELETE FROM Prices WHERE Date=%s AND Id_Product IN (%s)
                """ % (self.putQuotes(str(Refdate)), Id_Products)

        return query

    # Get query para deletar historico dos coupons na tabela AssetEvents
    def Delete_Coupons_AssetEvents(self, Refdate, Id_Products):

        query = """
                DELETE FROM AssetEvents WHERE Date=%s AND Id_Product IN (%s)
                """ % (self.putQuotes(str(Refdate)), Id_Products)

        return query

    # Get query para deletar historico dos coupons na tabela BondsRating
    def Delete_Bonds_Ratings(self, Refdate, Id_Products):

        query = """
                DELETE FROM BondsRating WHERE Date=%s AND Id_Product IN (%s)
                """ % (self.putQuotes(str(Refdate)), Id_Products)

        return query

    # Get YdDate da tabela de prices
    def getYd_Date(self, Refdate):

        strRefdate = str(Refdate)

        query = """
                DECLARE @RefDate DATE
                SET @RefDate = %s

                SELECT top 1 [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;
                """ % (self.putQuotes(strRefdate))

        return query

    # Get Prices of Entities que ainda não foram atualizadas
    def getMissing_EntQuotesPx(self, Refdate, Yd_Date):

        query = f"""
                SELECT
                	DISTINCT Entities.Id_Product
                	,Products.Name
                	,Td_Px.Price AS Td_Px
                	,Yd_Px.Price AS Yd_Px
                    ,Yd_Px.Value AS Yd_Value
                    ,Yd_Px.Delta1$ AS Delta1$
                    ,Yd_Px.Delta2$ AS Delta2$
                FROM
                	Entities
                LEFT JOIN Products ON Entities.Id_Product=Products.Id
                LEFT JOIN Prices AS Td_Px ON Td_Px.Id_Product=Products.Id AND
                	Td_Px.Date='{Refdate}'
                LEFT JOIN Prices AS Yd_Px ON Yd_Px.Id_Product=Products.Id AND
                	Yd_Px.Date='{Yd_Date}'
                WHERE
                	Td_Px.Price IS NULL
                	AND Yd_Px.Price IS NOT NULL
                """
        return query

    # Get all FX Products
    def getFX_SPOT(self, Refdate):

        FX_Refdate = str(Refdate)[4:6] + str(Refdate)[6:8] + str(Refdate)[2:4] + ' Curncy'

        query = """
                SELECT DISTINCT
                	 Products.Id as Id_Product
                    ,Products.name as Product
                    ,REPLACE(Products.BBGTicker, 'Refdate', '%s') as BBGTicker
                    ,Instruments.Name as Instrument
                    ,AssetGroups.Name as AssetGroup
                	,Currencies.IsInv as IsInv
                FROM
                	Products
                LEFT JOIN Instruments ON Products.Id_Instrument=Instruments.Id
                LEFT JOIN AssetGroups ON Instruments.Id_AssetGroup=AssetGroups.Id
                LEFT JOIN Currencies ON Currencies.Id_Product=Products.Id
                WHERE
                	Instruments.Name='FX Spot'
                	-- Remove casado
                	AND Products.Id NOT IN (5103)
                """ % FX_Refdate

        return query

    # Get all FX Products
    def getBDHF_FX(self, Refdate):

        FX_Refdate = str(Refdate)[4:6] + str(Refdate)[6:8] + str(Refdate)[2:4] + ' Curncy'

        query = """
                SELECT DISTINCT
                	 Products.Id as Id_Product
                    ,Products.name as Product
                    ,REPLACE(Products.BBGTicker, 'Refdate', '%s') as BBGTicker
                    ,Instruments.Name as Instrument
                    ,AssetGroups.Name as AssetGroup
                	,Currencies.IsInv as IsInv
                FROM
                	Products
                LEFT JOIN Instruments ON Products.Id_Instrument=Instruments.Id
                LEFT JOIN AssetGroups ON Instruments.Id_AssetGroup=AssetGroups.Id
                LEFT JOIN Currencies ON Currencies.Id_Product=Products.Id
                WHERE
                	Instruments.Name='FX Spot'
                	-- Remove casado
                	AND Products.Id NOT IN (5103, 2492, 2587)
                """ % FX_Refdate

        return query

    # Get query ativos com posição de acordo com o AssetGroup
    def getPos_by_AssetGroup(self, refdate, assetGroup):

        # Refdate to string
        strRefdate = str(refdate)
        FX_Refdate = str(refdate)[4:6] + str(refdate)[6:8] + str(refdate)[2:4] + ' Curncy'
        bbg_tck = 'prodname.BBGTicker'

        # Switch case Asset group
        if assetGroup == "Equity":
            asset = assetGroup

        elif assetGroup == "Bond":
            asset = assetGroup
            bbg_tck = """
                     CASE
		                  WHEN (prodname.BBGTicker LIKE '%Corp' AND Inst.Name <> 'Letras Financeiras do Tesouro (LFT)') THEN REPLACE(prodname.BBGTicker, ' Corp', '@BGN Corp')
            			  WHEN prodname.BBGTicker LIKE '%Govt' THEN REPLACE(prodname.BBGTicker, ' Govt', '@BGN Corp')
            			  WHEN Inst.Name = 'Letras Financeiras do Tesouro (LFT)'  THEN REPLACE(prodname.BBGTicker, ' Corp', '@ANBE Corp')
                     END
                     """

        elif assetGroup == "FX Forwards":
            asset = assetGroup
            bbg_tck = "CONCAT(prodname.BBGTicker,'" + FX_Refdate + "')"

        elif assetGroup == "Listed Opt":
            asset = assetGroup

        elif assetGroup == "Futures":
            asset = assetGroup

        elif assetGroup == "Funds":
            asset = assetGroup

        # Main query
        query = f"""
        DECLARE @Refdate DATE
        DECLARE @YdDate DATE

        SET @Refdate={self.putQuotes(strRefdate)}

        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;

        SELECT DISTINCT
	       prodname.Id as Id_Product
           ,Prodname.name as Product
           ,prodname.Id_Currency as Id_Currency
           ,{bbg_tck} as BBGTicker
           ,Prodname.BBGTicker as BBGTicker_Alt
           ,Prodname.Underlying as Underlying
           ,Prodname.Valuation as Valuation
           ,Inst.Name as Instrument
           ,Fut_Cont_Size as Fut_Cont_Size
           ,Fut_Val_Pt as Fut_Val_Pt
           ,Opt_Val_Pt as Opt_Val_Pt
           ,AssetGroup.name as AssetGroup


	from
		--Main Trade Table:
		(select [Trades].TradeDate, [Trades].Id_Product, [Trades].Id_Strategy,[Trades].Id_Entity, [Trades].Id_Account,[Trades].Quantity,[Trades].Price,[Trades].Value from Trades where Trades.TradeDate <= @Refdate) as Pos
		--Joins:
		inner join (select * from [Products] where ([Products].Expiration >= @Refdate or [Products].Expiration is null) and [Products].Id_Instrument NOT IN (83, 84, 85, 96)) as Prodname on Prodname.Id = Pos.Id_Product
		inner join (select * from [Instruments] where [Instruments].Id_CashFlow<>10) as Inst on Inst.id = Prodname.Id_instrument
		inner join (select * from [AssetGroups]) as AssetGroup on AssetGroup.id = Inst.Id_AssetGroup

    where
    	Inst.Id_CashFlow not in (7, 10)
    	AND AssetGroup.name={self.putQuotes(asset)}

    group by
    Prodname.name, [prodname].Id_RiskFactor1, Prodname.Expiration, prodname.Id, Pos.Id_Strategy,Pos.Id_Product, prodname.Id_Currency, Prodname.BBGTicker,Prodname.Underlying,
    Pos.Id_Account,
    Inst.Name, Inst.Id_AssetGroup,Inst.Id,
    Inst.Id_Currency,
    Pos.Id_Entity,
    Prodname.Id_Instrument,
    Inst.Id_CashFlow,
    AssetGroup.name,
    Fut_Cont_Size,
    Fut_Val_Pt,
    Opt_Val_Pt,
    Prodname.Valuation

    --Removing No position holded or traded asset:
    	HAVING sum(case when pos.TradeDate < @Refdate then pos.Quantity end) <> 0
    	or sum(case when pos.TradeDate = @Refdate and pos.Quantity > 0 then pos.Quantity end) <> 0
    	or sum(case when pos.TradeDate = @Refdate and pos.Quantity < 0 then pos.Quantity end) <> 0

    order by
    	Prodname.Name

        """
        # % (self.putQuotes(strRefdate), bbg_tck, self.putQuotes(asset))

        # Return query
        return query

    # Get query ativos com posição de acordo com o AssetGroup
    def getPos_by_Instrument(self, Refdate, Instrument):

        # Refdate to string
        strRefdate = str(Refdate)

        if Instrument == "Certificado de Recebíveis do Agronegócio (CRA)":
            Inst = Instrument

        # Main query
        query = f"""
        DECLARE @Refdate DATE
        DECLARE @YdDate DATE

        SET @Refdate={self.putQuotes(strRefdate)}

        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;

        SELECT DISTINCT
	       prodname.Id as Id_Product
           ,Prodname.name as Product
           ,prodname.Id_Currency as Id_Currency
           ,Prodname.BBGTicker as BBGTicker
           ,Prodname.Underlying as Underlying
           ,Prodname.Info as Info
           ,Fut_Cont_Size as Fut_Cont_Size
           ,Fut_Val_Pt as Fut_Val_Pt
           ,Opt_Val_Pt as Opt_Val_Pt
           ,Inst.Name as Instrument
           ,AssetGroup.name as AssetGroup


	from
		--Main Trade Table:
		(select [Trades].TradeDate, [Trades].Id_Product, [Trades].Id_Strategy,[Trades].Id_Entity, [Trades].Id_Account,[Trades].Quantity,[Trades].Price,[Trades].Value from Trades where Trades.TradeDate <= @Refdate) as Pos
		--Joins:
		inner join (select * from [Products] where ([Products].Expiration >= @Refdate or [Products].Expiration is null) and [Products].Id_Instrument NOT IN (83, 84, 85, 96)) as Prodname on Prodname.Id = Pos.Id_Product
		inner join (select * from [Instruments] where [Instruments].Id_CashFlow<>10) as Inst on Inst.id = Prodname.Id_instrument
		inner join (select * from [AssetGroups]) as AssetGroup on AssetGroup.id = Inst.Id_AssetGroup

    where
    	Inst.Id_CashFlow not in (7, 10)
    	AND Inst.Name={self.putQuotes(Inst)}

    group by
    Prodname.name, [prodname].Id_RiskFactor1, Prodname.Expiration, prodname.Id, Pos.Id_Strategy,Pos.Id_Product, prodname.Id_Currency, Prodname.BBGTicker,Prodname.Underlying,
    Pos.Id_Account,
    Inst.Name, Inst.Id_AssetGroup,Inst.Id,
    Inst.Id_Currency,
    Pos.Id_Entity,
    Prodname.Id_Instrument,
    Inst.Id_CashFlow,
    Prodname.Info,
    Fut_Cont_Size,
    Fut_Val_Pt,
    Opt_Val_Pt,
    AssetGroup.name


    --Removing No position holded or traded asset:
    	HAVING sum(case when pos.TradeDate < @Refdate then pos.Quantity end) <> 0
    	or sum(case when pos.TradeDate = @Refdate and pos.Quantity > 0 then pos.Quantity end) <> 0
    	or sum(case when pos.TradeDate = @Refdate and pos.Quantity < 0 then pos.Quantity end) <> 0

    order by
    	Prodname.Name

        """
        # % (self.putQuotes(strRefdate), bbg_tck, self.putQuotes(asset))

        # Return query
        return query

    # Get query ativos com posição
    def getOpt_Underlying(self, refdate):

        # Refdate to string
        strRefdate = str(refdate)

        # Query
        query = """
        DECLARE @YdDate DATE
        DECLARE @RefDate DATE

        SET @RefDate = %s

        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;


        SELECT DISTINCT
	       prodname.Underlying as Underlying

      from
		    --Main Trade Table:
	        (select [Trades].TradeDate, [Trades].Id_Product, [Trades].Id_Strategy,[Trades].Id_Entity, [Trades].Id_Account,[Trades].Quantity,[Trades].Price,[Trades].Value from Trades where Trades.TradeDate <= @Refdate) as Pos
            --Joins:
	        inner join (select * from [Products] where ([Products].Expiration >= @Refdate or [Products].Expiration is null) and [Products].Id_Instrument NOT IN (83, 84, 85, 96)) as Prodname on Prodname.Id = Pos.Id_Product
	        inner join (select * from [Instruments] where [Instruments].Id_CashFlow<>10) as Inst on Inst.id = Prodname.Id_instrument
		    inner join (select * from [AssetGroups]) as AssetGroup on AssetGroup.id = Inst.Id_AssetGroup
		    inner join (select * from [RiskFactor]) as Risk1 on risk1.id = prodname.id_riskfactor1

      where
	        Inst.Id_CashFlow not in (7, 10)
	        AND AssetGroup.name='Listed Opt'

      group by
	     prodname.Underlying as Underlying

--Removing No position holded or traded asset:
	HAVING sum(case when pos.TradeDate < @Refdate then pos.Quantity end) <> 0
	or sum(case when pos.TradeDate = @Refdate and pos.Quantity > 0 then pos.Quantity end) <> 0
	or sum(case when pos.TradeDate = @Refdate and pos.Quantity < 0 then pos.Quantity end) <> 0

        """ % (self.putQuotes(strRefdate))

        # Return queryf
        return query

    # Get query ativos que devem ter preco 1
    def getPos_PriceUmDF(self, refdate):

        # Refdate to string
        strRefdate = str(refdate)

        query = """
		DECLARE @YdDate DATE
        DECLARE @RefDate DATE

        SET @RefDate = %s

        SELECT top 1 @YdDate = [date] from Prices where [date] < @Refdate ORDER BY [date] DESC;


        SELECT DISTINCT
	       prodname.Id as Id_Product
           ,Prodname.name as Product
           ,prodname.Id_Currency as Id_Currency
           ,prodname.BBGTicker as BBGTicker
           ,Prodname.BBGTicker as BBGTicker_Alt
           ,Inst.Name as Instrument
           ,AssetGroup.name as AssetGroup

      from
		    --Main Trade Table:
	        (select [Trades].TradeDate, [Trades].Id_Product, [Trades].Id_Strategy,[Trades].Id_Entity, [Trades].Id_Account,[Trades].Quantity,[Trades].Price,[Trades].Value from Trades where Trades.TradeDate <= @Refdate) as Pos
            --Joins:
	        inner join (select * from [Products] where ([Products].Expiration >= @Refdate or [Products].Expiration is null) and [Products].Id_Instrument NOT IN (83, 84, 85, 96)) as Prodname on Prodname.Id = Pos.Id_Product
	        inner join (select * from [Instruments] where [Instruments].Id_CashFlow<>10) as Inst on Inst.id = Prodname.Id_instrument
		    inner join (select * from [AssetGroups]) as AssetGroup on AssetGroup.id = Inst.Id_AssetGroup
		    inner join (select * from [RiskFactor]) as Risk1 on risk1.id = prodname.id_riskfactor1

      where
	        Inst.Id_CashFlow in (7, 10)
	        OR AssetGroup.name='Cash'

      group by
	     Prodname.name
	     ,Prodname.BBGTicker
	     ,Inst.Name
	     ,prodname.Id
	     ,AssetGroup.name
         ,prodname.Id_Currency
         """ % (self.putQuotes(strRefdate))

        return query

    # Get query dos ativos que estao faltando preco
    def getPos_PriceMiss(self, refdate):
        # Refdate to string
        strRefdate = str(refdate)

        query = f"""
            declare @Refdate as varchar(15)
            declare @YdDate as varchar(15)

            -- Colocar data a ser analisada
            set @Refdate = '{strRefdate}'

            set @YdDate = (SELECT TOP 1 Date FROM Prices WHERE Date<@Refdate ORDER BY Date DESC)

            Select DISTINCT (@Refdate) AS Refdate, Prodname.name as Product, Inst.Name as Instrument,prodname.Id as Id_Product,
             px.Price as TdPrice,
             ypx.[Price] as YdPrice,
              ccy.Name as Ccy

            	from
            		--Main Trade Table:
            		(select [Trades].TradeDate, [Trades].Id_Product, [Trades].Id_Strategy,[Trades].Id_Entity, [Trades].Id_Account,[Trades].Quantity,[Trades].Price,[Trades].Value from Trades where Trades.TradeDate <= @Refdate) as Pos
            		--Joins:
            		inner join (select * from [Products] where ([Products].Expiration >= @Refdate or [Products].Expiration is null) and [Products].Id_Instrument NOT IN (83, 84, 85, 96)) as Prodname on Prodname.Id = Pos.Id_Product
            		inner join (select * from [Instruments] where [Instruments].Id_CashFlow<>10) as Inst on Inst.id = Prodname.Id_instrument
            		inner join (select * from [Currencies]) as Ccy on ccy.id = Prodname.Id_Currency
            		left join (select * from [Prices]) as px on px.Id_Product = Prodname.Id and px.[Date] = @Refdate
            		left join (select * from [Prices]) as ypx on ypx.Id_Product = Prodname.Id and ypx.[Date] = @YdDate

            where
            	Inst.Id_CashFlow not in (7, 10)
            	AND px.Price IS NULL

            group by
            Ccy.IsInv,
            Prodname.name, [prodname].Id_RiskFactor1, Prodname.Expiration, prodname.Id, Pos.Id_Strategy,Pos.Id_Product, prodname.Id_Currency, Prodname.BBGTicker,Prodname.Underlying,
            Pos.Id_Account,
            px.Price, px.Value,px.DV01,px.Ivol,px.[delta%],px.[Delta1$],px.[Delta2$],px.[Gamma$],px.[Theta$],px.[Vega$],
            ypx.[Price], ypx.Value,
            Inst.Name, Inst.Id_AssetGroup,Inst.Id,
            ccy.Name,ccy.id,
            Inst.Id_Currency,
            Pos.Id_Entity,
            Prodname.Id_Instrument,
            Inst.Id_CashFlow

            --Removing No position holded or traded asset:
            	HAVING sum(case when pos.TradeDate < @Refdate then pos.Quantity end) <> 0
            	or sum(case when pos.TradeDate = @Refdate and pos.Quantity > 0 then pos.Quantity end) <> 0
            	or sum(case when pos.TradeDate = @Refdate and pos.Quantity < 0 then pos.Quantity end) <> 0

            order by
            	Prodname.Name
    """

        return query

    # Get products prices
    def getPrices_byProdId(self, Refdate, Id_Products):
        # Refdate to string
        strRefdate = str(Refdate)

        query = """
                DECLARE @RefDate DATE
                SET @RefDate = %s

                SELECT
                	*
                FROM
                	Prices
                LEFT JOIN Products ON Products.Id=Prices.Id_Product
                WHERE
                	Date=@RefDate
                	AND Id_Product IN (%s)
                """ % (self.putQuotes(strRefdate), Id_Products)

        return query

    # Get override prices
    def getPrices_Override(self, Refdate):
        # Refdate to string
        strRefdate = str(Refdate)

        query = """
                DECLARE @RefDate DATE
                SET @RefDate = %s

                SELECT
                	*
                FROM
                	Prices_Over
                LEFT JOIN Products ON Products.Id=Prices_Over.Id_Product
                WHERE
                	Date=@RefDate
                """ % (self.putQuotes(strRefdate))

        return query

    # Get holidays
    def getHolidays(self, Feriados):

        if Feriados == "BRA":
            query = "SELECT * FROM Feriados_BRA ORDER BY Data"
        elif Feriados == "USA":
            query = "SELECT * FROM Feriados_USA ORDER BY Data"
        elif Feriados == "BRA+USA":
            query = "SELECT * FROM Feriados_BRA_USA ORDER BY Data"
        elif Feriados == "BMF":
            query = "SELECT * FROM Feriados_BMF ORDER BY Data"

        return query

    # Get Index DI
    def getIndex_CDIAcum(self, Refdate):
        # Refdate to string
        strRefdate = str(Refdate)

        query = f"SELECT Value FROM IndexesValue WHERE Id_RiskFactor=458 AND Date='{str(Refdate)}'"

        return query

    # Get Total Amortization
    def getTotalAmort_CRA(self, Refdate, Id_Product):
        # Refdate to string
        strRefdate = str(Refdate)

        query = f"SELECT SUM(Value) FROM AssetEvents WHERE Id_Product IN ({Id_Product}) AND Date<='{str(Refdate)}' GROUP BY Value"

        return query

    # Get Products by Id
    def getProducts_byId(self, Id_Products):

        query = f"SELECT * FROM Products WHERE Id IN ({Id_Products})"

        return query

    # Get Position for Bloomberg VaR
    def getPositionVaR(self, refdate):

        query = f"""SELECT 
            RefDate
            ,PortfolioName
            ,Name
            ,BBGTicker
            ,Quantity
        FROM 
            vRskLayoutInputPORTv6 
        WHERE 
            RefDate='{refdate}'"""

        return query
