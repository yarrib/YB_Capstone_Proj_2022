/* 
	DATA ACQUISITION V4

	get training data for baseline model from WDW
	
	using azure sql data warehouse
*/
if object_id('tempdb..#tmpFlatFact') is not null
	drop table #tmpFlatFact

select
	J.JobId, AI.AssociateID
	,O.City as OfficeCity,O.StateCode as OfficeState,left(O.Zip,5) as OfficeZip, O.OfficeName
	,A.AccountGUID, A.AccountName, A.AccountCity, A.AccountState, left(A.AccountZip,5) as AccountZip, A.AccountNaicsName
	-- NEED TO ADD ACCOUNT INFO HERE LIKE SIZE, REVENUE, ETC
	,M.MarketName
	,S.ServiceName
	,SL.ServiceLineName
	,I.IndustryName
	,J.JobTaxComplexity, J.JobName
	,C.CalendarYear
	-- aggregates
	,sum(FP.ChargeHours) as ChargeHours -- our time commitment
	,sum(FP.ProductionRevActual) as ProductionRevActual -- rev net of any adjustmnets
	,sum(FP.Cost) as Cost --cost of doing business 
	,count(DISTINCT AI.AssociateKey) as [NumAssociatesOnJob] --how many unique individuals were needed


into #tmpFlatFact
from 
	wdw.fctProduction FP
	left outer join wdw.dimOffice O on O.OfficeKey = FP.OfficeKey and O.IsCurrent = 1 -- office info - should be for the client
	left outer join wdw.dimAccount A on A.AccountKey = FP.AccountKey and A.IsCurrent = 1 -- account info
	left outer join wdw.dimAccountEntityType AET on AET.AccountEntityTypeId = A.AccountEntityTypeId -- entity type for account (used as a filter only)
	left outer join wdw.dimMarket M on M.MarketKey = FP.MarketKey and M.IsCurrent = 1 -- market info
	left outer join wdw.dimService S on S.ServiceKey = FP.ServiceKey and S.IsCurrent = 1 -- service info
	left outer join wdw.dimServiceLine SL on SL.ServiceLineId = S.ServiceLineId and SL.IsCurrent=1 -- service line info
	left outer join wdw.dimIndustry I on I.IndustryKey = FP.IndustryKey and I.IsCurrent = 1 -- industry info
	left outer join wdw.dimJob J on J.JobKey = FP.JobKey and J.IsCurrent = 1
	left outer join wdw.dimCalendar C on C.CalendarKey = FP.CalendarKey
	left outer join wdw.dimAssociate AI on AI.AssociateKey = FP.AssociateKey -- join to dim associate and distinct_count associate ids

where
	S.ServiceLineId in (1,8,17) -- a&a, tax, financial svcs
	and A.AccountRelationshipType like '%client%' -- don't want prospects
	and AET.AccountEntityTypeID not in (3,8,18) -- filter out private clients entity type not in (ind,trust,estate)
	and (C.CalendarYear between 2020 and 2022) -- fy 2020,21,22
	and J.JobStatusId = 4 -- completed jobs only
group by
	J.JobId, AI.AssociateID
	,O.OfficeId,O.City,O.StateCode,O.Zip, O.OfficeName
	,A.AccountGUID, A.AccountName, A.AccountCity, A.AccountState, A.AccountZip, A.AccountNaicsCd, A.AccountNaicsName
	,M.MarketName
	,S.ServiceName, S.ServiceLineId
	,SL.ServiceLineName
	,I.IndustryName,I.IndustryKey
	,J.JobTaxComplexity, J.JobName
	,C.CalendarYear



/* ************************* check our temp table ************************** */
select count (*) from #tmpFlatFact


select top 10 * from #tmpFlatFact order by JobId desc

select count(distinct JobId) from #tmpFlatFact
select CalendarYear, count(*) from #tmpFlatFact group by CalendarYear

/* ************************* the output we really want ************************** */
select * from #tmpFlatFact

