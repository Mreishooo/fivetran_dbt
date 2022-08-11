{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "BookingDate",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  fts AS (
   SELECT *  
   FROM {{ source( 'ft_mdb1_se','bi_selectforexport_bq') }}
  ),

  dpl AS ( 
    select *
    FROM {{ ref('mdb_production_location') }}
  ),
  
  dat AS ( 
    SELECT *
    FROM {{ ref('mdb_article_type') }}
  ),
  
  dp AS ( 
    SELECT *
    FROM {{ ref('mdb_production') }}
  ),
  
  dt AS ( 
    SELECT *
    FROM {{ ref('mdb_theatre') }}
  ),

  da AS ( 
    SELECT *
    FROM {{ ref('mdb_age') }}
  )

SELECT
fts.FactTicketSalesId,
	 fts.DimCountryId
    ,fts.CountryCode
    ,fts.SourceCode
    ,fts.WebOrderNumber
    ,fts.MainOrderNumber
    ,fts.SubOrderNumber
    ,fts.BarCode
    -- Transaction / status
    ,fts.TransactionType
    ,fts.CancellationStatus
    ,fts.IsReplaced
    ,fts.IsReplacement
    ,fts.ReplacementType
    ,CASE
       WHEN fts.TransactionType = 'Cancellation' AND IsReplaced  AND fts.CancellationStatus  THEN true
       ELSE false
     END                              AS IsReplacedCancellation
    --Booking Date
    ,fts.DimBookingDateId
    , PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string )) AS BookingDate
	--,ddb.MonthId                      AS BookingMonthId
    --,ddb.Month                        AS BookingMonth
	--,ddb.MonthOfYearId                AS BookingMonthOfYearId
    --,ddb.MonthOfYearShort             AS BookingMonthOfYearShort
    --,ddb.DayOfMonth                   AS BookingDayOfMonth
    --,ddb.Weekday                      AS BookingWeekday
    --,ddb.WeekdayShort                 AS BookingWeekdayShort
    --,ddb.WeekIsoOfYearShort           AS BookingWeekIsoOfYearShort
    --,ddb.Year                         AS BookingYear
    --,CONVERT(bit,CASE WHEN ddb.DimDateId < YEAR(ddb.Date) * 10000 + MONTH(GETDATE()) * 100 + DAY(GETDATE()) THEN 1 ELSE 0 END) AS IsBookingYTDOfYear
    ,fts.InflowBookingCodeGT180
    --Performance Date
    ,fts.DimPerformanceDateId 
    , PARSE_DATE("%Y%m%d", cast(DimPerformanceDateId as string )) AS PerformanceDate
	,fts.DimPerformanceTimeId
    ,fts.PerformanceDateTime
    --,ddp.Date                         AS PerformanceDate
	--,ddp.MonthId                      AS PerformanceMonthId
    --,ddp.Month                        AS PerformanceMonth
    --,ddp.MonthOfYearID                AS PerformanceMonthOfYearId
    --,ddp.MonthOfYearShort             AS PerformanceMonthOfYearShort
    --,ddp.DayOfMonth                   AS PerformanceDayOfMonth
    --,ddp.Weekday                      AS PerformanceWeekday
    --,ddp.WeekdayShort                 AS PerformanceWeekdayShort
    ,fts.PerformanceWeekdayTime
    --,ddp.WeekIsoOfYearShort           AS PerformanceWeekIsoOfYearShort
   -- ,CASE
    --   WHEN ddp.Weekday IN ('Saturday','Sunday') THEN 'Weekend'
    --   ELSE 'Weekday'
    -- END                              AS PerformanceWeekPart
    --,ddp.Year                         AS PerformanceYear
    ,fts.LeadWeeksPerformanceNumber  AS LeadWeeksPerformanceNumber
    ,fts.LeadDaysPerformanceNumber   AS LeadDaysPerformanceNumber
    -- additional dates and times
    ,dpl.ProductionLocationPremiereDate
    ,dpl.SalesStartDate
    -- Production / Prerformance
	,fts.DimProductionId
	,fts.DimArticleTypeId
    ,dat.ArticleTypeCode
    ,dpl.ProductionLocationId
	,fts.DimProductionLocationId
    ,dp.ProductionName
    ,fts.PerformanceStatus
	,fts.DimTheatreId
    ,dt.TheatreCode
    ,dt.TheatreName
    ,dt.CityCode                      AS TheatreCityCode
    ,dt.City                          AS TheatreCity
    ,fts.ProductionLocationPerformanceYear
    -- Distribution
    ,fts.SourceDistributionChannel
    ,fts.SourceDistributionChannelName
    ,fts.SourceClientId
    ,fts.SourceSalesPartner
    ,fts.LocalSalesChannel1
    ,fts.LocalSalesChannel2
    ,fts.DistributionOwner
    ,fts.DistributionPoint
    -- additional Source Information
    ,fts.SourcePromotionId
    ,fts.SourcePromotionName
    ,fts.SourcePromotionCode
    ,fts.SourcePromotionAdvertisingPartnerId
    ,fts.SourceCustomerCode
    --GoldenCustomer
	,fts.DimGoldenCustomerId
    ,fts.GoldenCustomerCurrentAge
    ,fts.GoldenCustomerGender
    ,fts.GoldenCustomerCountryCode
    ,fts.GoldenCustomerPostalCode
    ,fts.GoldenCustomerPostalCodeLength
    ,fts.IsReturningCustomerFlag
    ,fts.IsReturningCustomer
    ,fts.DimGoldenCustomerAgeAtBookingId
    ,da.AgeBinA                      AS GoldenCustomerAgeBinA
    ,da.AgeBinB                      AS GoldenCustomerAgeBinB
    ,da.AgeBinC                      AS GoldenCustomerAgeBinC
    ,da.AgeBinD                      AS GoldenCustomerAgeBinD
    ,da.AgeDeceniaBinCode            AS GoldenCustomerAgeDeceniaBinCode
    ,da.AgeDeceniaBinName            AS GoldenCustomerAgeDeceniaBinName
    --Customer
    ,fts.CustomerCurrentAge
    ,fts.CustomerGender 
    ,fts.CustomerCountryCode
    ,fts.CustomerPostalCode
    ,fts.CustomerPostalCodeLength
    -- Seat
    ,fts.SourceSeat
    ,fts.SourceSeatRow
    ,fts.SourceSeatNumber
    --PriceType
    ,fts.SourcePriceCategoryId
    ,fts.PriceCategoryName
    ,fts.PriceCategoryGroup
    ,fts.PriceCategorySortCode
	,fts.SourcePriceTypeName
    ,fts.PriceTypeName
    ,fts.PriceTypeGroup
    --sums
    ,fts.ArticleCount
    ,fts.NetPriceValueEUR
    ,fts.NetNetPriceValueEUR
    ,fts.TicketPriceValueEUR
    ,fts.CustomerPriceValueEUR
    ,fts.TPT_DEValueEUR
FROM  fts
left join  dpl    using (DimProductionLocationId)
left join   dat using (DimArticleTypeId)
left join  dp on (dp.DimProductionId=fts.DimProductionId )
left join  dt  using (DimTheatreId) 
left join  da   on (da.DimAgeId = fts.DimGoldenCustomerAgeAtBookingId)


