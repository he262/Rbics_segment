SET NOCOUNT ON
DECLARE @securityDate INT = {}
  IF OBJECT_ID('tempdb..#rbicsRevenuePercentRaw') IS NOT NULL
            DROP TABLE #rbicsRevenuePercentRaw;

        IF OBJECT_ID('tempdb..#rbicsSegmentAndRevenuePercentRaw') IS NOT NULL
            DROP TABLE #rbicsSegmentAndRevenuePercentRaw;

     
  IF OBJECT_ID('tempdb..#stoxxId') IS NOT NULL
            DROP TABLE #stoxxId;

        CREATE TABLE #stoxxId (
                              securityId INT NOT NULL,
                              stoxxId VARCHAR(12) NOT NULL
                                  PRIMARY KEY CLUSTERED ([securityId] ASC, [stoxxId] ASC)
                                  WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = ON, IGNORE_DUP_KEY = OFF,
                                        ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 95
                                       ) ON [PRIMARY]
                              );

INSERT INTO #stoxxId
(
    securityId,
    stoxxId
) 
SELECT  B.securityId,A.stoxxId FROM SID.security A 
JOIN SID.securityDescription  B  ON B.securityId = A.id 
WHERE b.vf<=@securityDate AND b.vt>@securityDate and stoxxId in ({})


 CREATE TABLE [#rbicsSegmentAndRevenuePercentRaw] (
                                                         [securityId] INT NOT NULL,
                                                         stoxxId VARCHAR(12) NOT NULL,
                                                         rbics2Segment VARCHAR(500) NOT NULL,
                                                         rbics2L6Id BIGINT NOT NULL,
                                                         [revenuePercent] VARCHAR(50)
                                                         );

  
 INSERT INTO #rbicsSegmentAndRevenuePercentRaw
                (
                    securityId,
                    stoxxId,
                    rbics2Segment,
                    rbics2L6Id,
                    revenuePercent
                )
                SELECT
                         a.securityId,
                         a.stoxxId,
                         REPLACE(REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
REPLACE( REPLACE( REPLACE(a.segmentName
						, CHAR(0x0000),'') ,CHAR(0x0001),'') ,CHAR(0x0002),'') ,CHAR(0x0003),'') ,CHAR(0x0004),'') 
,CHAR(0x0005),'') ,CHAR(0x0006),'') ,CHAR(0x0007),'') ,CHAR(0x0008),'') ,CHAR(0x000B),'') 
,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
						rbics2Segment ,
                         a.rbics2L6Id,
                         SUM(a.revenuePercent) AS revenuePercent
                FROM     (
                             SELECT
                                      s.securityId,
                                      sm.stoxxId,
                                      rev.[name]                   AS segmentName,
                                      rev.rbics2L6Id,
                                      rev.revenuePercent,
                                      cr.periodEndDate,
                                      MAX(cr.periodEndDate) OVER (PARTITION BY
                                                                      sm.stoxxId
                                                                 ) AS periodEndDateFilter
                             FROM     SIDExternal.revereLive.RBICS2CompanyReport AS cr
                                 JOIN SIDExternal.revereLive.securityMapping     AS sm
                                     ON cr.companyId = sm.rbicsCompanyId
                                         AND sm.vf   <= @securityDate
                                         AND sm.vt   > @securityDate
                                 JOIN #stoxxId                                   AS s
                                     ON sm.stoxxId   = s.stoxxId
                                 JOIN SIDExternal.revereLive.RBICS2ReportItem    AS rev
                                     ON cr.reportId  = rev.reportID
                                         AND rev.vf  <= @securityDate
                                         AND rev.vt  > @securityDate
                             WHERE    cr.vf <= @securityDate
                                 AND cr.vt  > @securityDate
                         ) a
                WHERE    periodEndDate = a.periodEndDateFilter
                GROUP BY a.securityId,
                         a.segmentName,
                         a.stoxxId,
                         a.rbics2L6Id;
 

SELECT  stoxxId,
       securityId,
       rbics2Segment,
       rbics2L6Id,
       revenuePercent FROM #rbicsSegmentAndRevenuePercentRaw