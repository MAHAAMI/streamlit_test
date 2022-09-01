With SHIPMENT_CTE
as (
   select gf.shipment_number
    ,NC.MASTERLOADNUM
      ,gf.mode
      ,CASE
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('WA', 'OR', 'BC') THEN 'PNW'
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('CA') THEN 'PSW'
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('MA', 'NY', 'NJ', 'MD') THEN 'NE'
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('VA', 'SC', 'NC', 'GA') THEN 'SE'
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('FL', 'LA', 'TX') THEN 'GULF'
           WHEN gf.ORIGIN_PORT_STATE_PROVINCE IN ('NS', 'PQ') THEN 'CAEC'
        ELSE UPPER(SSR1.NAME) END AS PortOriginGroupings
      ,CONCAT_WS(', ', 'ORIGIN',gf.ORIGIN_PORT_COUNTRY) AS OriginCountry
      ,gf.INTERMEDIARY_PORT_NAMES
      ,CASE
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('WA', 'OR', 'BC') THEN 'PNW'
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('CA') THEN 'PSW'
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('MA', 'NY', 'NJ', 'MD') THEN 'NE'
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('VA', 'SC', 'NC', 'GA') THEN 'SE'
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('FL', 'LA', 'TX') THEN 'GULF'
           WHEN gf.DELIVERY_LOCATION_STATE_PROVINCE IN ('NS', 'PQ') THEN 'CAEC'
         ELSE UPPER(SSR2.NAME) END AS PortDischargeGroupings
      ,CONCAT_WS(', ','DELIVERY',gf.DELIVERY_LOCATION_COUNTRY) as DeliveryCountry
      ,gf.shipper_name
      ,gf.DELIVERY_DATE
      ,gf.DELIVERY_REQUESTED_DATE
      ,datediff(day,gf.DELIVERY_REQUESTED_DATE,gf.DELIVERY_DATE) as ShipmentDelay

   from GF_COMMERCIAL_DOMAIN.BROKER.GF_SHIPMENTS GF
   inner join CDC_EXPRESS.broker.dbo_nvoconsolidation nc
          on GF.Shipment_number = nc.houseloadnum 
         and nc.HVR_ISDELETE = 0
   
   inner join CDC_MDM.BROKER.MDM_COUNTRY C1
           on GF.ORIGIN_PORT_COUNTRY = C1.CODE
          and C1.HVR_ISDELETE = 0
    
   inner join CDC_MDM.BROKER.MDM_COUNTRY C2
           on GF.DESTINATION_PORT_COUNTRY = C2.CODE
          AND C2.HVR_ISDELETE = 0
    
   INNER JOIN CDC_MDM.BROKER.MDM_SHIPPINGSUBREGION SSR1
           ON C1.SHIPPINGSUBREGIONID = SSR1.SHIPPINGSUBREGIONID
          AND SSR1.HVR_ISDELETE = 0
    
  INNER JOIN CDC_MDM.BROKER.MDM_SHIPPINGSUBREGION SSR2
           ON C2.SHIPPINGSUBREGIONID = SSR2.SHIPPINGSUBREGIONID
          AND SSR2.HVR_ISDELETE = 0
    
   where GF.DELIVERY_DATE >= dateadd(day, -60, CURRENT_DATE::date)
   --and GF.DELIVERY_DATE >= ADD_MONTHS(CURRENT_DATE, -1)
      and GF.shipping_method = 'Ocean'
      AND gf.DELIVERY_REQUESTED_DATE is not null
  and gf.SHIPMENT_CUSTOMER_CODE in ('C8498384','C7357580')
  --and gf.shipment_number = 382375192
  ),
  --select * from ds_visibility.dbo.Shipment;

-- drop table if exists ds_visibility.dbo.CarrierTemp;
-- create temporary table ds_visibility.dbo.CarrierTemp
CarrierTemp_CTE
as (
    select S.masterloadnum
          ,max(LB.SEQNUM) AS MAXSeqNum
    
    from SHIPMENT_CTE S
    inner join CDC_EXPRESS.broker.dbo_loadbooks LB
    on S.masterloadnum = lB.LOADNUM
    AND LB.HVR_ISDELETE = 0
    
    where lb.bounced <> 1
    and lb.booktype in ('OLH','ALH')
    
    group by S.masterloadnum
   ),
   
   --select * from ds_visibility.dbo.CarrierTemp;

-- drop table if exists ds_visibility.dbo.Carrier;
-- create temporary table ds_visibility.dbo.Carrier
Carrier_CTE
as (
     select CT.masterloadnum
           ,lb.carriercode
           ,c.scac
               
    from CarrierTemp_CTE CT
    
    LEFT JOIN CDC_EXPRESS.broker.dbo_loadbooks LB
          ON CT.MasterLoadnum = LB.LoadNum 
         AND CT.MAXSeqNum = LB.SeqNum
         AND LB.HVR_ISDELETE = 0
    
    LEFT JOIN CDC_EXPRESS.broker.dbo_carriers C
           ON LB.CarrierCode = C.COMPANYCODE
          AND C.HVR_ISDELETE = 0

    WHERE LB.BookType IN ('OLH','ALH')
          AND LB.Bounced <> 1

      GROUP BY CT.MASTERLOADnum
             ,LB.CarrierCode
	         ,C.SCAC

    ),
--select * from ds_visibility.dbo.Carrier;

-- drop table if exists ds_visibility.dbo.DoorStops;
-- create temporary table ds_visibility.dbo.DoorStops
DoorStops_CTE
as (
    select SH.Shipment_Number
        ,max(s.stopnum) as stopnum
  
    from SHIPMENT_CTE  SH
    inner join CDC_EXPRESS.BROKER.DBO_STOPS S
            on SH.SHIPMENT_NUMBER = S.LOADNUM
           AND S.HVR_ISDELETE = 0

         where s.stoptype IN ('D','DP')
           AND S.STOPSTYLE = 'P'
      group by SH.Shipment_Number
  )
  
--drop table if exists ds_visibility.dbo.Finaltable;
--create temporary table ds_visibility.dbo.Finaltable
 -- as 
    select SH.shipment_number
        ,SH.mode
        ,SH.PortOriginGroupings
        ,SH.OriginCountry
        ,case when SH.INTERMEDIARY_PORT_NAMES is null then 'no_ip' else 'ip' end as IntermediaryPort
        ,SH.PORTDISCHARGEGROUPINGS
        --,S.STATE AS DESTINATION_PORT_STATE_PROVINCE
        /*,CASE
           WHEN S.STATE IN ('WA', 'OR', 'BC') THEN 'PNW'
           WHEN S.STATE IN ('CA') THEN 'PSW'
           WHEN S.STATE IN ('MA', 'NY', 'NJ', 'MD') THEN 'NE'
           WHEN S.STATE IN ('VA', 'SC', 'NC', 'GA') THEN 'SE'
           WHEN S.STATE IN ('FL', 'LA', 'TX') THEN 'GULF'
           WHEN S.STATE IN ('NS', 'PQ') THEN 'CAEC'
         ELSE UPPER(S.CITY) END AS PortDischargeGroupings*/
        ,SH.DeliveryCountry
        ,SH.shipper_name
        ,CASE WHEN S.STATE NOT IN ('WA', 'OR', 'BC','CA','MA', 'NY', 'NJ', 'MD','VA', 'SC', 'NC', 'GA','FL', 'LA', 'TX','NS', 'PQ') THEN 'INLAND' ELSE 'COASTAL' END AS STOPSTYLE
        ,SH.DELIVERY_DATE
        ,SH.DELIVERY_REQUESTED_DATE
        ,CONCAT(yearofweek(SH.DELIVERY_REQUESTED_DATE),'-',CASE WHEN WEEK(SH.DELIVERY_REQUESTED_DATE) < 10
                                                          THEN CONCAT('WK0',WEEK(SH.DELIVERY_REQUESTED_DATE))
                                                          ELSE CONCAT('WK',WEEK(SH.DELIVERY_REQUESTED_DATE)) END) "WEEKNUM"
        ,CASE WHEN SH.MODE = 'Full Container' THEN 'FCL'
              WHEN SH.MODE = 'Less than Container' THEN 'LCL'
              WHEN SH.MODE = 'Consolidation' THEN 'CONSOLIDATION'
              END AS ShipmentType
         ,C.SCAC AS CarrierSCAC
        ,SH.ShipmentDelay
        
    from SHIPMENT_CTE SH
    INNER JOIN DoorStops_CTE DS
            on SH.SHIPMENT_NUMBER = DS.Shipment_Number
 
    INNER JOIN CDC_EXPRESS.BROKER.DBO_STOPS S
            ON DS.Shipment_Number = S.LOADNUM
           AND DS.STOPNUM = S.STOPNUM
           and S.STOPTYPE IN ('D','DP')
           and S.STOPSTYLE = 'P'
           AND S.HVR_ISDELETE = 0
           
      left join Carrier_CTE C
      on SH.Masterloadnum =  C.masterloadnum
      where c.scac is not null      
           ;
           
           
