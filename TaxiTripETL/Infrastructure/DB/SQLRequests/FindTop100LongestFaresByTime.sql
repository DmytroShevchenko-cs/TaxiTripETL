SELECT TOP 100
    TpepPickupDatetime,
    TpepDropoffDatetime,
    DATEDIFF(second, TpepPickupDatetime, TpepDropoffDatetime) AS DurationInSeconds
FROM dbo.TaxiTrips
ORDER BY DurationInSeconds DESC;
