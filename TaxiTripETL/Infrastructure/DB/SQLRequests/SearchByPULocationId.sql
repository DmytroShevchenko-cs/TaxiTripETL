SELECT TOP (@Take)
       [TpepPickupDatetime],
       [TpepDropoffDatetime],
       [PassengerCount],
       [TripDistance],
       [StoreAndFwdFlag],
       [PULocationID],
       [DOLocationID],
       [FareAmount],
       [TipAmount]
FROM [dbo].[TaxiTrips]
WHERE [PULocationID] = @PULocationID
ORDER BY [TpepPickupDatetime] DESC;


