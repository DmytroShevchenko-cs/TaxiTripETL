SELECT TOP 1 PULocationID, AVG(TipAmount) AS AverageTip
FROM dbo.TaxiTrips
GROUP BY PULocationID
ORDER BY AverageTip DESC;
