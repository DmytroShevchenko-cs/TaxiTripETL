-- Main table for taxi trips
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[TaxiTrips]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[TaxiTrips](
        [Id] [bigint] IDENTITY(1,1) NOT NULL,
        [TpepPickupDatetime] [datetime2](7) NOT NULL,
        [TpepDropoffDatetime] [datetime2](7) NOT NULL,
        [PassengerCount] [int] NOT NULL,
        [TripDistance] [float] NOT NULL,
        [StoreAndFwdFlag] [varchar](3) NOT NULL,
        [PULocationID] [int] NOT NULL,
        [DOLocationID] [int] NOT NULL,
        [FareAmount] [decimal](18, 2) NOT NULL,
        [TipAmount] [decimal](18, 2) NOT NULL,
        CONSTRAINT [PK_TaxiTrips] PRIMARY KEY CLUSTERED ([Id] ASC)
    );
END
GO

-- Unique index to enforce business rule and prevent duplicate records in the main table
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TaxiTrips_UniqueTrip' AND object_id = OBJECT_ID('[dbo].[TaxiTrips]'))
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_TaxiTrips_UniqueTrip]
    ON [dbo].[TaxiTrips] ([TpepPickupDatetime], [TpepDropoffDatetime], [PassengerCount]);
END
GO
