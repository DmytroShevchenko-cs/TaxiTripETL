namespace TaxiTripETL.Infrastructure.Models.TaxiTrip;

public sealed class TaxiTrip
{
    public DateTime PickupDateTime { get; init; }
    public DateTime DropoffDateTime { get; init; }
    public int PassengerCount { get; init; }
    public double TripDistance { get; init; }
    public string StoreAndFwdFlag { get; init; } = string.Empty;
    public int PULocationID { get; init; }
    public int DOLocationID { get; init; }
    public decimal FareAmount { get; init; }
    public decimal TipAmount { get; init; }
}
