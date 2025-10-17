namespace TaxiTripETL.Repositories.Analysis;

public interface IAnalysisRepository
{
    /// <summary>
    /// Finds the pickup location (PULocationID) with the highest average tip amount and prints the result to the console.
    /// </summary>
    Task FindLocationWithHighestAverageTipAsync();

    /// <summary>
    /// Finds the top 100 longest trips by distance and prints the results to the console.
    /// </summary>
    Task FindTop100LongestFaresByDistanceAsync();

    /// <summary>
    /// Finds the top 100 longest trips by duration and prints the results to the console.
    /// </summary>
    Task FindTop100LongestFaresByTimeAsync();
}
