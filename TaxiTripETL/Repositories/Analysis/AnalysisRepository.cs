using Microsoft.Data.SqlClient;
using TaxiTripETL.Infrastructure.Models;

namespace TaxiTripETL.Repositories.Analysis;

public class AnalysisRepository : IAnalysisRepository
{
    private readonly ConnectionStringHolder _connectionStringHolder;

    public AnalysisRepository(ConnectionStringHolder connectionStringHolder)
    {
        _connectionStringHolder = connectionStringHolder;
    }

    /// <inheritdoc />
    public async Task FindLocationWithHighestAverageTipAsync()
    {
        Console.WriteLine("\nFinding PULocationID with the highest average tip...");
        var query = await File.ReadAllTextAsync($"{StaticFilePaths.QueryPath}/FindLocationWithHighestAverageTip.sql");

        await using var connection = new SqlConnection(_connectionStringHolder.ConnectionString);
        await using var command = new SqlCommand(query, connection);
        await connection.OpenAsync();

        await using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            Console.WriteLine($"  -> PULocationID: {reader["PULocationID"],-10} | Average Tip: {reader["AverageTip"]:C}");
        }
        else
        {
            Console.WriteLine("  -> No data found.");
        }
    }

    /// <inheritdoc />
    public async Task FindTop100LongestFaresByDistanceAsync()
    {
        Console.WriteLine("\nFinding top 100 longest fares by distance...");
        var query = await File.ReadAllTextAsync($"{StaticFilePaths.QueryPath}/FindTop100LongestFaresByDistance.sql");

        await using var connection = new SqlConnection(_connectionStringHolder.ConnectionString);
        await using var command = new SqlCommand(query, connection);
        await connection.OpenAsync();

        await using var reader = await command.ExecuteReaderAsync();
        var count = 1;
        while (await reader.ReadAsync())
        {
            Console.WriteLine($"  {count++,3}. Distance: {reader["TripDistance"]:N2} miles, Fare: {reader["FareAmount"]:C}, Tip: {reader["TipAmount"]:C}");
        }
    }

    /// <inheritdoc />
    public async Task FindTop100LongestFaresByTimeAsync()
    {
        Console.WriteLine("\nFinding top 100 longest fares by time...");
        var query = await File.ReadAllTextAsync($"{StaticFilePaths.QueryPath}/FindTop100LongestFaresByTime.sql");

        await using var connection = new SqlConnection(_connectionStringHolder.ConnectionString);
        await using var command = new SqlCommand(query, connection);
        await connection.OpenAsync();

        await using var reader = await command.ExecuteReaderAsync();
        var count = 1;
        while (await reader.ReadAsync())
        {
            var duration = TimeSpan.FromSeconds((int)reader["DurationInSeconds"]);
            Console.WriteLine($"  {count++,3}. Duration: {duration}, From: {reader["TpepPickupDatetime"]} To: {reader["TpepDropoffDatetime"]}");
        }
    }

    /// <inheritdoc />
    public async Task SearchByPULocationIdAsync(int puLocationId, int take = 100)
    {
        Console.WriteLine($"\nSearching trips for PULocationID = {puLocationId} (top {take})...");
        var query = await File.ReadAllTextAsync($"{StaticFilePaths.QueryPath}/SearchByPULocationId.sql");

        await using var connection = new SqlConnection(_connectionStringHolder.ConnectionString);
        await using var command = new SqlCommand(query, connection);
        command.Parameters.AddWithValue("@PULocationID", puLocationId);
        command.Parameters.AddWithValue("@Take", take);
        await connection.OpenAsync();

        await using var reader = await command.ExecuteReaderAsync();
        var count = 1;
        while (await reader.ReadAsync())
        {
            Console.WriteLine(
                $"  {count++,3}. PU: {reader["PULocationID"]}, DO: {reader["DOLocationID"]}, Pickup: {reader["TpepPickupDatetime"]}, Dropoff: {reader["TpepDropoffDatetime"]}, Distance: {reader["TripDistance"]:N2}, Fare: {reader["FareAmount"]:C}, Tip: {reader["TipAmount"]:C}");
        }
    }
}
