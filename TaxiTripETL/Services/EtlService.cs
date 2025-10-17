namespace TaxiTripETL.Services;

using DuplicateWriter;
using Infrastructure.Models.TaxiTrip;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Repositories.TaxiTripBulkRepository;
using TaxiTripReader;

public sealed class EtlService
{
	private readonly ITaxiTripReader _reader;
	private readonly ITaxiTripBulkRepository _repository;
	private readonly ITaxiTripWriter _taxiTripWriter;
	private readonly ILogger<EtlService> _logger;

	public EtlService(ITaxiTripReader reader, ITaxiTripBulkRepository repository, ITaxiTripWriter taxiTripWriter, ILogger<EtlService> logger)
	{
		_reader = reader;
		_repository = repository;
		_taxiTripWriter = taxiTripWriter;
		_logger = logger;
	}

	/// <summary>
	/// Runs the ETL process: reads records from the CSV, filters out duplicates found within the file, and bulk inserts the unique records into the database.
	/// </summary>
	/// <returns>The total number of records successfully inserted into the database.</returns>
	public async Task<long> RunAsync(string inputCsvPath, int batchSize, bool convertToUtc, CancellationToken cancellationToken)
	{
		await _taxiTripWriter.WriteHeaderAsync(cancellationToken);

		var seen = new HashSet<string>(StringComparer.Ordinal);
		var buffer = new List<TaxiTrip>(batchSize);
		long totalInserted = 0;

		await foreach (var trip in _reader.ReadAsync(inputCsvPath, convertToUtc, cancellationToken))
		{
            // Check for duplicates within the current CSV file
			var key = $"{trip.PickupDateTime:o}|{trip.DropoffDateTime:o}|{trip.PassengerCount}";
			if (!seen.Add(key))
			{
                await _taxiTripWriter.WriteAsync(trip, cancellationToken);
				continue;
			}

			buffer.Add(trip);
			if (buffer.Count >= batchSize)
			{
				totalInserted += await TryBulkInsertAsync(buffer, batchSize, cancellationToken);
				buffer.Clear();
			}
		}

        // Insert any remaining records in the buffer
		if (buffer.Count > 0)
		{
			totalInserted += await TryBulkInsertAsync(buffer, batchSize, cancellationToken);
			buffer.Clear();
		}

		await _taxiTripWriter.DisposeAsync();
		return totalInserted;
	}

    /// <summary>
    /// Attempts to bulk insert a batch of records, handling potential unique key violations gracefully.
    /// </summary>
    private async Task<long> TryBulkInsertAsync(IReadOnlyList<TaxiTrip> trips, int batchSize, CancellationToken cancellationToken)
    {
        try
        {
            return await _repository.BulkInsertAsync(trips, batchSize, cancellationToken);
        }
        catch (SqlException ex) when (ex.Number == 2627 || ex.Number == 2601) // Unique key violation
        {
	        _logger.LogWarning(ex, "A batch of records was skipped due to unique key violation. Batch contains duplicate entries.");
	        return 0;
        }
        catch (Exception ex)
        {
	        _logger.LogError(ex, "Unexpected error during bulk insert.");
	        throw;
        }
    }
}
