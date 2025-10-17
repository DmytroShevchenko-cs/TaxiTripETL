using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TaxiTripETL.Infrastructure.Models.TaxiTrip;

namespace TaxiTripETL.Services.DuplicateWriter;

public sealed class DublicateTaxiTripWriter : ITaxiTripWriter
{
	private readonly StreamWriter _writer;

	public DublicateTaxiTripWriter(string outputPath)
	{
		_writer = new StreamWriter(outputPath);
	}

	/// <summary>
	/// Asynchronously writes the header for the duplicate records file.
	/// </summary>
	public async Task WriteHeaderAsync(CancellationToken cancellationToken)
	{
        var header = string.Join(",",
            nameof(TaxiTrip.PickupDateTime),
            nameof(TaxiTrip.DropoffDateTime),
            nameof(TaxiTrip.PassengerCount),
            nameof(TaxiTrip.TripDistance),
            nameof(TaxiTrip.StoreAndFwdFlag),
            nameof(TaxiTrip.PULocationID),
            nameof(TaxiTrip.DOLocationID),
            nameof(TaxiTrip.FareAmount),
            nameof(TaxiTrip.TipAmount)
        );
		await _writer.WriteLineAsync(header.AsMemory(), cancellationToken);
	}

	/// <summary>
	/// Asynchronously writes a single duplicate taxi trip record.
	/// </summary>
	public async Task WriteAsync(TaxiTrip trip, CancellationToken cancellationToken)
	{
		var line = string.Join(',',
			FormatCsv(trip.PickupDateTime),
			FormatCsv(trip.DropoffDateTime),
			trip.PassengerCount,
			trip.TripDistance,
			FormatCsv(trip.StoreAndFwdFlag),
			trip.PULocationID,
			trip.DOLocationID,
			trip.FareAmount,
			trip.TipAmount);
		await _writer.WriteLineAsync(line.AsMemory(), cancellationToken);
	}

	public async ValueTask DisposeAsync()
	{
		await _writer.FlushAsync().ConfigureAwait(false);
		await _writer.DisposeAsync();
	}

	private static string FormatCsv(object value)
	{
		return value switch
		{
			DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
			string s when s.Contains(',') => "\"" + s.Replace("\"", "\"\"") + "\"",
            string s => s,
			_ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
		};
	}
}
