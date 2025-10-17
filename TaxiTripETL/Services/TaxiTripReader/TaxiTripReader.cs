using System.Globalization;
using System.Runtime.InteropServices;
using CsvHelper;
using CsvHelper.Configuration;
using TaxiTripETL.Infrastructure.Models.TaxiTrip;

namespace TaxiTripETL.Services.TaxiTripReader;

public sealed class TaxiTripReader : ITaxiTripReader
{

    private static readonly Dictionary<string, string> PropertyToHeaderMapping = new(StringComparer.OrdinalIgnoreCase)
    {
        { nameof(TaxiTrip.PickupDateTime), "tpep_pickup_datetime" },
        { nameof(TaxiTrip.DropoffDateTime), "tpep_dropoff_datetime" },
        { nameof(TaxiTrip.PassengerCount), "passenger_count" },
        { nameof(TaxiTrip.TripDistance), "trip_distance" },
        { nameof(TaxiTrip.StoreAndFwdFlag), "store_and_fwd_flag" },
        { nameof(TaxiTrip.PULocationID), "pulocationid" },
        { nameof(TaxiTrip.DOLocationID), "dolocationid" },
        { nameof(TaxiTrip.FareAmount), "fare_amount" },
        { nameof(TaxiTrip.TipAmount), "tip_amount" }
    };

    /// <inheritdoc />
    public async Task ValidateFileAsync(string inputCsvPath, CancellationToken cancellationToken)
    {
        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            PrepareHeaderForMatch = args => args.Header?.Trim().ToLowerInvariant(),
        };

        using var reader = new StreamReader(inputCsvPath);
        using var csv = new CsvReader(reader, csvConfig);

        if (await csv.ReadAsync())
        {
            csv.ReadHeader();
            ValidateHeader(csv.HeaderRecord);
        }
        else
        {
            throw new InvalidDataException("The CSV file is empty or does not contain a header.");
        }
    }

    /// <inheritdoc />
	public async IAsyncEnumerable<TaxiTrip> ReadAsync(string inputCsvPath, bool convertToUtc, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
	{
		var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
		{
			HasHeaderRecord = true,
			DetectDelimiter = true,
			TrimOptions = TrimOptions.Trim,
			IgnoreBlankLines = true,
			PrepareHeaderForMatch = args => args.Header?.Trim().ToLowerInvariant(),
		};

		var utcTimeZone = TimeZoneInfo.Utc;

		using var reader = new StreamReader(inputCsvPath);
		using var csv = new CsvReader(reader, csvConfig);

        // Header is assumed to be valid at this point because ValidateFileAsync should be called first.
		await csv.ReadAsync();
        csv.ReadHeader();

		while (await csv.ReadAsync())
		{
			cancellationToken.ThrowIfCancellationRequested();

            if (!TryParseDateTime(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.PickupDateTime)]), out var pickup) ||
                !TryParseDateTime(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.DropoffDateTime)]), out var dropoff))
            {
                continue;
            }

			if (convertToUtc)
			{
				pickup = ConvertToUtc(pickup, utcTimeZone);
				dropoff = ConvertToUtc(dropoff, utcTimeZone);
			}

			yield return new TaxiTrip
			{
				PickupDateTime = pickup,
				DropoffDateTime = dropoff,
				PassengerCount = TryParseInt(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.PassengerCount)])),
				TripDistance = TryParseDouble(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.TripDistance)])),
				StoreAndFwdFlag = NormalizeFlag(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.StoreAndFwdFlag)])),
				PULocationID = TryParseInt(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.PULocationID)])),
				DOLocationID = TryParseInt(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.DOLocationID)])),
				FareAmount = TryParseDecimal(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.FareAmount)])),
				TipAmount = TryParseDecimal(csv.GetField(PropertyToHeaderMapping[nameof(TaxiTrip.TipAmount)]))
			};
		}
	}

    private static void ValidateHeader(string[]? headerRecord)
    {
        if (headerRecord == null)
        {
            throw new InvalidDataException("The CSV file is empty or does not contain a valid header.");
        }

        var requiredHeaders = new HashSet<string>(PropertyToHeaderMapping.Values, StringComparer.OrdinalIgnoreCase);
        var actualHeaders = new HashSet<string>(headerRecord, StringComparer.OrdinalIgnoreCase);

        var missingHeaders = requiredHeaders.Where(h => !actualHeaders.Contains(h)).ToList();

        if (missingHeaders.Any())
        {
            throw new InvalidDataException($"The selected CSV file is not a valid taxi trip file. It is missing the following required columns: {string.Join(", ", missingHeaders)}");
        }
    }

	private static bool TryParseDateTime(string? input, out DateTime value)
	{
		if (string.IsNullOrWhiteSpace(input))
		{
			value = default;
			return false;
		}
		var formats = new[] { "MM/dd/yyyy hh:mm:ss tt", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.fff", "M/d/yyyy H:mm", "M/d/yyyy H:mm:ss" };
		if (DateTime.TryParseExact(input.Trim(), formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dt))
		{
			value = dt;
			return true;
		}
		if (DateTime.TryParse(input.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dt))
		{
			value = dt;
			return true;
		}
		value = default;
		return false;
	}

	private static DateTime ConvertToUtc(DateTime local, TimeZoneInfo targetTimeZone)
	{
		if (local.Kind == DateTimeKind.Utc) return local;
		var unspecified = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
		return TimeZoneInfo.ConvertTimeToUtc(unspecified, TimeZoneInfo.FindSystemTimeZoneById(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Eastern Standard Time" : "America/New_York"));
	}

	private static int TryParseInt(string? input) => int.TryParse(input?.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;
    private static double TryParseDouble(string? input) => double.TryParse(input?.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : 0.0;
	private static decimal TryParseDecimal(string? input) => decimal.TryParse(input?.Trim(), NumberStyles.Number | NumberStyles.AllowExponent, CultureInfo.InvariantCulture, out var v) ? v : 0m;
	private static string NormalizeFlag(string? input)
	{
		var v = (input ?? string.Empty).Trim();
		if (string.Equals(v, "Y", StringComparison.OrdinalIgnoreCase)) return "Yes";
		if (string.Equals(v, "N", StringComparison.OrdinalIgnoreCase)) return "No";
		return v;
	}
}
