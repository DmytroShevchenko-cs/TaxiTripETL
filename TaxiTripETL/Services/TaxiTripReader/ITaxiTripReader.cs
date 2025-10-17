using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TaxiTripETL.Infrastructure.Models.TaxiTrip;

namespace TaxiTripETL.Services.TaxiTripReader;

public interface ITaxiTripReader
{
    /// <summary>
    /// Validates that the specified file has the required header columns for a taxi trip CSV.
    /// </summary>
    Task ValidateFileAsync(string inputCsvPath, CancellationToken cancellationToken);

    /// <summary>
    /// Asynchronously reads taxi trip records from the specified source path.
    /// </summary>
	IAsyncEnumerable<TaxiTrip> ReadAsync(string inputCsvPath, bool convertToUtc, CancellationToken cancellationToken);
}
