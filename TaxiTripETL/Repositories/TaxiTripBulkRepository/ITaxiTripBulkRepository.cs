using TaxiTripETL.Infrastructure.Models.TaxiTrip;

namespace TaxiTripETL.Repositories.TaxiTripBulkRepository;

public interface ITaxiTripBulkRepository
{
    /// <summary>
    /// Asynchronously performs a bulk insert of taxi trip records directly into the final table.
    /// </summary>
	Task<long> BulkInsertAsync(IReadOnlyList<TaxiTrip> trips, int batchSize, CancellationToken cancellationToken);
}
