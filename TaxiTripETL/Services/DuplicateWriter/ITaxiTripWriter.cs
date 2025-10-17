using System;
using System.Threading;
using System.Threading.Tasks;
using TaxiTripETL.Infrastructure.Models.TaxiTrip;

namespace TaxiTripETL.Services.DuplicateWriter;

public interface ITaxiTripWriter : IAsyncDisposable
{
	Task WriteHeaderAsync(CancellationToken cancellationToken);

	Task WriteAsync(TaxiTrip trip, CancellationToken cancellationToken);
}
