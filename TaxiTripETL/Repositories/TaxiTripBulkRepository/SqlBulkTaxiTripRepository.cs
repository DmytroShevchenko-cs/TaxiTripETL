using Microsoft.Data.SqlClient;
using TaxiTripETL.Infrastructure.Models.TaxiTrip;
using System.Data;

namespace TaxiTripETL.Repositories.TaxiTripBulkRepository;

public sealed class SqlBulkTaxiTripRepository : ITaxiTripBulkRepository
{
    private readonly string _connectionString;

    public SqlBulkTaxiTripRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <inheritdoc />
    public async Task<long> BulkInsertAsync(IReadOnlyList<TaxiTrip> trips, int batchSize,
        CancellationToken cancellationToken)
    {
        if (trips.Count == 0)
        {
            return 0;
        }

        var dataTable = new DataTable();
        dataTable.Columns.Add("TpepPickupDatetime", typeof(DateTime));
        dataTable.Columns.Add("TpepDropoffDatetime", typeof(DateTime));
        dataTable.Columns.Add("PassengerCount", typeof(byte));
        dataTable.Columns.Add("TripDistance", typeof(decimal));
        dataTable.Columns.Add("StoreAndFwdFlag", typeof(string));
        dataTable.Columns.Add("PULocationID", typeof(int));
        dataTable.Columns.Add("DOLocationID", typeof(int));
        dataTable.Columns.Add("FareAmount", typeof(decimal));
        dataTable.Columns.Add("TipAmount", typeof(decimal));


        long totalRowsInserted = 0;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < trips.Count; i += batchSize)
        {
            dataTable.Clear();

            int currentBatchSize = Math.Min(batchSize, trips.Count - i);

            for (int j = 0; j < currentBatchSize; j++)
            {
                var trip = trips[i + j];
                var row = dataTable.NewRow();
                row["TpepPickupDatetime"] = trip.PickupDateTime;
                row["TpepDropoffDatetime"] = trip.DropoffDateTime;
                row["PassengerCount"] = trip.PassengerCount;
                row["TripDistance"] = trip.TripDistance;
                row["StoreAndFwdFlag"] = (object)trip.StoreAndFwdFlag ?? DBNull.Value;
                row["PULocationID"] = trip.PULocationID;
                row["DOLocationID"] = trip.DOLocationID;
                row["FareAmount"] = trip.FareAmount;
                row["TipAmount"] = trip.TipAmount;
                dataTable.Rows.Add(row);

            }

            using var bulkCopy = new SqlBulkCopy(connection)
            {
                DestinationTableName = "dbo.TaxiTrips",
                BatchSize = batchSize,
                BulkCopyTimeout = 0,
                NotifyAfter = currentBatchSize,
            };

            bulkCopy.ColumnMappings.Add("TpepPickupDatetime", "TpepPickupDatetime");
            bulkCopy.ColumnMappings.Add("TpepDropoffDatetime", "TpepDropoffDatetime");
            bulkCopy.ColumnMappings.Add("PassengerCount", "PassengerCount");
            bulkCopy.ColumnMappings.Add("TripDistance", "TripDistance");
            bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "StoreAndFwdFlag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("FareAmount", "FareAmount");
            bulkCopy.ColumnMappings.Add("TipAmount", "TipAmount");

            await bulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);

            totalRowsInserted += currentBatchSize;
        }

        return totalRowsInserted;
    }
}