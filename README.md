### TaxiTripETL

Console ETL app (.NET 8) that loads NYC Taxi trips from CSV into MS SQL Server, provides simple analysis queries, and logs via `ILogger`.

### Features
- Reads CSV, validates headers, optionally converts timestamps to UTC
- Filters duplicates within a single CSV (duplicates are written to `duplicates.csv`)
- Bulk inserts into SQL Server in batches
- Runs predefined analysis queries (top by distance/time, highest average tip by pickup location)
- Structured logging via `ILogger` (info to console, warnings/errors as structured logs)

### Requirements
- .NET SDK 8.0+
- SQL Server

### Configuration
Main config: `TaxiTripETL/appsettings.json`

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost,1433;Database=TaxiTrips;User Id=sa;Password=VerySecurePass123!;TrustServerCertificate=True;"
  },
  "BatchSize": 20000,
  "ConvertToUtc": true
}
```

You can override settings via environment variables (recommended for secrets):
- `ConnectionStrings__DefaultConnection`
- `BatchSize`
- `ConvertToUtc`

Example (PowerShell):
```powershell
$env:ConnectionStrings__DefaultConnection = "Server=localhost,1433;Database=TaxiTrips;User Id=sa;Password=YourPass!;TrustServerCertificate=True;"
$env:BatchSize = 10000
$env:ConvertToUtc = true
```

### CSV format (required headers)
The file must include at least the following columns (case-insensitive; whitespace trimmed):

```
tpep_pickup_datetime,
tpep_dropoff_datetime,
passenger_count,
trip_distance,
store_and_fwd_flag,
pulocationid,
dolocationid,
fare_amount,
tip_amount
```

### Build and run locally
1) Restore and run:
```powershell
dotnet build TaxiTripETL/TaxiTripETL.csproj
dotnet run --project TaxiTripETL/TaxiTripETL.csproj
```

2) On startup, the app initializes the DB schema if the connection is valid (`DbInitializer`).

3) In the interactive menu:
- Option 1 — enter the CSV path (e.g., `C:/data/trips.csv` on Windows)
- Option 2 — run ETL
- Option 3 — run analysis
- Option 4 — exit

Tips:
- If the file is not found, verify the path is valid for your OS and you have read permissions.
- Watch the console logs for warnings and errors.

### Troubleshooting
- "File not found" when entering the path:
  - Verify the path is correct for your OS and the file exists with read permissions.
- DB connectivity issues:
  - Check the connection string, port, `sa` password, and `TrustServerCertificate=True`.


