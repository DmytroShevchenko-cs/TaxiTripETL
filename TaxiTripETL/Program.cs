using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TaxiTripETL.Infrastructure.DB;
using TaxiTripETL.Repositories.Analysis;
using TaxiTripETL.Repositories.TaxiTripBulkRepository;
using TaxiTripETL.Services;
using TaxiTripETL.Services.DuplicateWriter;
using TaxiTripETL.Services.TaxiTripReader;
using Microsoft.Extensions.Logging;

namespace TaxiTripETL;

public sealed class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((hostingContext, config) =>
                {
                    config.SetBasePath(Directory.GetCurrentDirectory());
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                    config.AddJsonFile($"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true);
                    config.AddEnvironmentVariables();
                    config.AddCommandLine(args);
                })
                .ConfigureServices((hostContext, services) =>
                {
                    var connectionString = hostContext.Configuration.GetConnectionString("DefaultConnection");
                    if (string.IsNullOrEmpty(connectionString))
                    {
                        throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
                    }

                    services.AddSingleton(new ConnectionStringHolder(connectionString));

                    // ETL Services
                    services.AddSingleton<ITaxiTripReader, TaxiTripReader>();
                    services.AddSingleton<ITaxiTripBulkRepository>(sp => new SqlBulkTaxiTripRepository(sp.GetRequiredService<ConnectionStringHolder>().ConnectionString));
                    services.AddScoped<ITaxiTripWriter>(sp => new DublicateTaxiTripWriter("duplicates.csv"));
                    services.AddScoped<EtlService>();

                    // Analysis Services
                    services.AddScoped<IAnalysisRepository, AnalysisRepository>();

                    // Main application service
                    services.AddScoped<InteractiveService>();
                })
                .Build();

            // Initialize database schema on startup
            var connectionStringHolder = host.Services.GetRequiredService<ConnectionStringHolder>();
            await DbInitializer.InitializeAsync(connectionStringHolder.ConnectionString);

            // Run the interactive menu
            var interactiveService = host.Services.GetRequiredService<InteractiveService>();
            await interactiveService.RunAsync();

            return 0;
        }
        catch (Exception ex)
        {
            using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var logger = loggerFactory.CreateLogger<Program>();
            logger.LogCritical(ex, "A critical error occurred during application startup.");
            return 1;
        }
    }
}

public sealed class ConnectionStringHolder
{
    public string ConnectionString { get; }
    public ConnectionStringHolder(string connectionString) => ConnectionString = connectionString;
}