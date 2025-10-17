using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using TaxiTripETL.Services.TaxiTripReader;

namespace TaxiTripETL.Services;

using Repositories.Analysis;

public class InteractiveService
{
    private readonly IConfiguration _config;
    private readonly EtlService _etlService;
    private readonly IAnalysisRepository _analysisRepository;
    private readonly ITaxiTripReader _tripReader;
    private readonly ILogger<InteractiveService> _logger;
    private string? _selectedCsvPath;

    public InteractiveService(IConfiguration config, EtlService etlService, IAnalysisRepository analysisRepository, ITaxiTripReader tripReader, ILogger<InteractiveService> logger)
    {
        _config = config;
        _etlService = etlService;
        _analysisRepository = analysisRepository;
        _tripReader = tripReader;
        _logger = logger;
    }

    /// <summary>
    /// Runs the main interactive loop, displaying a menu and handling user input.
    /// </summary>
    public async Task RunAsync()
    {
        Console.WriteLine("--- Taxi Trip ETL & Analysis ---");
        while (true)
        {
            Console.WriteLine("\nMain Menu:");
            Console.WriteLine($"1. Select CSV file (Current: {_selectedCsvPath ?? "None"})");
            Console.WriteLine("2. Run ETL Process");
            Console.WriteLine("3. Run Analysis Queries");
            Console.WriteLine("4. Search by PULocationID");
            Console.WriteLine("5. Exit");
            Console.Write("> ");

        var input = Console.ReadLine();
        switch (input)
            {
                case "1":
                    await SelectAndValidateCsvFileAsync();
                    break;
                case "2":
                    await RunEtlProcessAsync();
                    break;
                case "3":
                    await RunAnalysisQueriesAsync();
                    break;
                case "4":
                    await RunSearchByPULocationIdAsync();
                    break;
                case "5":
                    return;
                default:
                _logger.LogWarning("Invalid option, please try again. Input: {Input}", input);
                    break;
            }
        }
    }

    /// <summary>
    /// Prompts the user to enter the path to a CSV file and immediately validates its format.
    /// </summary>
    private async Task SelectAndValidateCsvFileAsync()
    {
        Console.WriteLine("\nEnter the full path to the CSV file:");
        Console.Write("> ");
        var path = Console.ReadLine()?.Trim('\"', ' ');

        if (string.IsNullOrEmpty(path))
        {
            _logger.LogWarning("No path entered.");
            return;
        }

        if (!File.Exists(path))
        {
            _logger.LogWarning("File not found at path: {Path}", path);
            return;
        }

        try
        {
            Console.WriteLine("Validating file header...");
            await _tripReader.ValidateFileAsync(path, CancellationToken.None);
            _selectedCsvPath = path;
            Console.WriteLine("File validation successful. CSV file selected.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "File validation failed.");
            // Do not set the path if validation fails
        }
    }

    /// <summary>
    /// Runs the ETL process using the selected CSV file. Retrieves batch size and UTC conversion settings from configuration.
    /// </summary>
    private async Task RunEtlProcessAsync()
    {
        if (string.IsNullOrEmpty(_selectedCsvPath))
        {
            _logger.LogWarning("ETL requested without a selected CSV file.");
            return;
        }

        try
        {
            Console.WriteLine("ETL process starting...");
            var batchSize = _config.GetValue<int>("BatchSize");
            var convertToUtc = _config.GetValue<bool>("ConvertToUtc");

            var inserted = await _etlService.RunAsync(_selectedCsvPath, batchSize, convertToUtc, CancellationToken.None);
            Console.WriteLine($"ETL process complete. Inserted rows (this run): {inserted}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred during the ETL process.");
        }
    }

    /// <summary>
    /// Executes all predefined analysis queries and prints their results to the console.
    /// </summary>
    private async Task RunAnalysisQueriesAsync()
    {
        try
        {
            await _analysisRepository.FindLocationWithHighestAverageTipAsync();
            await _analysisRepository.FindTop100LongestFaresByDistanceAsync();
            await _analysisRepository.FindTop100LongestFaresByTimeAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred during analysis.");
        }
    }

    private async Task RunSearchByPULocationIdAsync()
    {
        Console.WriteLine("\nEnter PULocationID:");
        Console.Write("> ");
        var puInput = Console.ReadLine();
        if (!int.TryParse(puInput, out var puLocationId))
        {
            _logger.LogWarning("Invalid PULocationID: {Input}", puInput);
            return;
        }

        Console.WriteLine("Enter how many rows to take (default 100):");
        Console.Write("> ");
        var takeInput = Console.ReadLine();
        int take = 100;
        if (!string.IsNullOrWhiteSpace(takeInput) && !int.TryParse(takeInput, out take))
        {
            _logger.LogWarning("Invalid number, defaulting to 100. Input: {Input}", takeInput);
            take = 100;
        }

        try
        {
            await _analysisRepository.SearchByPULocationIdAsync(puLocationId, take);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred during search.");
        }
    }
}
