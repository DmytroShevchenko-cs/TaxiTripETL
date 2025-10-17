namespace TaxiTripETL.Infrastructure.DB;

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Models;

public static class DbInitializer
{
    public static async Task InitializeAsync(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var databaseName = builder.InitialCatalog;

        // To check/create the database, we must connect to a database that is guaranteed to exist.
        var masterConnectionString = new SqlConnectionStringBuilder(connectionString)
        {
            InitialCatalog = "master"
        }.ConnectionString;

        await using (var masterConnection = new SqlConnection(masterConnectionString))
        {
            await masterConnection.OpenAsync();

            // Check if the database already exists
            await using var checkCmd = new SqlCommand(
                "SELECT name FROM sys.databases WHERE name = @dbName",
                masterConnection);

            checkCmd.Parameters.AddWithValue("@dbName", databaseName);

            var result = await checkCmd.ExecuteScalarAsync();

            // If the database doesn't exist, create it
            if (result == null)
            {
                Console.WriteLine($"Database '{databaseName}' not found. Creating...");
                var createDbQuery = $"CREATE DATABASE [{databaseName}]";
                await using var createCmd = new SqlCommand(createDbQuery, masterConnection);
                await createCmd.ExecuteNonQueryAsync();
                Console.WriteLine($"Database '{databaseName}' created successfully.");
            }
        }

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        var createTableScript = await File.ReadAllTextAsync($"{StaticFilePaths.QueryPath}/init.sql");

        var scriptBatches = createTableScript.Split(new[] { "GO", "go" }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var batch in scriptBatches)
        {
            if (string.IsNullOrWhiteSpace(batch)) continue;
            await using var command = new SqlCommand(batch, connection);
            await command.ExecuteNonQueryAsync();
        }

        Console.WriteLine("Database schema is up to date.");
    }
}
