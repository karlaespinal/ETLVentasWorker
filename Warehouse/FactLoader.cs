using System.Text.Json;
using ETLVentasWorker.Models;
using Microsoft.Data.Sqlite;

namespace ETLVentasWorker.Warehouse;

public class FactLoader
{
    private readonly ILogger<FactLoader> _logger;
    private readonly WarehouseInitializer _warehouseInitializer;

    public FactLoader(ILogger<FactLoader> logger, WarehouseInitializer warehouseInitializer)
    {
        _logger = logger;
        _warehouseInitializer = warehouseInitializer;
    }

    public async Task LoadFactsAsync(CancellationToken cancellationToken)
    {
        var stagingPath = Path.Combine(Directory.GetCurrentDirectory(), "StagingFiles");

        if (!Directory.Exists(stagingPath))
        {
            _logger.LogWarning("La carpeta StagingFiles no existe.");
            return;
        }

        var files = Directory.GetFiles(stagingPath, "*.json");

        if (files.Length == 0)
        {
            _logger.LogWarning("No se encontraron archivos JSON en StagingFiles.");
            return;
        }

        var connectionString = _warehouseInitializer.GetConnectionString();

        await using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        
        await using (var deleteCommand = connection.CreateCommand())
        {
            deleteCommand.CommandText = "DELETE FROM FactVentas;";
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger.LogInformation("Tabla FactVentas limpiada correctamente antes de la carga.");

        foreach (var file in files)
        {
            var sourceName = Path.GetFileName(file).Split('_')[0];
            var fechaCarga = DateTime.Now;

            var fuenteId = await GetFuenteIdAsync(connection, sourceName, cancellationToken);
            var fechaId = await GetOrInsertFechaAsync(connection, fechaCarga, cancellationToken);

            var json = await File.ReadAllTextAsync(file, cancellationToken);

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            var records = JsonSerializer.Deserialize<List<StagingRecord>>(json, options) ?? new List<StagingRecord>();

            foreach (var record in records)
            {
                // Valores simulados para la fact table
                var cantidad = 1;
                var total = record.Id * 100.0;

                await InsertFactAsync(connection, fuenteId, fechaId, cantidad, total, cancellationToken);
            }

            _logger.LogInformation("Archivo procesado y cargado a facts: {File}", Path.GetFileName(file));
        }
    }

    private async Task<int> GetFuenteIdAsync(SqliteConnection connection, string nombreFuente, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT FuenteId FROM DimFuenteDatos WHERE NombreFuente = $nombreFuente;";
        command.Parameters.AddWithValue("$nombreFuente", nombreFuente);

        var result = await command.ExecuteScalarAsync(cancellationToken);

        if (result != null && result != DBNull.Value)
            return Convert.ToInt32(result);

        throw new Exception($"No se encontró FuenteId para la fuente: {nombreFuente}");
    }

    private async Task<int> GetOrInsertFechaAsync(SqliteConnection connection, DateTime fecha, CancellationToken cancellationToken)
    {
        var fechaTexto = fecha.ToString("yyyy-MM-dd");

        await using var checkCommand = connection.CreateCommand();
        checkCommand.CommandText = "SELECT FechaId FROM DimFecha WHERE FechaCompleta = $fechaCompleta;";
        checkCommand.Parameters.AddWithValue("$fechaCompleta", fechaTexto);

        var result = await checkCommand.ExecuteScalarAsync(cancellationToken);

        if (result != null && result != DBNull.Value)
            return Convert.ToInt32(result);

        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO DimFecha (FechaCompleta, Anio, Mes, Dia)
            VALUES ($fechaCompleta, $anio, $mes, $dia);
            SELECT last_insert_rowid();";

        insertCommand.Parameters.AddWithValue("$fechaCompleta", fechaTexto);
        insertCommand.Parameters.AddWithValue("$anio", fecha.Year);
        insertCommand.Parameters.AddWithValue("$mes", fecha.Month);
        insertCommand.Parameters.AddWithValue("$dia", fecha.Day);

        var newId = await insertCommand.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(newId);
    }

    private async Task InsertFactAsync(
        SqliteConnection connection,
        int fuenteId,
        int fechaId,
        int cantidad,
        double total,
        CancellationToken cancellationToken)
    {
        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO FactVentas (FuenteId, FechaId, Cantidad, Total)
            VALUES ($fuenteId, $fechaId, $cantidad, $total);";

        insertCommand.Parameters.AddWithValue("$fuenteId", fuenteId);
        insertCommand.Parameters.AddWithValue("$fechaId", fechaId);
        insertCommand.Parameters.AddWithValue("$cantidad", cantidad);
        insertCommand.Parameters.AddWithValue("$total", total);

        await insertCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}