using System.Text.Json;
using ETLVentasWorker.Models;
using Microsoft.Data.Sqlite;

namespace ETLVentasWorker.Warehouse;

public class DimensionLoader
{
    private readonly ILogger<DimensionLoader> _logger;
    private readonly WarehouseInitializer _warehouseInitializer;

    public DimensionLoader(ILogger<DimensionLoader> logger, WarehouseInitializer warehouseInitializer)
    {
        _logger = logger;
        _warehouseInitializer = warehouseInitializer;
    }

    public async Task LoadDimensionsAsync(CancellationToken cancellationToken)
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

        foreach (var file in files)
        {
            var sourceName = Path.GetFileName(file).Split('_')[0];
            var fechaCarga = DateTime.Now;

            var fuenteId = await InsertOrGetFuenteAsync(connection, sourceName, cancellationToken);
            var fechaId = await InsertFechaAsync(connection, fechaCarga, cancellationToken);

            var json = await File.ReadAllTextAsync(file, cancellationToken);

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            var records = JsonSerializer.Deserialize<List<StagingRecord>>(json, options) ?? new List<StagingRecord>();

            foreach (var record in records)
            {
                var valorTexto = record.Nombre ?? record.Comentario ?? record.Descripcion ?? "Sin valor";

                await InsertRegistroAsync(
                    connection,
                    record.Id,
                    valorTexto,
                    record.Tipo ?? sourceName,
                    fuenteId,
                    fechaId,
                    cancellationToken);
            }

            _logger.LogInformation("Archivo procesado y cargado a dimensiones: {File}", Path.GetFileName(file));
        }
    }

    private async Task<int> InsertOrGetFuenteAsync(SqliteConnection connection, string nombreFuente, CancellationToken cancellationToken)
    {
        await using var checkCommand = connection.CreateCommand();
        checkCommand.CommandText = "SELECT FuenteId FROM DimFuenteDatos WHERE NombreFuente = $nombreFuente;";
        checkCommand.Parameters.AddWithValue("$nombreFuente", nombreFuente);

        var result = await checkCommand.ExecuteScalarAsync(cancellationToken);

        if (result != null && result != DBNull.Value)
            return Convert.ToInt32(result);

        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO DimFuenteDatos (NombreFuente)
            VALUES ($nombreFuente);
            SELECT last_insert_rowid();";
        insertCommand.Parameters.AddWithValue("$nombreFuente", nombreFuente);

        var newId = await insertCommand.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(newId);
    }

    private async Task<int> InsertFechaAsync(SqliteConnection connection, DateTime fecha, CancellationToken cancellationToken)
    {
        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO DimFecha (FechaCompleta, Anio, Mes, Dia)
            VALUES ($fechaCompleta, $anio, $mes, $dia);
            SELECT last_insert_rowid();";
        insertCommand.Parameters.AddWithValue("$fechaCompleta", fecha.ToString("yyyy-MM-dd"));
        insertCommand.Parameters.AddWithValue("$anio", fecha.Year);
        insertCommand.Parameters.AddWithValue("$mes", fecha.Month);
        insertCommand.Parameters.AddWithValue("$dia", fecha.Day);

        var newId = await insertCommand.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(newId);
    }

    private async Task InsertRegistroAsync(
        SqliteConnection connection,
        int idOriginal,
        string valorTexto,
        string tipo,
        int fuenteId,
        int fechaId,
        CancellationToken cancellationToken)
    {
        await using var insertCommand = connection.CreateCommand();
        insertCommand.CommandText = @"
            INSERT INTO DimRegistro (IdOriginal, ValorTexto, Tipo, FuenteId, FechaId)
            VALUES ($idOriginal, $valorTexto, $tipo, $fuenteId, $fechaId);";

        insertCommand.Parameters.AddWithValue("$idOriginal", idOriginal);
        insertCommand.Parameters.AddWithValue("$valorTexto", valorTexto);
        insertCommand.Parameters.AddWithValue("$tipo", tipo);
        insertCommand.Parameters.AddWithValue("$fuenteId", fuenteId);
        insertCommand.Parameters.AddWithValue("$fechaId", fechaId);

        await insertCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}