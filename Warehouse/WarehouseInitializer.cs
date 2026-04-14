using Microsoft.Data.Sqlite;

namespace ETLVentasWorker.Warehouse;

public class WarehouseInitializer
{
    private readonly ILogger<WarehouseInitializer> _logger;
    private readonly string _connectionString;

    public WarehouseInitializer(ILogger<WarehouseInitializer> logger)
    {
        _logger = logger;
        var dbPath = Path.Combine(Directory.GetCurrentDirectory(), "DataWarehouse.db");
        _connectionString = $"Data Source={dbPath}";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var createDimFuenteDatos = @"
        CREATE TABLE IF NOT EXISTS DimFuenteDatos (
            FuenteId INTEGER PRIMARY KEY AUTOINCREMENT,
            NombreFuente TEXT NOT NULL UNIQUE
        );";

        var createDimFecha = @"
        CREATE TABLE IF NOT EXISTS DimFecha (
            FechaId INTEGER PRIMARY KEY AUTOINCREMENT,
            FechaCompleta TEXT NOT NULL,
            Anio INTEGER NOT NULL,
            Mes INTEGER NOT NULL,
            Dia INTEGER NOT NULL
        );";

        var createDimRegistro = @"
        CREATE TABLE IF NOT EXISTS DimRegistro (
            RegistroId INTEGER PRIMARY KEY AUTOINCREMENT,
            IdOriginal INTEGER NOT NULL,
            ValorTexto TEXT,
            Tipo TEXT,
            FuenteId INTEGER NOT NULL,
            FechaId INTEGER NOT NULL,
            FOREIGN KEY (FuenteId) REFERENCES DimFuenteDatos(FuenteId),
            FOREIGN KEY (FechaId) REFERENCES DimFecha(FechaId)
        );";

        foreach (var sql in new[] { createDimFuenteDatos, createDimFecha, createDimRegistro })
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger.LogInformation("Base de datos DataWarehouse.db y tablas dimensión creadas correctamente.");
    }

    public string GetConnectionString()
    {
        return _connectionString;
    }
}