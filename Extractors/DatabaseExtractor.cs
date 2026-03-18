using ETLVentasWorker.Interfaces;

namespace ETLVentasWorker.Extractors;

public class DatabaseExtractor : IExtractor
{
    private readonly ILogger<DatabaseExtractor> _logger;

    public string SourceName => "Database";

    public DatabaseExtractor(ILogger<DatabaseExtractor> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<object>> ExtractAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando extracción desde Base de Datos...");

        var data = new List<object>
        {
            new { Id = 201, Descripcion = "Dato BD 1", Tipo = "Database" },
            new { Id = 202, Descripcion = "Dato BD 2", Tipo = "Database" }
        };

        _logger.LogInformation("Extracción de Base de Datos completada. Registros: {Count}", data.Count);

        return await Task.FromResult(data);
    }
}