using ETLVentasWorker.Interfaces;

namespace ETLVentasWorker.Extractors;

public class ApiExtractor : IExtractor
{
    private readonly ILogger<ApiExtractor> _logger;

    public string SourceName => "API";

    public ApiExtractor(ILogger<ApiExtractor> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<object>> ExtractAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando extracción desde API...");

        var data = new List<object>
        {
            new { Id = 101, Comentario = "Dato desde API", Tipo = "API" },
            new { Id = 102, Comentario = "Otro dato API", Tipo = "API" }
        };

        _logger.LogInformation("Extracción API completada. Registros: {Count}", data.Count);

        return await Task.FromResult(data);
    }
}