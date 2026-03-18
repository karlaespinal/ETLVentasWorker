using ETLVentasWorker.Interfaces;

namespace ETLVentasWorker.Extractors;

public class CsvExtractor : IExtractor
{
    private readonly ILogger<CsvExtractor> _logger;

    public string SourceName => "CSV";

    public CsvExtractor(ILogger<CsvExtractor> logger)
    {
        _logger = logger;
    }

    public async Task<IEnumerable<object>> ExtractAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Iniciando extracción desde CSV...");

        var data = new List<object>
        {
            new { Id = 1, Nombre = "Luis", Tipo = "CSV" },
            new { Id = 2, Nombre = "Ana", Tipo = "CSV" }
        };

        _logger.LogInformation("Extracción CSV completada. Registros: {Count}", data.Count);

        return await Task.FromResult(data);
    }
}