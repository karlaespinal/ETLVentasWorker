using ETLVentasWorker.Interfaces;
using ETLVentasWorker.Staging;

namespace ETLVentasWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly StagingWriter _stagingWriter;

    public Worker(
        ILogger<Worker> logger,
        IEnumerable<IExtractor> extractors,
        StagingWriter stagingWriter)
    {
        _logger = logger;
        _extractors = extractors;
        _stagingWriter = stagingWriter;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ETL Worker iniciado en: {time}", DateTimeOffset.Now);

        foreach (var extractor in _extractors)
        {
            var data = await extractor.ExtractAsync(stoppingToken);
            await _stagingWriter.SaveAsync(extractor.SourceName, data, stoppingToken);
        }

        _logger.LogInformation("Proceso de extracción ETL finalizado.");
    }
}