using ETLVentasWorker.Interfaces;
using ETLVentasWorker.Staging;
using ETLVentasWorker.Warehouse;

namespace ETLVentasWorker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly StagingWriter _stagingWriter;
    private readonly WarehouseInitializer _warehouseInitializer;
    private readonly DimensionLoader _dimensionLoader;

    public Worker(
        ILogger<Worker> logger,
        IEnumerable<IExtractor> extractors,
        StagingWriter stagingWriter,
        WarehouseInitializer warehouseInitializer,
        DimensionLoader dimensionLoader)
    {
        _logger = logger;
        _extractors = extractors;
        _stagingWriter = stagingWriter;
        _warehouseInitializer = warehouseInitializer;
        _dimensionLoader = dimensionLoader;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("ETL Worker iniciado en: {time}", DateTimeOffset.Now);

        foreach (var extractor in _extractors)
        {
            var data = await extractor.ExtractAsync(stoppingToken);
            await _stagingWriter.SaveAsync(extractor.SourceName, data, stoppingToken);
        }

        await _warehouseInitializer.InitializeAsync(stoppingToken);
        await _dimensionLoader.LoadDimensionsAsync(stoppingToken);

        _logger.LogInformation("Proceso ETL y carga de dimensiones finalizado.");
    }
}