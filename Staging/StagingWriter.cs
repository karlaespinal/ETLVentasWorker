using System.Text.Json;

namespace ETLVentasWorker.Staging;

public class StagingWriter
{
    private readonly ILogger<StagingWriter> _logger;

    public StagingWriter(ILogger<StagingWriter> logger)
    {
        _logger = logger;
    }

    public async Task SaveAsync(string sourceName, IEnumerable<object> data, CancellationToken cancellationToken)
    {
        var folderPath = Path.Combine(Directory.GetCurrentDirectory(), "StagingFiles");
        Directory.CreateDirectory(folderPath);

        var filePath = Path.Combine(folderPath, $"{sourceName}_{DateTime.Now:yyyyMMdd_HHmmss}.json");

        var json = JsonSerializer.Serialize(data, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        await File.WriteAllTextAsync(filePath, json, cancellationToken);

        _logger.LogInformation("Datos guardados en staging: {FilePath}", filePath);
    }
}