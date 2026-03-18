namespace ETLVentasWorker.Interfaces;

public interface IExtractor
{
    string SourceName { get; }
    Task<IEnumerable<object>> ExtractAsync(CancellationToken cancellationToken);
}