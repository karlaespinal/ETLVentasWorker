using ETLVentasWorker;
using ETLVentasWorker.Extractors;
using ETLVentasWorker.Interfaces;
using ETLVentasWorker.Staging;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<IExtractor, CsvExtractor>();
builder.Services.AddSingleton<IExtractor, ApiExtractor>();
builder.Services.AddSingleton<IExtractor, DatabaseExtractor>();

builder.Services.AddSingleton<StagingWriter>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();