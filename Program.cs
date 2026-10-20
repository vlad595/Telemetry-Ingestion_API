using Models;
using Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();

builder.Services.AddSingleton<IMetricsProducer, RabbitMqMetricsProducer>();

builder.Services.AddCors(options =>
{
    options.AddPolicy(name: "AllowAll", policy => {policy.AllowAnyOrigin();policy.AllowAnyHeader();});
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseCors("AllowAll");
    app.MapOpenApi();
}

app.MapPost("/api/telemetry/ingest", (MachineMetrics metrics, IMetricsProducer producer) =>
{
    producer.Publish(metrics);
    return Results.Accepted(); 
});

app.Run();
