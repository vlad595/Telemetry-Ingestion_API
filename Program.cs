using Models;
using Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();
builder.Services.AddMemoryCache();

builder.Services.AddSingleton<IMetricsProducer, RabbitMqMetricsProducer>();
builder.Services.AddSingleton<IUserSubscriptionHandler, UserSubscriptionHandler>();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", policy => policy
        .AllowAnyOrigin()
        .AllowAnyHeader()
        .AllowAnyMethod());
});

var app = builder.Build();
app.UseCors("AllowAll");

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapPost("/api/telemetry/ingest", async (MachineMetrics metrics, IMetricsProducer producer, IUserSubscriptionHandler subscriptionHandler, ILogger<Program> logger) =>
{
    int subscriptionTier = await subscriptionHandler.GetSubscriptionTier(metrics.instanceId.ToString());
    await producer.Publish(metrics, subscriptionTier);
    return Results.Accepted(); 
});

app.Run();
