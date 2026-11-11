using Models;
using Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();
builder.Services.AddMemoryCache();

builder.Services.AddSingleton<IMetricsProducer, RabbitMqMetricsProducer>();
builder.Services.AddSingleton<IUserSubscriptionHandler, UserSubscriptionHandler>();
builder.Services.AddSingleton<NotificationAlertService>();

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

app.MapPost("/api/telemetry/ingest", async (MachineMetrics metrics, IMetricsProducer producer, IUserSubscriptionHandler subscriptionHandler, NotificationAlertService alerts, ILogger<Program> logger) =>
{
    int subscriptionTier = await subscriptionHandler.GetSubscriptionTier(metrics.instanceId.ToString());
    await producer.Publish(metrics, subscriptionTier);
    try
    {
        await alerts.PublishIfNeeded(metrics, producer);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Could not publish Telegram alert for agent {AgentId}", metrics.instanceId);
    }
    return Results.Accepted(); 
});

app.Run();
