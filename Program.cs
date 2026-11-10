using Models;
using Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddOpenApi();
builder.Services.AddMemoryCache();

builder.Services.AddSingleton<IMetricsProducer, RabbitMqMetricsProducer>();
builder.Services.AddSingleton<IUserSubscriptionHandler, UserSubscriptionHandler>();

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

app.MapPost("/api/telemetry/ingest", async (MachineMetrics metrics, IMetricsProducer producer, IUserSubscriptionHandler subscriptionHandler) =>
{
    int subscriptionTier = await subscriptionHandler.GetSubscriptionTier(metrics.instanceId.ToString());
    await producer.Publish(metrics, subscriptionTier);
    return Results.Accepted(); 
});

app.Run();
