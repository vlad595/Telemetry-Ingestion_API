using System.Globalization;
using Microsoft.Extensions.Caching.Memory;
using Models;

namespace Services;

public sealed class NotificationThresholds
{
    public float CpuLoadFraction { get; set; } = 0.9f;
    public float RamLoadFraction { get; set; } = 0.9f;
    public float MinFreeDiskMb { get; set; } = 1024;
    public int CooldownMinutes { get; set; } = 30;
}

public sealed class NotificationAlertService(IMemoryCache cache, IConfiguration configuration)
{
    [Flags]
    private enum AlertKind { None = 0, Cpu = 1, Ram = 2, Disk = 4 }

    private sealed class AgentAlertState
    {
        public SemaphoreSlim Gate { get; } = new(1, 1);
        public AlertKind Active { get; set; }
        public AlertKind Pending { get; set; }
        public DateTimeOffset LastSentAt { get; set; } = DateTimeOffset.MinValue;
    }

    private readonly object _cacheLock = new();
    private readonly NotificationThresholds _thresholds = configuration.GetSection("Notifications").Get<NotificationThresholds>()
        ?? new NotificationThresholds();

    public async Task PublishIfNeeded(MachineMetrics metric, IMetricsProducer producer)
    {
        AgentAlertState state;
        lock (_cacheLock)
        {
            state = cache.GetOrCreate($"notification:{metric.instanceId}", entry =>
            {
                entry.SlidingExpiration = TimeSpan.FromDays(1);
                return new AgentAlertState();
            })!;
        }

        await state.Gate.WaitAsync();
        try
        {
            AlertKind current = GetCurrentAlerts(metric);
            state.Pending = (state.Pending | (current & ~state.Active)) & current;
            state.Active = current;

            if (state.Pending == AlertKind.None ||
                DateTimeOffset.UtcNow - state.LastSentAt < TimeSpan.FromMinutes(_thresholds.CooldownMinutes))
            {
                return;
            }

            AlertKind toSend = state.Pending;
            string message = BuildMessage(metric, toSend);
            await producer.PublishNotification(metric.instanceId, message);
            state.Pending &= ~toSend;
            state.LastSentAt = DateTimeOffset.UtcNow;
        }
        finally
        {
            state.Gate.Release();
        }
    }

    private AlertKind GetCurrentAlerts(MachineMetrics metric)
    {
        AlertKind result = AlertKind.None;
        if (metric.CpuLoad >= _thresholds.CpuLoadFraction) result |= AlertKind.Cpu;
        if (metric.RamLoad >= _thresholds.RamLoadFraction) result |= AlertKind.Ram;
        if (metric.FreeDiskSpace > 0 && metric.FreeDiskSpace <= _thresholds.MinFreeDiskMb) result |= AlertKind.Disk;
        return result;
    }

    private static string BuildMessage(MachineMetrics metric, AlertKind alerts)
    {
        var details = new List<string>();
        if (alerts.HasFlag(AlertKind.Cpu)) details.Add($"CPU: {(metric.CpuLoad * 100).ToString("0.#", CultureInfo.InvariantCulture)}%");
        if (alerts.HasFlag(AlertKind.Ram)) details.Add($"RAM: {(metric.RamLoad * 100).ToString("0.#", CultureInfo.InvariantCulture)}%");
        if (alerts.HasFlag(AlertKind.Disk)) details.Add($"Вільне місце: {metric.FreeDiskSpace.ToString("0.#", CultureInfo.InvariantCulture)} МБ");
        return $"Критичний стан агента {metric.instanceId}: {string.Join(", ", details)}.";
    }
}
