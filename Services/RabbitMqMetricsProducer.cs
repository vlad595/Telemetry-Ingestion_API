using System;
using System.Text.Json;
using Models;
using RabbitMQ.Client;
using Microsoft.Extensions.Configuration;
using System.Text;

namespace Services
{
    public interface IMetricsProducer
    {
        Task Publish(MachineMetrics metric, int subscriptionTier);
    }
    public class RabbitMqMetricsProducer: IMetricsProducer, IAsyncDisposable
    {
        private IConnection? _connection;
        private IChannel? _channel;
        private readonly IConfiguration _config;
        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _publishLock = new SemaphoreSlim(1, 1);
        public RabbitMqMetricsProducer(IConfiguration configuration)
        {
            _config = configuration;
        }
        private async Task EnsureConnection()
        {
            if (_channel is not null) return;
            await _connectionLock.WaitAsync();
            try {
                if (_channel is not null) return;

                var factory = new ConnectionFactory
                {
                    HostName = _config["RabbitMQ:Host"] ?? "localhost",
                    Password = _config.GetSection("RabbitMQ")["password"] ?? "",
                    UserName= _config.GetSection("RabbitMQ")["username"] ?? ""
                };

                _connection = await factory.CreateConnectionAsync();
                _channel = await _connection.CreateChannelAsync();
                await _channel.QueueDeclareAsync("metrics_queue", true, false, false, null);
                await _channel.QueueDeclareAsync("analytics-metrics_queue", true, false, false, null);
                await _channel.QueueDeclareAsync("telegram_notification", true, false, false, null);

            }
            finally
            {
                _connectionLock.Release();
            }
        }
        public async Task Publish(MachineMetrics metrics, int subscriptionTier)
        {
            await EnsureConnection();

            var json = JsonSerializer.Serialize(metrics);
            var body = Encoding.UTF8.GetBytes(json);

            var properties = new BasicProperties();
            properties.Persistent = false;

            await _publishLock.WaitAsync();
            try
            {
                await _channel!.BasicPublishAsync("", "metrics_queue", true, properties, body);

                if (subscriptionTier is 1 or 2)
                {
                    await _channel.BasicPublishAsync("", "analytics-metrics_queue", true, properties, body);
                    await _channel.BasicPublishAsync("", "telegram_notification", true, properties, body);
                }
            }
            finally
            {
                _publishLock.Release();
            }
        }
        public async ValueTask DisposeAsync()
        {
            if (_channel is not null)
            {
                await _channel.CloseAsync();
            }
            if (_connection is not null)
            {
                await _connection.CloseAsync();
            }
            _connectionLock.Dispose();
            _publishLock.Dispose();
        }
    }
}
