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
        void Publish(MachineMetrics metric);
    }
    public class RabbitMqMetricsProducer: IMetricsProducer, IAsyncDisposable
    {
        private IConnection _connection;
        private IChannel _channel;
        private const string ExchangeName = "telemetry_exchange";
        private readonly IConfiguration _config;
        private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
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

                await _channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Fanout, true);
                await _channel.QueueBindAsync("metrics_queue", ExchangeName, "server.metrics");
                await _channel.QueueBindAsync("analytics-metrics_queue", ExchangeName, "server.metrics");
            }
            finally
            {
                _connectionLock.Release();
            }
        }
        public async void Publish(MachineMetrics metrics)
        {
            await EnsureConnection();
            
            var json = JsonSerializer.Serialize(metrics);
            var body = Encoding.UTF8.GetBytes(json);

            var properties = new BasicProperties();
            properties.Persistent = false;

            await _channel.BasicPublishAsync(ExchangeName, "server.metrics", true, properties, body);
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
        }
    }
}