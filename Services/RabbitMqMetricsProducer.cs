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
    public class RabbitMqMetricsProducer: IMetricsProducer, IDisposable
    {
        private IConnection _connection;
        private IChannel _channel;
        private const string ExchangeName = "telemetry_exchange";
        public RabbitMqMetricsProducer(IConfiguration configuration)
        {
            InitializeConnection(configuration);
        }
        private async void InitializeConnection(IConfiguration configuration)
        {
            var factory = new ConnectionFactory
            {
                HostName = configuration["RabbitMQ:Host"] ?? "localhost",
                Password = configuration.GetSection("RabbitMQ")["password"] ?? "",
                UserName= configuration.GetSection("RabbitMQ")["username"] ?? ""
                
            };

            _connection = await factory.CreateConnectionAsync();
            _channel = await _connection.CreateChannelAsync();

            await _channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Fanout, true);
            await _channel.QueueBindAsync("metrics_queue", ExchangeName, "server.metrics");
        }
        public async void Publish(MachineMetrics metrics)
        {

            if (_channel is null)
            {
                Console.WriteLine("Помилка: Канал RabbitMQ не ініціалізовано. Перевірте з'єднання.");
                return;
            }
            
            var json = JsonSerializer.Serialize(metrics);
            var body = Encoding.UTF8.GetBytes(json);

            var properties = new BasicProperties();
            properties.Persistent = false;

            await _channel.BasicPublishAsync(ExchangeName, "server.metrics", true, properties, body);
        }
        public async void Dispose()
        {
            await _channel.CloseAsync();
            await _connection.CloseAsync();
        }
    }
}