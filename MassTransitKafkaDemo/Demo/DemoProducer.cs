using System;
using System.Threading;
using System.Threading.Tasks;
using MassTransit.KafkaIntegration;
using MassTransitKafkaDemo.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MassTransitKafkaDemo.Demo
{
    public class DemoProducer : BackgroundService
    {
        private readonly ILogger<DemoProducer> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;

        public DemoProducer(ILogger<DemoProducer> logger, IServiceScopeFactory serviceScopeFactory)
        {
            _logger = logger;
            _serviceScopeFactory = serviceScopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                using var scope = _serviceScopeFactory.CreateScope();
                var producer = scope.ServiceProvider.GetService<ITopicProducer<string, ITaskEvent>>();
                await Produce(producer, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Stopping");
            }
        }

        private async Task Produce(ITopicProducer<string, ITaskEvent> producer, CancellationToken stoppingToken)
        {
            var random = new Random();
            async Task ProduceMessage(Guid key, ITaskEvent value) => 
                await producer.Produce(key.ToString(), value, stoppingToken);

            async Task Wait(int min, int max) => 
                await Task.Delay(random.Next(min, max), stoppingToken);

            while (true)
            {
                var id = Guid.NewGuid();
                await ProduceMessage(id, new TaskRequested()
                {
                    Id = id,
                    RequestedDate = DateTime.Now,
                    RequestedBy = "test"
                });
                await Wait(250, 500);
                var startedOn = $"DEV{random.Next(1,999).ToString().PadLeft(3, '0')}";
                await ProduceMessage(id, new TaskStarted()
                {
                    Id = id,
                    StartedDate = DateTime.Now,
                    StartedOn = startedOn
                });
                await Wait(1000, 2500);
                await ProduceMessage(id, new TaskCompleted()
                {
                    Id = id,
                    CompletedDate = DateTime.Now
                });
                await Wait(1000, 2000);
            }
        }
    }
}