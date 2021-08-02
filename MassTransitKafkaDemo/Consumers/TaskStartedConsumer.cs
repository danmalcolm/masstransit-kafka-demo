using System.Threading.Tasks;
using MassTransit;
using MassTransitKafkaDemo.Messages;
using Microsoft.Extensions.Logging;

namespace MassTransitKafkaDemo.Consumers
{
    public class TaskStartedConsumer : IConsumer<TaskStarted>
    {
        private readonly ILogger<TaskStartedConsumer> _logger;

        public TaskStartedConsumer(ILogger<TaskStartedConsumer> logger)
        {
            _logger = logger;
        }

        public async Task Consume(ConsumeContext<TaskStarted> context)
        {
            var message = context.Message;
            _logger.LogInformation($"Task {message.Id} started on {message.StartedOn} at {message.StartedDate}");
            await Task.CompletedTask;
        }
    }
}