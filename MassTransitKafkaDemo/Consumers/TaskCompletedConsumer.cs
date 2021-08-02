using System.Threading.Tasks;
using MassTransit;
using MassTransitKafkaDemo.Messages;
using Microsoft.Extensions.Logging;

namespace MassTransitKafkaDemo.Consumers
{
    public class TaskCompletedConsumer : IConsumer<TaskCompleted>
    {
        private readonly ILogger<TaskCompletedConsumer> _logger;

        public TaskCompletedConsumer(ILogger<TaskCompletedConsumer> logger)
        {
            _logger = logger;
        }

        public async Task Consume(ConsumeContext<TaskCompleted> context)
        {
            var message = context.Message;
            _logger.LogInformation($"Task {message.Id} completed at {message.CompletedDate}");
            await Task.CompletedTask;
        }
    }
}