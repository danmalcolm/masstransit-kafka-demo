using System.Threading.Tasks;
using MassTransit;
using MassTransitKafkaDemo.Messages;
using Microsoft.Extensions.Logging;

namespace MassTransitKafkaDemo.Consumers
{
    /// <summary>
    /// Example of 
    /// </summary>
    public class TaskEventConsumer : IConsumer<ITaskEvent>
    {
        private readonly ILogger<TaskEventConsumer> _logger;

        public TaskEventConsumer(ILogger<TaskEventConsumer> logger)
        {
            _logger = logger;
        }
        
        public async Task Consume(ConsumeContext<ITaskEvent> context)
        {
            void LogEvent(string description) =>
                _logger.LogInformation("{0:O} Received ({1} {2}) event: {3}", "", context.Message.Id, context.Message.GetType(), description);

            var message = context.Message;
            switch (message)
            {
                case TaskRequested requested:
                    LogEvent($"Task requested by {requested.RequestedBy} at {requested.RequestedDate}");
                    break;
                case TaskStarted started:
                    LogEvent($"Task started on {started.StartedOn} at {started.StartedDate}");
                    break;
                case TaskCompleted completed:
                    LogEvent($"Task completed at {completed.CompletedDate}");
                    break;
            }
            await Task.CompletedTask;
        }
    }
}
