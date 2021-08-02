using System.Threading.Tasks;
using Confluent.Kafka;

namespace MassTransitKafkaDemo.Infrastructure.AvroSerializers
{
    /// <summary>
    /// Wraps generic AvroSerializer
    /// </summary>
    public interface ISerializerWrapper
    {
        Task<byte[]> SerializeAsync(object data, SerializationContext context);
    }
}