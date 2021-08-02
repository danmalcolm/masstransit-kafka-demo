using Avro.IO;

namespace MassTransitKafkaDemo.Infrastructure.AvroSerializers
{
    /// <summary>
    /// Wraps generic Avro SpecificReader
    /// </summary>
    public interface IReaderWrapper
    {
        object Read(BinaryDecoder decoder);
    }
}
