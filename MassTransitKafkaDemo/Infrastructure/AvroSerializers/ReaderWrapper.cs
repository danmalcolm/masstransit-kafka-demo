using Avro.IO;
using Avro.Specific;

namespace MassTransitKafkaDemo.Infrastructure.AvroSerializers
{
    internal class ReaderWrapper<T> : IReaderWrapper
    {
        private readonly SpecificReader<T> _reader;

        public ReaderWrapper(Avro.Schema writerSchema, Avro.Schema readerSchema)
        {
            _reader = new SpecificReader<T>(writerSchema, readerSchema);
        }

        public object Read(BinaryDecoder decoder) => _reader.Read(default, decoder);
    }
}
