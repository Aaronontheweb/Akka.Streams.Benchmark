using System;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.IO;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Streams.Benchmark
{
    class Program
    {
        public static Flow<ByteString, ByteString, NotUsed> Encoder =>
            Framing.SimpleFramingProtocolEncoder(256 * 1024 * 1024);

        public static Flow<ByteString, ByteString, NotUsed> Decoder =>
            Framing.SimpleFramingProtocolDecoder(256 * 1024 * 1024);
        
        static async Task Main(string[] args)
        {
            var actorSystem = ActorSystem.Create("AkkaStreams", @"akka.loglevel = DEBUG");
            var materializer = actorSystem.Materializer();
            
            // create server
            Source<Dsl.Tcp.IncomingConnection, Task<Dsl.Tcp.ServerBinding>> connections = actorSystem.TcpStream().Bind("127.0.0.1", 8888);

            var (serverBind, source) = connections.PreMaterialize(materializer);
            
            // server event handler - per connection
            source.RunForeach(conn =>
            {
                var echo = Flow.Create<ByteString>()
                    .Via(Decoder)
                    .Select(c => c.ToString())
                    .Select(c =>
                    {
                        return c.ToUpperInvariant();
                    })
                    .Select(ByteString.FromString)
                    .Via(Encoder)
                    .GroupedWithin(100, TimeSpan.FromMilliseconds(20))
                    .Select(x => x.Aggregate(ByteString.Empty, (s, byteString) => s.Concat(byteString)));

                    conn.HandleWith(echo, materializer);
            }, materializer);

            await serverBind;
            
            // start client to connect to server
            var clientTask = actorSystem.TcpStream().OutgoingConnection(serverBind.Result.LocalAddress);

            // generate repeating loop of data
            var repeater = ByteString.FromString("A");
            var dataGenerator = Source.Repeat(repeater)
                .Via(Encoder)
                .Batch(100, s => s, (s, byteString) => s.Concat(byteString));

            // compute rate at which data is sent client --> server --> client per second
            var bytesPerSecondFlow = Flow.Create<ByteString>()
                .Via(Decoder)
                .GroupedWithin(1000, TimeSpan.FromMilliseconds(1000))
                .Select(bytes => bytes.Sum(x => x.Count))
                .Aggregate((0L, DateTime.UtcNow), (l, s) =>
                {
                    var (accum, time) = l;
                    accum += s;

                    var timeSpan = (DateTime.UtcNow - time);
                    if (timeSpan.TotalSeconds >= 1.0d)
                    {
                        Console.WriteLine($"{accum} byte/{timeSpan.TotalSeconds}");
                        return (0L, DateTime.UtcNow); // reset
                    }

                    return (accum, time);
                })
                .To(Sink.Ignore<(long, DateTime)>());

            // run BiDi flow
            dataGenerator.Via(clientTask).To(bytesPerSecondFlow).Run(materializer);
            
            await actorSystem.WhenTerminated;
        }
    }
}
