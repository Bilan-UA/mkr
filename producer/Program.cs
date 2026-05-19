using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string BOOTSTRAP_SERVERS = "kafka:29092";
const string REQUESTS_TOPIC    = "demo-requests";
const string RESPONSES_TOPIC   = "demo-responses";
const int    START             = 10;
const int    FINISH            = 100;
const int    TIMEOUT_MS        = 30_000;

using (var admin = new AdminClientBuilder(
        new AdminClientConfig { BootstrapServers = BOOTSTRAP_SERVERS }).Build())
{
    var meta = admin.GetMetadata(TimeSpan.FromSeconds(10));
    var existing = meta.Topics.Select(t => t.Topic).ToHashSet();

    var toCreate = new[] { REQUESTS_TOPIC, RESPONSES_TOPIC }
        .Where(t => !existing.Contains(t))
        .Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 })
        .ToList();

    if (toCreate.Any())
        await admin.CreateTopicsAsync(toCreate);
}

var correlationId = Guid.NewGuid().ToString();

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = BOOTSTRAP_SERVERS,
    GroupId          = $"producer-reply-{correlationId}",  
    AutoOffsetReset  = AutoOffsetReset.Latest,
    EnableAutoCommit = false,
}).Build();

consumer.Subscribe(RESPONSES_TOPIC);
consumer.Consume(TimeSpan.FromMilliseconds(500));

using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = BOOTSTRAP_SERVERS,
}).Build();

var message = new Message<string, string>
{
    Key   = correlationId,
    Value = $"{START},{FINISH}",
    Headers = new Headers { { "correlation-id", System.Text.Encoding.UTF8.GetBytes(correlationId) } },
};

await producer.ProduceAsync(REQUESTS_TOPIC, message);
Console.WriteLine($"-> Запит надіслано: start={START} finish={FINISH} (id={correlationId})");

var deadline = DateTime.UtcNow.AddMilliseconds(TIMEOUT_MS);

while (DateTime.UtcNow < deadline)
{
    var result = consumer.Consume(TimeSpan.FromSeconds(1));
    if (result is null) continue;

    var replyCorrelation = result.Message.Headers
        .TryGetLastBytes("correlation-id", out var bytes)
        ? System.Text.Encoding.UTF8.GetString(bytes)
        : null;

    if (replyCorrelation == correlationId)
    {
        Console.WriteLine($"<- Отримано відповідь: avgSteps={result.Message.Value}");
        break;
    }
}

Console.WriteLine("Готово. Контейнер живе.");
consumer.Close();

await Task.Delay(Timeout.Infinite);
