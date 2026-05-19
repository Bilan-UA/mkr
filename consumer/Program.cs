using Confluent.Kafka;
using Confluent.Kafka.Admin;

const string BOOTSTRAP_SERVERS = "kafka:29092";
const string REQUESTS_TOPIC    = "demo-requests";
const string RESPONSES_TOPIC   = "demo-responses";
const string GROUP_ID          = "demo-responder-group";

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

using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = BOOTSTRAP_SERVERS,
    GroupId          = GROUP_ID,
    AutoOffsetReset  = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
}).Build();

consumer.Subscribe(REQUESTS_TOPIC);

using var producer = new ProducerBuilder<string, string>(new ProducerConfig
{
    BootstrapServers = BOOTSTRAP_SERVERS,
}).Build();

Console.WriteLine($"Чекаю запитів у '{REQUESTS_TOPIC}'.");

while (true)
{
    var result = consumer.Consume(CancellationToken.None);

    var parts = result.Message.Value.Split(',');
    if (parts.Length != 2
        || !int.TryParse(parts[0].Trim(), out int start)
        || !int.TryParse(parts[1].Trim(), out int finish))
    {
        Console.WriteLine($"[WARN] Некоректний формат повідомлення: {result.Message.Value}");
        continue;
    }

    Console.WriteLine($"<- Отримано запит: start={start} finish={finish}");

    double avgSteps = ComputeAvgCollatzSteps(start, finish);

    var correlationId = result.Message.Headers
        .TryGetLastBytes("correlation-id", out var bytes)
        ? System.Text.Encoding.UTF8.GetString(bytes)
        : result.Message.Key ?? Guid.NewGuid().ToString();

    var reply = new Message<string, string>
    {
        Key   = correlationId,
        Value = avgSteps.ToString("F2"),
        Headers = new Headers { { "correlation-id", System.Text.Encoding.UTF8.GetBytes(correlationId) } },
    };

    await producer.ProduceAsync(RESPONSES_TOPIC, reply);
    Console.WriteLine($"-> Надіслано відповідь: avgSteps={avgSteps:F2}");
}

static double ComputeAvgCollatzSteps(int start, int finish)
{
    long totalSteps = 0;
    int  count      = 0;

    for (int n = start; n <= finish; n++)
    {
        totalSteps += CollatzSteps(n);
        count++;
    }

    return count == 0 ? 0 : (double)totalSteps / count;
}

static long CollatzSteps(long n)
{
    long steps = 0;
    while (n != 1)
    {
        n = (n % 2 == 0) ? n / 2 : 3 * n + 1;
        steps++;
    }
    return steps;
}
//dev