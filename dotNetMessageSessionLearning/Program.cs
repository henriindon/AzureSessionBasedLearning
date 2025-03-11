using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

class Program
{
    static async Task Main(string[] args)
    {
        string appName = (args.Length == 0) ? "Grovey One" : args[0];

        var builder = new ConfigurationBuilder()
            .AddUserSecrets<Program>();

        var configuration = builder.Build();

        string connectionString = configuration["CodeyGrove:ServiceBus:ConnectionString"];
        string topicName = configuration["CodeyGrove:ServiceBus:TopicName"];
        string subscriptionName = configuration["CodeyGrove:ServiceBus:SubscriptionName"];

        using var loggerFactory = LoggerFactory.Create(loggingBuilder =>
        {
            loggingBuilder.SetMinimumLevel(LogLevel.Debug);
            loggingBuilder.AddFilter("Azure.Messaging.ServiceBus", LogLevel.Debug);
            loggingBuilder.AddSimpleConsole(options =>
            {
                options.IncludeScopes = true;
                options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
            });
        });

        ILogger logger = loggerFactory.CreateLogger<Program>();

        using (logger.BeginScope("Application: {AppName} Listener", appName))
        {
            await ReceiveMessagesFromSessionAsync(connectionString, topicName, subscriptionName, logger);
        }
    }

    static async Task ReceiveMessagesFromSessionAsync(string connectionString, string topicName, string subscriptionName, ILogger logger)
    {
        var clientOptions = new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions
            {
                Mode = ServiceBusRetryMode.Exponential,
                MaxRetries = 5,
                Delay = TimeSpan.FromSeconds(1),
                MaxDelay = TimeSpan.FromSeconds(30)
            },
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using (ServiceBusClient client = new ServiceBusClient(connectionString, clientOptions))
        {
            logger.LogInformation($"Start service bus listener for topic {topicName} / subs {subscriptionName}");

            ServiceBusSessionReceiver receiver = await client.AcceptNextSessionAsync(topicName, subscriptionName);

            while (true)
            {
                ServiceBusReceivedMessage message = await receiver.ReceiveMessageAsync();

                if (message != null)
                {
                    string messageBody = message.Body.ToString();
                    logger.LogInformation($"Received message for fruit called \"{message.SessionId}\": {messageBody}");
                    await Task.Delay(100000);
                    await receiver.CompleteMessageAsync(message);
                }
                else
                {
                    break;
                }
            }
        }
    }
}
