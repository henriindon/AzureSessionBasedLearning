using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace dotnetMessageSessionSender
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string fruitName = args.Length == 0 ? "Banana" : args[0];

            var builder = new ConfigurationBuilder()
                .AddUserSecrets<Program>();

            var configuration = builder.Build();

            string connectionString = configuration["CodeyGrove:ServiceBus:ConnectionString"];
            string topicName = configuration["CodeyGrove:ServiceBus:TopicName"];

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

            using (logger.BeginScope("Application: {FruitName} Sender", fruitName))
            {
                await SendMessageAsync(connectionString, topicName, fruitName, logger);
            }
        }

        static async Task SendMessageAsync(string connectionString, string topicName, string sessionId, ILogger logger)
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusSender sender = client.CreateSender(topicName);

                for (int i = 0; i < 10; i++)
                {
                    string messageBody = $"Message {i}";
                    ServiceBusMessage message = new ServiceBusMessage(messageBody)
                    {
                        SessionId = sessionId
                    };

                    logger.LogInformation($"Sending message: {messageBody} with session ID: {sessionId}");
                    await sender.SendMessageAsync(message);
                }

                logger.LogInformation("All messages sent.");
            }
        }
    }
}

