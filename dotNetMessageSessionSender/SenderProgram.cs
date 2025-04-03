using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Amqp.Framing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Tracing;

namespace dotnetMessageSessionSender
{
    internal class SenderProgram
    {
        static async Task Main(string[] args)
        {
            string fruitName = args.Length == 0 ? "Banana" : args[0];
            string sessionStateTesting = args.Length != 2 ? "false" : args[1];

            var builder = new ConfigurationBuilder()
                .AddUserSecrets<SenderProgram>()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            var configuration = builder.Build();

            string connectionString = configuration["ServiceBus:ConnectionString"];
            string topicName = configuration["ServiceBus:TopicName"];

            var services = new ServiceCollection();
            services.AddLogging(loggingBuilder => {
                loggingBuilder
                .AddSimpleConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss.fff ";
                });
            });

            IServiceProvider serviceProvider = services.BuildServiceProvider();

            ILogger<SenderProgram> logger = serviceProvider.GetRequiredService<ILogger<SenderProgram>>();

            // https://learn.microsoft.com/en-us/dotnet/azure/sdk/logging#configure-custom-logging
            using AzureEventSourceListener listener = new AzureEventSourceListener((e, message) => Console.WriteLine($"{DateTime.Now} {message}"), level: EventLevel.Verbose);

            using (logger.BeginScope("Application: {FruitName} Sender", fruitName))
            {
                if (sessionStateTesting == "true")
                {
                    for (int i = 0; i < 15; i++)
                    {
                        await SendMessageForSessionStateTestingAsync(connectionString, topicName, i.ToString(), logger);
                        await Task.Delay(TimeSpan.FromMinutes(1));
                    }
                }
                else
                {
                    await SendMessageAsync(connectionString, topicName, fruitName, logger, TimeSpan.FromMinutes(1));

                    //await Task.Delay(TimeSpan.FromSeconds(5));

                    //await SendMessageAsync(connectionString, topicName, fruitName, logger, TimeSpan.FromMinutes(5));
                }
            }

            Console.WriteLine("Sender complete. Press any key to continue .....");
            Console.ReadLine();
        }

        static async Task SendMessageAsync(string connectionString, string topicName, string sessionId, ILogger logger, TimeSpan ttl)
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusSender sender = client.CreateSender(topicName);

                for (int i = 0; i < 30; i++)
                {
                    string messageBody = $"[{DateTime.Now}] - Message {Guid.NewGuid()}";
                    ServiceBusMessage message = new ServiceBusMessage(messageBody)
                    {
                        SessionId = sessionId,
                        TimeToLive = ttl,
                    };

                    logger.LogInformation($"Sending message: {messageBody} with session ID: {sessionId}");
                    await sender.SendMessageAsync(message);

                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            logger.LogInformation("-----------------------------------------------------");
            logger.LogInformation("-----------------------------------------------------");
            logger.LogInformation($"Sender completed sending messages with session ID: {sessionId}");
        }

        static async Task SendMessageForSessionStateTestingAsync(string connectionString, string topicName, string sessionId, ILogger logger)
        {
            await using (ServiceBusClient client = new ServiceBusClient(connectionString))
            {
                ServiceBusSender sender = client.CreateSender(topicName);

                string messageBody = $"[{DateTime.Now}] - Message {Guid.NewGuid()}";
                ServiceBusMessage message = new ServiceBusMessage(messageBody)
                {
                    SessionId = sessionId
                };

                logger.LogInformation($"Sending message: {messageBody} with session ID: {sessionId}");
                await sender.SendMessageAsync(message);
            }

            logger.LogInformation("-----------------------------------------------------");
            logger.LogInformation("-----------------------------------------------------");
            logger.LogInformation($"Sender completed sending messages with session ID: {sessionId}");
        }
    }
}

