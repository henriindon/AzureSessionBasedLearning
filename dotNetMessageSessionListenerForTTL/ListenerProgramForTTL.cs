using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Tracing;

class ListenerProgramForTTL
{
    static async Task Main(string[] args)
    {
        string appName = (args.Length == 0) ? "TTL Testing Listener" : args[0];

        var builder = new ConfigurationBuilder()
            .AddUserSecrets<ListenerProgramForTTL>()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

        var configuration = builder.Build();

        string connectionString = configuration["ServiceBus:ConnectionString"];
        string topicName = configuration["ServiceBus:TopicName"];
        string subscriptionName = configuration["ServiceBus:SubscriptionName"];

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

        ILogger<ListenerProgramForTTL> logger = serviceProvider.GetRequiredService<ILogger<ListenerProgramForTTL>>();

        // https://learn.microsoft.com/en-us/dotnet/azure/sdk/logging#configure-custom-logging
        using AzureEventSourceListener listener = new AzureEventSourceListener((e, message) =>   Console.WriteLine($"{DateTime.Now} {message}"), level: EventLevel.Verbose);

        using (logger.BeginScope("Application: {AppName} Listener", appName))
        {
            await ProcessMessagesAsync(connectionString, topicName, subscriptionName, logger);
        }
    }

    static async Task ProcessMessagesAsync(string connectionString, string topicName, string subscriptionName, ILogger logger)
    {
        var clientOptions = new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions
            {
                Mode = ServiceBusRetryMode.Fixed,                
                MaxRetries = 2,                
                Delay = TimeSpan.FromSeconds(5),
                MaxDelay = TimeSpan.FromSeconds(10),
                TryTimeout = TimeSpan.FromSeconds(120)
            },
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using (ServiceBusClient client = new ServiceBusClient(connectionString, clientOptions))
        {
            var processorOptions = new ServiceBusSessionProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentSessions = 1,                
                MaxConcurrentCallsPerSession = 1
            };

            ServiceBusSessionProcessor processor = client.CreateSessionProcessor(topicName, subscriptionName, processorOptions);

            processor.ProcessMessageAsync += async args =>
            {
                string message = args.Message.Body.ToString();
                var fruitName = args.Message.SessionId;

                logger.LogInformation($"Processing: {message} -- {args.Message.ExpiresAt.ToLocalTime()}");

                Random randomDelay = new Random();

                await Task.Delay(TimeSpan.FromSeconds(3));

                logger.LogInformation($"Abandon processing: {message}");

                await args.AbandonMessageAsync(args.Message);
            };

            processor.ProcessErrorAsync += args =>
            {
                logger.LogError(args.Exception, "Message handler encountered an exception");
                return Task.CompletedTask;
            };

            await processor.StartProcessingAsync();

            logger.LogInformation($"Start service bus listener for topic {topicName} / subs {subscriptionName}");

            // Wait for the user to press a key to stop the processor
            Console.WriteLine("Press any key to stop the processor...");
            Console.ReadKey();

            await processor.StopProcessingAsync();
        }
    }
}
