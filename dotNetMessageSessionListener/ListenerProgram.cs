using Azure.Core.Diagnostics;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Tracing;

class ListenerProgram
{
    static async Task Main(string[] args)
    {
        string appName = (args.Length == 0) ? "Grovey One" : args[0];
        string sessionStateTesting = args.Length != 2 ? "true" : args[1];

        var builder = new ConfigurationBuilder()
            .AddUserSecrets<ListenerProgram>()
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

        ILogger<ListenerProgram> logger = serviceProvider.GetRequiredService<ILogger<ListenerProgram>>();

        // https://learn.microsoft.com/en-us/dotnet/azure/sdk/logging#configure-custom-logging
        using AzureEventSourceListener listener = new AzureEventSourceListener((e, message) => Console.WriteLine($"{DateTime.Now} {message}"), level: EventLevel.Verbose);

        using (logger.BeginScope("Application: {AppName} Listener", appName))
        {
            if (sessionStateTesting == "true")
            {
                await ProcessMessagesWithSessionStateAsync(connectionString, topicName, subscriptionName, logger);
            }
            else
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
                Delay = TimeSpan.FromSeconds(5),
                MaxDelay = TimeSpan.FromSeconds(10)
            },
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using (ServiceBusClient client = new ServiceBusClient(connectionString, clientOptions))
        {
            var processorOptions = new ServiceBusSessionProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentSessions = 1,                
                MaxConcurrentCallsPerSession = 1,
                MaxAutoLockRenewalDuration = TimeSpan.Zero
            };

            ServiceBusSessionProcessor processor = client.CreateSessionProcessor(topicName, subscriptionName, processorOptions);

            processor.ProcessMessageAsync += async args =>
            {
                string message = args.Message.Body.ToString();
                var fruitName = args.Message.SessionId;
                
                if (fruitName == "Banana")
                {
                    logger.LogInformation($"Start processing {fruitName} : {message} for about 5 minutes");

                    // Simulate long running process
                    await Task.Delay(TimeSpan.FromMinutes(5));

                    logger.LogInformation($"Complete processing message from fruit called \"{fruitName}\": {message}");
                }
                else
                    await Task.Delay(1000);

                await args.CompleteMessageAsync(args.Message);
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

    static async Task ProcessMessagesWithSessionStateAsync(string connectionString, string topicName, string subscriptionName, ILogger logger)
    {
        var clientOptions = new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions
            {
                Mode = ServiceBusRetryMode.Fixed
            },
            TransportType = ServiceBusTransportType.AmqpTcp
        };

        await using (ServiceBusClient client = new ServiceBusClient(connectionString, clientOptions))
        {
            var processorOptions = new ServiceBusSessionProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentSessions = 1,
                MaxConcurrentCallsPerSession = 1,
                MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(5)
            };

            ServiceBusSessionProcessor processor = client.CreateSessionProcessor(topicName, subscriptionName, processorOptions);

            processor.ProcessMessageAsync += async args =>
            {
                string message = args.Message.Body.ToString();
                var sessionId = args.Message.SessionId;

                logger.LogInformation($"Set session state for {sessionId}");

                // Set the session state
                byte[] sessionStateBytes = new byte[256 * 256];
                await args.SetSessionStateAsync(new BinaryData(sessionStateBytes));

                logger.LogInformation($"Start processing {sessionId} : {message} for about 5 seconds");

                logger.LogInformation($"Complete processing message from fruit called \"{sessionId}\": {message}");

                await args.CompleteMessageAsync(args.Message);
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
