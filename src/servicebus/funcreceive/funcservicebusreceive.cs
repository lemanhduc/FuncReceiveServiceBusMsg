using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System.Text;

namespace funcreceive
{
    public static class funcservicebusreceive
    {
        static ITopicClient topicClient;
        static ISubscriptionClient subscriptionClient;
        [FunctionName("funcservicebusreceive")]
        public static Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string connectionStringServiceBus = "Endpoint=sb://plenttdata.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=H04PgzviylO3hman4MOuXfSWPHgrCUDOKd5835UQYas=";
            string topicName = "pletopic";
            string SubscriptionName = "plesubscription";

            subscriptionClient = new SubscriptionClient(connectionStringServiceBus, topicName, SubscriptionName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press any key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            topicClient = new TopicClient(connectionStringServiceBus, topicName);

            RegisterMessageHandlerAndReceiveMessages();

            Console.ReadKey();

           topicClient.CloseAsync().Wait();

            return null;
        }
        static void RegisterMessageHandlerAndReceiveMessages()
        {
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            // Register the function that will process messages
            subscriptionClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, System.Threading.CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: Sequence Number:{message.SystemProperties.SequenceNumber} \t Body:{Encoding.UTF8.GetString(message.Body)}");

            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
        }
        // Use this Handler to look at the exceptions received on the MessagePump
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Exception:: {exceptionReceivedEventArgs.Exception}.");
            return Task.CompletedTask;
        }
    }
}
