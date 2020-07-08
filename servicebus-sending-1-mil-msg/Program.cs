using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace servicebus_sending_1_mil_msg
{
    public class Program
    {
        private const string ServiceBusConnectionString = "";

        private static readonly MessageSender MyMessageSender;

        static Program()
        {
            MyMessageSender = new MessageSender(ServiceBusConnectionString, "test-topic", RetryPolicy.NoRetry);
        }

        static async Task Main(string[] args)
        {
            await SendSeriesOfMessages(100);
            await SendSeriesOfMessages(500);
            await SendSeriesOfMessages(1000);
            await SendSeriesOfMessages(2000);
            await SendSeriesOfMessages(5000);
            await SendSeriesOfMessages(10000);
            await SendSeriesOfMessages(20000);
            await SendSeriesOfMessages(50000);
            await SendSeriesOfMessages(100000);


            Console.WriteLine("press any key to continue...");
            Console.ReadKey();
        }

        private static async Task SendSeriesOfMessages(int numberOfMessagesToSend)
        {
            byte[] payload = new byte[60000];
            for (int i = 0; i < 60000; i++)
            {
                payload[i] = 1;
            }

            Console.WriteLine($"use case ({numberOfMessagesToSend} messages) -> send msg one by one");
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < numberOfMessagesToSend; i++)
                {
                    await MyMessageSender.SendAsync(new Message(payload)).ConfigureAwait(false);
                }

                stopwatch.Stop();
                Console.WriteLine($"use case ({numberOfMessagesToSend} messages) -> send msg one by one, elapsed: {stopwatch.Elapsed.TotalSeconds:N0} s");
            }

            Console.WriteLine($"use case ({numberOfMessagesToSend} messages) -> send batch");
            {
                List<Message> collectionOf4Messages = new List<Message>(4);
                for (int j = 0; j < 4; j++)
                {
                    collectionOf4Messages.Add(new Message(payload));
                }

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                int numberOfBatches = numberOfMessagesToSend / 4;

                for (int i = 0; i < numberOfBatches; i++)
                {
                    await MyMessageSender.SendAsync(collectionOf4Messages).ConfigureAwait(false);
                }

                stopwatch.Stop();
                Console.WriteLine($"use case ({numberOfMessagesToSend} messages) -> send batch, elapsed: {stopwatch.Elapsed.TotalSeconds:N0} s");
            }
        }
    }
}
