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
            MyMessageSender = new MessageSender(ServiceBusConnectionString, "platformevent", RetryPolicy.NoRetry);
        }

        static async Task Main(string[] args)
        {
            try
            {
                // await SendSeriesOfMessages(100);
                // await SendSeriesOfMessages(500);
                // await SendSeriesOfMessages(1000);
                // await SendSeriesOfMessages(2000);
                // await SendSeriesOfMessages(5000);
                // await SendSeriesOfMessages(10000);
                await SendSeriesOfMessages(20000);
                // await SendSeriesOfMessages(50000);
                // await SendSeriesOfMessages(100000);
            }
            catch (Exception e)
            {

            }
            finally
            {
                
            }


            Console.WriteLine("press any key to continue...");
            Console.ReadKey();
        }

        private static async Task SendSeriesOfMessages(int numberOfMessagesToSend)
        {
            byte[] payload = new byte[4000];
            for (int i = 0; i < payload.Length; i++)
            {
                payload[i] = 1;
            }

            Console.WriteLine($"use case ({numberOfMessagesToSend} messages) -> send batch");
            {
                List<Message> collectionOf4Messages = new List<Message>(4);
                var limit = 256000 / payload.Length;
                for (int j = 0; j < limit; j++)
                {
                    collectionOf4Messages.Add(new Message(payload));
                }

                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                int numberOfBatches = numberOfMessagesToSend / (256000 / payload.Length);

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
