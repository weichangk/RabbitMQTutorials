using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Utils;

namespace RabbitProducerConfirmListener
{
    class Program
    {
        private const int MESSAGE_COUNT = 50_000;

        private static readonly String EXCHANGE_NAME = "exchange_ProductConfirm";

        private static readonly String ROUTING_KEY = "routingkey_ProductConfirm";

        private static readonly String QUEUE_NAME = "queue_ProductConfirm";

        static void Main(string[] args)
        {

            PublishMessagesIndividually();
            PublishMessagesInBatch();
            HandlePublishConfirmsAsynchronously();

            Console.ReadLine();
        }

        /// <summary>
        /// 生产者每条消息同步确认
        /// </summary>
        private static void PublishMessagesIndividually()
        {
            using (IConnection connection = MQClientConnUtils.GetConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
                channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
                channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

                channel.ConfirmSelect();
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = true;
                var timer = new Stopwatch();
                timer.Start();
                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = Encoding.UTF8.GetBytes(i.ToString());
                    channel.BasicPublish(exchange: EXCHANGE_NAME, routingKey: ROUTING_KEY, basicProperties: basicProperties, body: body);
                    channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));
                }
                timer.Stop();
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages individually in {timer.ElapsedMilliseconds:N0} ms");
            }
        }

        /// <summary>
        /// 生产者批量消息同步确认
        /// </summary>
        private static void PublishMessagesInBatch()
        {
            using (IConnection connection = MQClientConnUtils.GetConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
                channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
                channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

                channel.ConfirmSelect();
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = true;
                var batchSize = 100;
                var outstandingMessageCount = 0;
                var timer = new Stopwatch();
                timer.Start();
                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = Encoding.UTF8.GetBytes(i.ToString());
                    channel.BasicPublish(exchange: EXCHANGE_NAME, routingKey: ROUTING_KEY, basicProperties: basicProperties, body: body);
                    outstandingMessageCount++;

                    if (outstandingMessageCount == batchSize)
                    {
                        channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));
                        outstandingMessageCount = 0;
                    }
                }

                if (outstandingMessageCount > 0)
                    channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));

                timer.Stop();
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages in batch in {timer.ElapsedMilliseconds:N0} ms");
            }
        }

        /// <summary>
        /// 生产者消息异步确认
        /// </summary>
        private static void HandlePublishConfirmsAsynchronously()
        {
            using (IConnection connection = MQClientConnUtils.GetConnection())
            using (IModel channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
                channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
                channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

                channel.ConfirmSelect();
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = true;

                var outstandingConfirms = new ConcurrentDictionary<ulong, string>();

                void cleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
                {
                    if (multiple)
                    {
                        var confirmed = outstandingConfirms.Where(k => k.Key <= sequenceNumber);
                        foreach (var entry in confirmed)
                            outstandingConfirms.TryRemove(entry.Key, out _);
                    }
                    else
                        outstandingConfirms.TryRemove(sequenceNumber, out _);
                }

                channel.BasicAcks += (sender, ea) => cleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
                channel.BasicNacks += (sender, ea) =>
                {
                    outstandingConfirms.TryGetValue(ea.DeliveryTag, out string body);
                    Console.WriteLine($"Message with body {body} has been nack-ed. Sequence number: {ea.DeliveryTag}, multiple: {ea.Multiple}");
                    cleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
                };

                var timer = new Stopwatch();
                timer.Start();
                for (int i = 0; i < MESSAGE_COUNT; i++)
                {
                    var body = i.ToString();
                    outstandingConfirms.TryAdd(channel.NextPublishSeqNo, i.ToString());
                    channel.BasicPublish(exchange: EXCHANGE_NAME, routingKey: ROUTING_KEY, basicProperties: basicProperties, body: Encoding.UTF8.GetBytes(body));
                }

                if (!WaitUntil(60, () => outstandingConfirms.IsEmpty))
                    throw new Exception("All messages could not be confirmed in 60 seconds");

                timer.Stop();
                Console.WriteLine($"Published {MESSAGE_COUNT:N0} messages and handled confirm asynchronously {timer.ElapsedMilliseconds:N0} ms");
            }
        }

        private static bool WaitUntil(int numberOfSeconds, Func<bool> condition)
        {
            int waited = 0;
            while (!condition() && waited < numberOfSeconds * 1000)
            {
                Thread.Sleep(100);
                waited += 100;
            }

            return condition();
        }
    }
}
