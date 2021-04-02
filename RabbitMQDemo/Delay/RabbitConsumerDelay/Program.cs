using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using Utils;

namespace RabbitConsumerDelay
{
    class Program
    {
        static void Main(string[] args)
        {
            IConnection connection = MQClientConnUtils.GetConnection();
            IModel channel = connection.CreateModel();

            for (int i = 0; i < 5; i++)
            {
                int queueNum = i;
                System.Threading.Tasks.Task.Factory.StartNew(() =>
                {
                    Delay(channel, queueNum);
                });
                //Delay(channel, i);
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
            MQClientConnUtils.Close(channel, connection);
        }

        private static void Delay(IModel channel, int queueNum)
        {
            string queueName = $"queue_Delay{queueNum}.dlx";
            string routingkey = $"{queueName}_routingkey";

            channel.ExchangeDeclare("exchange_Delay.dlx", "direct", true, false, null);
            channel.QueueDeclare(queueName, true, false, false, null);
            channel.QueueBind(queueName, "exchange_Delay.dlx", routingkey);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Received {message}");
            };

            channel.BasicConsume(queue: queueName,
                                 autoAck: true,
                                 consumer: consumer);
        }
    }
}
