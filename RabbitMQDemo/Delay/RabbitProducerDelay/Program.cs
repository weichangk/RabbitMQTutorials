using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using Utils;

namespace RabbitProducerDelay
{
    class Program
    {
        static void Main(string[] args)
        {
            // 实例化一个连接
            IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            IModel channel = connection.CreateModel();
            for (int i = 0; i < 5; i++)
            {
                int queueNum = i;
                System.Threading.Tasks.Task.Factory.StartNew(() =>
                {
                    Delay(channel, queueNum, queueNum * 1000);
                });
                //Delay(channel, i, i * 1000);
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
            MQClientConnUtils.Close(channel, connection);
        }

        private static void Delay(IModel channel, int queueNum, int ttl)
        {
            string queueName = $"queue_Delay{queueNum}";
            string message = $"Hello Delay {queueNum}";
            string routingkey = $"{queueName}_routingkey";

            // 声明正常交换器（用于延时）； type="direct" 、持久化的、非自动删除=
            channel.ExchangeDeclare("exchange_Delay", "direct", true, false, null);
            //声明死信交换器（用于过期信息转移）；type="direct" 、持久化的、非自动删除
            channel.ExchangeDeclare("exchange_Delay.dlx", "direct", true, false, null);

            //声明死信队列（用于过期信息转移）；持久化、非排他的、非自动删除、有过期时间，绑定声明好的死信交换器
            channel.QueueDeclare($"{queueName}.dlx", true, false, false, null);
            Dictionary<String, Object> arg = new()
            {
                // 设置队列过期时间，单位：毫秒
                { "x-message-ttl", ttl },
                //设置死信交换器
                { "x-dead-letter-exchange", "exchange_Delay.dlx" }
            };
            // 声明正常队列（用于延时）；持久化、非排他的、非自动删除
            channel.QueueDeclare(queueName, true, false, false, arg);

            //绑定正常队列
            channel.QueueBind(queueName, "exchange_Delay", routingkey);
            //绑定死信队列
            channel.QueueBind($"{queueName}.dlx", "exchange_Delay.dlx", routingkey);


            // 消息发布，这个地方的ROUTING_KEY是routing key，当binding key和routing key匹配时，队列才会收到
            var basicProperties = channel.CreateBasicProperties();
            basicProperties.Persistent = true;//投递模式 delvery mode 为2 ，即消息会被持久化(即存入磁盘)在服务器中。
            channel.BasicPublish(exchange: "exchange_Delay",
                                 routingKey: routingkey,
                                 basicProperties: basicProperties,
                                 body: System.Text.Encoding.UTF8.GetBytes(message));
            Console.WriteLine($"Sent {message}");
        }
    }
}
