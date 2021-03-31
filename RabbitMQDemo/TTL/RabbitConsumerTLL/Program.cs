using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using Utils;

namespace RabbitConsumerTLL
{
    class Program
    {
        private static readonly String EXCHANGE_NAME = "exchange_TTL";

        private static readonly String ROUTING_KEY = "routingkey_TTL";

        private static readonly String QUEUE_NAME = "queue_TTL";
        static void Main(string[] args)
        {
            // 实例化一个连接
            using IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            using IModel channel = connection.CreateModel();
            // 创建一个 type="direct" 、持久化的、非自动删除的交换器
            channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

            Dictionary<String, Object> arg = new()
            {
                // 设置队列过期时间，单位：毫秒
                { "x-message-ttl", 20000 }
            };

            // 创建一个持久化、非排他的、非自动删除的队列
            channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
            // 将交换器与队列通过路由键绑定，这个地方的ROUTING_KEY是binding key
            channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            //绑定消息接收事件
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);
            };

            //推（push）模式消费消息
            channel.BasicConsume(queue: QUEUE_NAME,
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
