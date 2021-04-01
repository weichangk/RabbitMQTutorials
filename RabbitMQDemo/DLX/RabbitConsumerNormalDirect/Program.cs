using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using Utils;

namespace RabbitConsumerNormalDirect
{
    class Program
    {
        static void Main(string[] args)
        {
            // 实例化一个连接
            using IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            using IModel channel = connection.CreateModel();
            // 创建一个 type="direct" 、持久化的、非自动删除的交换器
            channel.ExchangeDeclare("exchange.normal.direct", "direct", true, false, null);

            Dictionary<String, Object> arg = new()
            {
                // 设置队列过期时间，单位：毫秒
                { "x-message-ttl", 10000 },
                //设置死信交换器
                { "x-dead-letter-exchange", "exchange.dlx" }
            };
            //声明正常队列；持久化、非排他的、非自动删除、有过期时间，绑定声明好的死信交换器
            channel.QueueDeclare("queue.normal.direct", true, false, false, arg);
            //绑定正常队列
            channel.QueueBind("queue.normal.direct", "exchange.normal.direct", "routingkey.normal.direct");


            //绑定消息接收事件
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);
            };

            //推（push）模式消费消息
            channel.BasicConsume(queue: "queue.normal.direct",
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
