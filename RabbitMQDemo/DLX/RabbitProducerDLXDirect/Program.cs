using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using Utils;

namespace RabbitProducerDLXDirect
{
    class Program
    {

        static void Main(string[] args)
        {

            // 实例化一个连接
            using (IConnection connection = MQClientConnUtils.GetConnection())
            // 实例化一个信道
            using (IModel channel = connection.CreateModel())
            {
                //声明正常交换器；type="direct" 、持久化的、非自动删除
                channel.ExchangeDeclare("exchange.normal.direct", "direct", true, false, null);
                //声明死信交换器；type="direct" 、持久化的、非自动删除
                channel.ExchangeDeclare("exchange.dlx", "direct", true, false, null);

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

                // 声明正常队列；持久化、非排他的、非自动删除
                channel.QueueDeclare("queue.dlx", true, false, false, null);
                //绑定死信队列
                channel.QueueBind("queue.dlx", "exchange.dlx", "routingkey.normal.direct");


                // 发送一条持久化的消息
                String message = "Hello DLX !";
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = true;//投递模式 delvery mode 为2 ，即消息会被持久化(即存入磁盘)在服务器中。
                channel.BasicPublish(exchange: "exchange.normal.direct",
                                     routingKey: "routingkey.normal.direct",
                                     basicProperties: basicProperties,
                                     body: System.Text.Encoding.UTF8.GetBytes(message));
                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
