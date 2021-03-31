using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using Utils;

namespace RabbitProducerTTLQueue
{
    class Program
    {

        private static readonly String EXCHANGE_NAME = "exchange_TTL";

        private static readonly String ROUTING_KEY = "routingkey_TTL";

        private static readonly String QUEUE_NAME = "queue_TTL";

        static void Main(string[] args)
        {

            // 实例化一个连接
            using (IConnection connection = MQClientConnUtils.GetConnection())
            // 实例化一个信道
            using (IModel channel = connection.CreateModel())
            {
                // 创建一个 type="direct" 、持久化的、非自动删除的交换器
                channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

                Dictionary<String, Object> arg = new()
                {
                    // 设置队列过期时间，单位：毫秒
                    { "x-message-ttl", 20000 }
                };

                // 创建一个持久化、非排他的、非自动删除的队列
                channel.QueueDeclare(QUEUE_NAME, true, false, false, arg);
                // 将交换器与队列通过路由键绑定，这个地方的ROUTING_KEY是binding key
                channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
                // 发送一条持久化的消息: hello world !
                String message = "Hello TTL Queue !";
                // 消息发布，这个地方的ROUTING_KEY是routing key，当binding key和routing key匹配时，队列才会收到
                var basicProperties = channel.CreateBasicProperties();
                basicProperties.Persistent = true;//投递模式 delvery mode 为2 ，即消息会被持久化(即存入磁盘)在服务器中。
                basicProperties.Expiration = "2000";///设置消息过期时间，单位：毫秒
                channel.BasicPublish(exchange: EXCHANGE_NAME,
                                     routingKey: ROUTING_KEY,
                                     basicProperties: basicProperties,
                                     body: System.Text.Encoding.UTF8.GetBytes(message));
                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
