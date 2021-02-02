using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Send
{
    class Program
    {
        private static readonly string EXCHANGE_NAME = "exchange_demo";
        private static readonly string ROUTING_KEY = "routingkey_demo";
        private static readonly string QUEUE_NAME = "queue_demo";
        private static readonly string IP_ADDRESS = "localhost";
        private static readonly int PORT = 5672;
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = IP_ADDRESS };
            using (var connection = factory.CreateConnection())//创建连接
            using (var channel = connection.CreateModel())//创建信道
            {
                //创建一个 type="direct" 、持久化的、非自动删除的交换器
                channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);

                //创建一个持久化、非排他的、非自动删除的队列
                channel.QueueDeclare(queue: QUEUE_NAME,
                                     durable: true,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                //将交换器与队列通过路由键绑定
                channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

                //发送一条持久化的消息: hello world!
                string message = "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);

                //发送消息
                channel.BasicPublish(exchange: EXCHANGE_NAME,
                                     routingKey: ROUTING_KEY,
                                     basicProperties: null,//???
                                     body: body);
                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
