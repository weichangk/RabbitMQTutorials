using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using Utils;

namespace RabbitConsumerDLXDirect
{
    class Program
    {
        static void Main(string[] args)
        {
            // 实例化一个连接
            using IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            using IModel channel = connection.CreateModel();
            // 创建一个 type="direct" 、持久化的、非自动删除的死信交换器
            channel.ExchangeDeclare("exchange.dlx", "direct", true, false, null);


            //声明死信队列；持久化、非排他的、非自动删除
            channel.QueueDeclare("queue.dlx", true, false, false, null);
            //绑定死信队列
            channel.QueueBind("queue.dlx", "exchange.dlx", "rk");

            //绑定消息接收事件
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Received {0}", message);
            };

            //推（push）模式消费消息
            channel.BasicConsume(queue: "queue.dlx",
                                 autoAck: true,
                                 consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
