using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using Utils;

namespace RabbitConsumerReject
{
    class Program
    {
        private static readonly String EXCHANGE_NAME = "exchange_demo2";

        private static readonly String ROUTING_KEY = "routingkey_demo2";

        private static readonly String QUEUE_NAME = "queue_demo2";
        static void Main(string[] args)
        {
            // 实例化一个连接
            using IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            using IModel channel = connection.CreateModel();
            // 创建一个 type="direct" 、持久化的、非自动删除的交换器
            channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
            // 创建一个持久化、非排他的、非自动删除的队列
            channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
            // 将交换器与队列通过路由键绑定，这个地方的ROUTING_KEY是binding key
            channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            bool autoAckStu = false;
            //绑定消息接收事件
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                if (message.Equals("Hello World !"))
                {
                    autoAckStu = false;
                    channel.BasicReject(ea.DeliveryTag, true);//拒绝消息，requeue为false ，则 RabbitMQ立即会把消息从队列中移除。requeue为true 留给其他消费者，自己还会再次接收到。。。
                    Console.WriteLine(" [x] Reject {0}", message);
                }
                else
                {
                    autoAckStu = true;
                    Console.WriteLine(" [x] Received {0}", message);
                }

            };


            //推（push）模式消费消息
            channel.BasicConsume(queue: QUEUE_NAME,
                                 autoAck: autoAckStu,
                                 consumer: consumer);



            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
