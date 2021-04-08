using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using Utils;

namespace RabbitConsumerAck
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

            ulong deliveryTag = 0;
            //绑定消息接收事件
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                deliveryTag = ea.DeliveryTag;
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine(" [x] Reject {0}, deliveryTag {1}", message, deliveryTag);
            };


            //推（push）模式消费消息
            channel.BasicConsume(queue: QUEUE_NAME,
                                 autoAck: false,//为true时自动确认
                                 consumer: consumer);
            //当autoAck等于true时，RabbitMQ会自动把发送出去的消息置为确认，然后从内存(或者磁盘)中删除，而不管消费者是否真正地消费到了这些消息。

            //从消费者来说，如果在订阅消费队列时将autoAck参数设置为true，那么当消费者接收到相关消息之后，还没来得及处理就宕机了，这样也算数据丢失。
            //为了保证消息从队列可靠地达到消费者，RabbitMQ提供了消息确认机制，消费者在订阅队列时，可以指定 autoAck参数设置为false，并进行手动确认显式调Basic.Ack命令。
            //采用消息确认机制后，消费者就有足够的时间处理消息，不用担心处理消息过程中消费者进程挂掉后消息丢失的问题。

            Console.WriteLine(" Press [enter] to exit and Ack");

            Console.ReadLine();

            channel.BasicAck(deliveryTag: deliveryTag, multiple: false);//手动显示确认
        }
    }
}
