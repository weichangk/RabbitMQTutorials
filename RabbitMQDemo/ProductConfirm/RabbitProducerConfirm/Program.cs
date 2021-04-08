using RabbitMQ.Client;
using System;
using Utils;

namespace RabbitProducerConfirm
{
    class Program
    {
        private static readonly String EXCHANGE_NAME = "exchange_ProductConfirm";

        private static readonly String ROUTING_KEY = "routingkey_ProductConfirm";

        private static readonly String QUEUE_NAME = "queue_ProductConfirm";

        static void Main(string[] args)
        {

            // 实例化一个连接
            IConnection connection = MQClientConnUtils.GetConnection();
            // 实例化一个信道
            IModel channel = connection.CreateModel();
            // 创建一个 type="direct" 、持久化的、非自动删除的交换器
            channel.ExchangeDeclare(EXCHANGE_NAME, "direct", true, false, null);
            // 创建一个持久化、非排他的、非自动删除的队列
            channel.QueueDeclare(QUEUE_NAME, true, false, false, null);
            // 将交换器与队列通过路由键绑定，这个地方的ROUTING_KEY是binding key
            channel.QueueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);

            // 消息发布，这个地方的ROUTING_KEY是routing key，当binding key和routing key匹配时，队列才会收到
            var basicProperties = channel.CreateBasicProperties();
            basicProperties.Persistent = true;//投递模式 delvery mode 为2 ，即消息会被持久化(即存入磁盘)在服务器中。

            channel.ConfirmSelect();//开始confirm模式
            for (int i = 0; i < 20; i++)
            {
                // 发送消息
                String message = "hello, confirm message " + i;
                channel.BasicPublish(exchange: EXCHANGE_NAME,
                                        routingKey: ROUTING_KEY,
                                        basicProperties: basicProperties,
                                        body: System.Text.Encoding.UTF8.GetBytes(message));
            }

            //同步批量确认
            if (channel.WaitForConfirms())
            {
                Console.WriteLine(" [x] Sent message ok ");
            }
            else
            {
                Console.WriteLine(" [x] Sent message fail ");
            }

            Console.ReadLine();
            channel.Close();

        }
    }
}
