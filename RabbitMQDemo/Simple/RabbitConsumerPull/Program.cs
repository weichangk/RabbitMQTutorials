using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Utils;

namespace RabbitConsumerPull
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


            //拉（pull）模式消费消息
            BasicGetResult res = channel.BasicGet(QUEUE_NAME, true);
            if (res != null)
            {
                var message = Encoding.UTF8.GetString(res.Body.ToArray());
                Console.WriteLine(" [x] Received {0}", message);
            }

            //ReceivePull(channel);


            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }


        //由于某些限制，消费者在某个条件成立时才能消费消息。需要批量拉取消息进行处理。才考虑拉模式。否则没必要开线程或循环使用拉模式取代推模式。。。。
        private static void ReceivePull(IModel channel)
        {
            //存放消息实体List
            List<MessageEntity> list = new List<MessageEntity>();

            double start = new TimeSpan(DateTime.Now.Ticks).TotalSeconds;
            MessageEntity entity = new MessageEntity();

            while (true)
            {
                //拉取消息
                BasicGetResult res = channel.BasicGet(QUEUE_NAME, false);

                if (res == null)
                {
                    //间隔时间，如果超过10s还没有消费到新到消息，则将消息入库，保证实效性
                    double interval = (new TimeSpan(DateTime.Now.Ticks).TotalSeconds) - start;
                    if (list != null && interval > 10)
                    {
                        //批量确认消息
                        channel.BasicAck(entity.getTag(), true);

                        //模仿业务处理
                        //......

                        list.Clear();

                        start = new TimeSpan(DateTime.Now.Ticks).TotalSeconds;
                    }
                    continue;
                }
                String str = Encoding.UTF8.GetString(res.Body.ToArray());
                entity.setMessage(str);
                entity.setTag(res.DeliveryTag);
                list.Add(entity);
                //100条消息批量入库一次
                if (list.Count % 100 == 0)
                {
                    //批量确认消息
                    channel.BasicAck(entity.getTag(), true);
                    //模仿业务处理
                    //......

                    list.Clear();

                    start = new TimeSpan(DateTime.Now.Ticks).TotalSeconds;
                }

                Thread.Sleep(1000);
            }

        }
    }

    class MessageEntity
    {
        private String message;
        private ulong tag;

        public String getMessage()
        {
            return message;
        }

        public void setMessage(String message)
        {
            this.message = message;
        }

        public ulong getTag()
        {
            return tag;
        }

        public void setTag(ulong tag)
        {
            this.tag = tag;
        }
    }

}
