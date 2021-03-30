using RabbitMQ.Client;
using System;

namespace Utils
{
    public class MQClientConnUtils
    {
        /// <summary>
        /// 主机
        /// </summary>
        private static readonly String HOST = "127.0.0.1";

        /// <summary>
        /// RabbitMQ（AMQP） 服务端默认端口号为 5672
        /// </summary>
        private static readonly int PORT = 5672;

        /// <summary>
        /// 虚拟主机
        /// </summary>
        private static readonly String VIRTUALHOST = "/";

        /// <summary>
        /// 用户名
        /// </summary>
        private static readonly String USERNAME = "guest";

        /// <summary>
        /// 密码
        /// </summary>
        private static readonly String PASSWORD = "guest";

        /// <summary>
        /// 获取RabbitMQ Connection连接
        /// </summary>
        /// <returns></returns>
        public static IConnection GetConnection() 
        {
            //配置连接工厂
            ConnectionFactory connectionFactory = new()
            {
                HostName = HOST,
                Port = PORT,

                //如果配置有用户名密码以及vhost，则配置即可。
                UserName = USERNAME,
                Password = PASSWORD,
                VirtualHost = VIRTUALHOST,
            };

            //连接工厂创建连接
            return connectionFactory.CreateConnection();
        }

        /// <summary>
        /// 获取RabbitMQ Connection连接
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="virtualHost"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public static IConnection GetConnection(String host, int port, String virtualHost, String username, String password)
        {
            //配置连接工厂
            ConnectionFactory connectionFactory = new()
            {
                HostName = host,
                Port = port,

                //如果配置有用户名密码以及vhost，则配置即可。
                VirtualHost = virtualHost,
                UserName = username,
                Password = password,
            };

            //连接工厂创建连接
            return connectionFactory.CreateConnection();
        }

        /// <summary>
        /// 关闭RabbitMQ连接
        /// </summary>
        /// <param name="channel"></param>
        /// <param name="connection"></param>
        public static void Close(IModel channel, IConnection connection) 
        {
            if (channel != null) channel.Close();

            if (connection != null) connection.Close();
        }
    }
}
