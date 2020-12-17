using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Configuration;
using System.Text;

namespace RabbitMqPublisher
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var factory = new ConnectionFactory
            {
                HostName = ConfigurationManager.AppSettings["hostname"],
                Port = Convert.ToInt32(ConfigurationManager.AppSettings["port"]),
                UserName = ConfigurationManager.AppSettings["username"],
                Password = ConfigurationManager.AppSettings["password"],
                //VirtualHost = ConfigurationManager.AppSettings["vhost"]
            };

            var connection = factory.CreateConnection();

            publishBinlist(connection);
            publishTest(connection);


            Console.Read();
        }

        public static void publishBinlist(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare("binlist", durable: true, exclusive: false, autoDelete: false, arguments: null);
            var message = new { Name = "victor bolum", Scheme = "publish message" };
            var messageString = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(messageString);

            channel.BasicPublish("", "binlist", null, body);

            Console.WriteLine("published binlist");

        }

        public static void publishTest(IConnection connection)
        {
            var channel = connection.CreateModel();
            channel.QueueDeclare("test", durable: true, exclusive: false, autoDelete: false, arguments: null);
            var message2 = new { Name = "victor bolum", Scheme = "publish message" };
            var messageString2 = JsonConvert.SerializeObject(message2);
            var body2 = Encoding.UTF8.GetBytes(messageString2);

            channel.BasicPublish("", "test", null, body2);

            Console.WriteLine("published test");
        }



    }
}
