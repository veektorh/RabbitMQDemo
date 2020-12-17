using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Configuration;
using System.Text;

namespace RabbitMqConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = ConfigurationManager.AppSettings["hostname"],
                    Port = Convert.ToInt32(ConfigurationManager.AppSettings["port"]),
                    UserName = ConfigurationManager.AppSettings["username"],
                    Password = ConfigurationManager.AppSettings["password"],
                    //VirtualHost = ConfigurationManager.AppSettings["vhost"]
                };

                var connection = factory.CreateConnection();

                binlistListener(connection);
                testListener(connection);
                Console.Read();
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occured, Make sure rabbit mq is running");
                Console.WriteLine(ex.Message);
                Console.Read();
            }
        }

        public static void testListener(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare("test", durable: true, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Message Received on test queue - {message}");
            };

            channel.BasicConsume("test", autoAck: true, consumer);

            Console.WriteLine("Listening for messages on test queue");
        }



        public static void binlistListener(IConnection connection)
        {
            var channel = connection.CreateModel();

            channel.QueueDeclare("binlist", durable: true, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Message Received on bin queue - {message}");
            };

            channel.BasicConsume("binlist", autoAck: true, consumer);
            Console.WriteLine("Listening for messages on binlist queue");
        }

    }
}
