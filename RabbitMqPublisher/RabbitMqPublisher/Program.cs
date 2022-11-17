using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
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

            publishDeadLetterExchange(connection);
            consumeDeadLetterExchange(connection);

            Console.Read();
        }

        
        public static void publishDeadLetterExchange(IConnection connection)
        {
            var retryExchange = "retry.exchange";
            string workerQueue = "work.queue";
            string retryQueue = "retry.queue";
            var channel = connection.CreateModel();

            //retry exchange will receive message after its been dropped by the worker queue
            channel.ExchangeDeclare(retryExchange, ExchangeType.Fanout);
            channel.QueueDeclare(retryQueue, true, false, false, null);
            channel.QueueBind(retryQueue, retryExchange, retryQueue, null);


            channel.QueueDeclare(workerQueue, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", retryExchange}
            });

            var message = new { Name = "victor bolum", Scheme = "publish message" };
            var messageString = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(messageString);

            var properties = channel.CreateBasicProperties();
            properties.Expiration = "5000";
            channel.BasicPublish("", workerQueue, properties, body);

            Console.WriteLine($"published message");

        }

        public static void consumeDeadLetterExchange(IConnection connection)
        {
            var retryExchange = "retry.exchange";
            string retryQueue = "retry.queue";
            string workerQueue = "work.queue";
            var channel = connection.CreateModel();

            channel.QueueDeclare(retryQueue, true, false, false, null);
            channel.QueueBind(retryQueue, retryExchange, retryQueue, null);

            channel.QueueDeclare(workerQueue, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", retryExchange}
            });

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var messagee = Encoding.UTF8.GetString(body);
                Console.WriteLine("received [x] {0}", messagee);
                channel.BasicAck(ea.DeliveryTag, false);

                if (true)
                {
                    
                    var message = new { Name = "victor bolum", Scheme = "publish message" };
                    var messageString = JsonConvert.SerializeObject(message);
                    var body2 = Encoding.UTF8.GetBytes(messageString);

                    var properties = channel.CreateBasicProperties();
                    properties.Expiration = "5000";
                    channel.BasicPublish("", workerQueue, properties, body);
                }
            };
            channel.BasicConsume(queue: retryQueue, autoAck: false, consumer: consumer);
            

            Console.WriteLine($"consumed message");

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

        public static void publishDirectExchange1(IConnection connection)
        {
            var channel = connection.CreateModel();
            channel.ExchangeDeclare("nbxtran", ExchangeType.Direct);
           // channel.QueueDeclare("allTransactions", durable: true, exclusive: false, autoDelete: false, arguments: null);
            var message = new { Name = "victor bolum", Scheme = "publish message" };
            var messageString = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(messageString);

            channel.BasicPublish("nbxtran", "transactions.#", null, body);

            Console.WriteLine("published transactions.#");

        }
        public static void publishDirectExchange2(IConnection connection)
        {
            var channel = connection.CreateModel();
            channel.ExchangeDeclare("nbxtran", ExchangeType.Direct);
            //channel.QueueDeclare("allTransactions", durable: true, exclusive: false, autoDelete: false, arguments: null);
            var message = new { Name = "victor bolum", Scheme = "publish message" };
            var messageString = JsonConvert.SerializeObject(message);
            var body = Encoding.UTF8.GetBytes(messageString);

            channel.BasicPublish("nbxtran", "transactions2.#", null, body);

            Console.WriteLine("published transactions2.#");

        }




    }
}
