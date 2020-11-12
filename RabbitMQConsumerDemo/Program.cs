using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQConsumerDemo
{
    internal static class Program
    {
        private static async Task Main(string[] args)
        {
            const string queueName = "ha.telemetry";

            Console.WriteLine("RabbitMQ Consumer Demo");

            try
            {
                var connectionFactory = new ConnectionFactory()
                {
                    HostName = "10.190.0.14",
                    UserName = "test",
                    Password = "test",
                    Port = 9101,
                    RequestedConnectionTimeout = TimeSpan.FromMilliseconds(3000), // milliseconds
                };

                using (var rabbitConnection = connectionFactory.CreateConnection())
                {
                    using (var channel = rabbitConnection.CreateModel())
                    {
                        channel.QueueDeclare(
                            queue: queueName,
                            durable: true,  // durable queue
                            exclusive: false,
                            autoDelete: false,
                            arguments: null);

                        CancellationTokenSource cts = new CancellationTokenSource();
                        Console.CancelKeyPress += (_, e) =>
                        {
                            e.Cancel = true; // prevent the process from terminating.
                            cts.Cancel();
                        };

                        var consumer = new EventingBasicConsumer(channel);

                        consumer.Received += (model, ea) =>
                        {
                            var body = ea.Body.ToArray();
                            var message = Encoding.UTF8.GetString(body);

                            Console.WriteLine(" [x] Received {0}; Id: {1}; Type: {2};", message, ea.BasicProperties.MessageId, ea.BasicProperties.Type);

                            // positively acknowledge a single delivery, the message will be discarded
                            channel.BasicAck(ea.DeliveryTag, false);

                            // requeue all unacknowledged deliveries up to this delivery tag
                            //channel.BasicNack(ea.DeliveryTag, false, true);
                        };

                        channel.BasicConsume(queue: queueName,
                                             autoAck: false, // no automatic confirms
                                             consumer: consumer);

                        await Task.Delay(-1, cts.Token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.ToString());
                Console.ForegroundColor = ConsoleColor.White;
            }

            Console.WriteLine($"");
            Console.WriteLine($"Press any key to finish.");
            Console.Read();
        }
    }
}