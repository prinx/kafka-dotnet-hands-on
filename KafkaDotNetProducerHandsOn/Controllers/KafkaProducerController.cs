using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;

namespace KafkaDotNetProducerHandsOn.Controllers
{
    [Route("api/kafka")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig 
        {
            BootstrapServers = "localhost:9092"
        };
        private readonly string topic = "product_created_topic";

        [HttpPost]
        public IActionResult Post([FromQuery] string message)
        {
            return Created(string.Empty, SendToKafka(topic, message));
        }

        private Object SendToKafka(string topic, string message)
        {
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Something went wrong: {e}");
                }
            }

            return null;
        }
    }
}
