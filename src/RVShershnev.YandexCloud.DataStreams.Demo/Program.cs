using System.Text.Json;
using RVShershnev.YandexCloud.DataStreams;

namespace RVShershnev.YandexCloud.DataStreams.Demo
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = JsonSerializer.Deserialize<DataStreamsConfiguration>(File.ReadAllText("Configuration"));
            if (configuration == null)
            {
                return;
            }
            using(var client = new DataStreamsClient(configuration))
            {
                var orders = new List<Order>()
                {
                    new Order
                    {
                        Id = 1,
                        Title = "Book"
                    },
                    new Order
                    {
                        Id = 2,
                        Title = "Phone"
                    },
                    new Order
                    {
                        Id = 3,
                        Title = "Game"
                    },
                };
                await client.PutRecords(orders);
            }
        }
    }
}