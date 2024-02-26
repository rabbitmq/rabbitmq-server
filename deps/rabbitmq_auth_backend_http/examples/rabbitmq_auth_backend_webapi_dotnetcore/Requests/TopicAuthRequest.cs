using Microsoft.AspNetCore.Mvc;

namespace RabbitMqAuthBackendHttp.Requests
{
    public class TopicAuthRequest
    {
        public required string UserName { get; set; }

        public required string Vhost { get; set; }

        public required string Name { get; set; }

        public Resource Resource { get; set; }

        public TopicPermission Permission { get; set; }

        [ModelBinder(Name = "routing_key")]
        public required string RoutingKey { get; set; }
    }

    public enum TopicPermission
    {
        Write,

        Read
    }
}
