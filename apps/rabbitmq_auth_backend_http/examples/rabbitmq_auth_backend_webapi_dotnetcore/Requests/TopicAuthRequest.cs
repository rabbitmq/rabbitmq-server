using Microsoft.AspNetCore.Mvc;

namespace RabbitMqAuthBackendHttp.Requests
{
    public class TopicAuthRequest
    {
        public string UserName { get; set; }

        public string Vhost { get; set; }

        public string Name { get; set; }

        public Resource Resource { get; set; }

        public TopicPermission Permission { get; set; }

        [ModelBinder(Name = "routing_key")]
        public string RoutingKey { get; set; }
    }

    public enum TopicPermission
    {
        Write,

        Read
    }
}
