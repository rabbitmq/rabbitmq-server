namespace RabbitMqAuthBackendHttp.Requests
{
    public class VhostAuthRequest
    {
        public required string UserName { get; set; }

        public required string Vhost { get; set; }

        public required string Ip { get; set; }
    }
}
