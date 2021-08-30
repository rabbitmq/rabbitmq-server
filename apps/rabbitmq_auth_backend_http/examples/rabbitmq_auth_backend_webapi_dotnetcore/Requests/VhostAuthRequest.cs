namespace RabbitMqAuthBackendHttp.Requests
{
    public class VhostAuthRequest
    {
        public string UserName { get; set; }

        public string Vhost { get; set; }

        public string Ip { get; set; }
    }
}
 