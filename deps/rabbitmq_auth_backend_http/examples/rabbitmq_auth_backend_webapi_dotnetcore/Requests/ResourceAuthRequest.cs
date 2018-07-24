namespace RabbitMqAuthBackendHttp.Requests
{
    public class ResourceAuthRequest
    {
        public string UserName { get; set; }

        public string Vhost { get; set; }

        public Resource Resource { get; set; }

        public string Name { get; set; }

        public ResourcePermission Permission { get; set; }
    }

    public enum ResourcePermission
    {
        Configure,

        Write,

        Read
    }
}
