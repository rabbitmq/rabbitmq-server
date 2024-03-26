namespace RabbitMqAuthBackendHttp.Requests
{
    public class ResourceAuthRequest
    {
        public required string UserName { get; set; }

        public required string Vhost { get; set; }

        public Resource Resource { get; set; }

        public required string Name { get; set; }

        public ResourcePermission Permission { get; set; }
    }

    public enum ResourcePermission
    {
        Configure,

        Write,

        Read
    }
}
