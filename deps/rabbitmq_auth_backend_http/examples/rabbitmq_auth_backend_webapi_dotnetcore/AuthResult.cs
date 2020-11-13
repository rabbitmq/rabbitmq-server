using Microsoft.AspNetCore.Mvc;

namespace RabbitMqAuthBackendHttp
{
    public static class AuthResult
    {
        public static IActionResult Allow()
        {
            return new OkObjectResult("allow");
        }

        public static IActionResult Allow(params string[] tags)
        {
            return new OkObjectResult($"allow {string.Join(" ", tags)}");
        }

        public static IActionResult Deny()
        {
            return new OkObjectResult("deny");
        }
    }
}
