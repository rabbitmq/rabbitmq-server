using System;
using Microsoft.AspNetCore.Mvc;
using RabbitMqAuthBackendHttp.Requests;

namespace RabbitMqAuthBackendHttp.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> Get()
        {
            return "AuthController";
        }

        [Route("user")]
        [HttpPost]
        public IActionResult CheckUser([FromForm]UserAuthRequest request)
        {
            var tags = new [] {"administrator", "management"};

            try
            {
                var userlog = string.Format("user : {0}, password : {1}", request.UserName, request.Password);
                Console.WriteLine(userlog);

                if (request.UserName == "authuser") //Sample check you can put your custom logic over here
                    return AuthResult.Deny();

            }
            catch (Exception ex)
            {
                //check or log error
            }

            return AuthResult.Allow(tags);
        }

        [Route("vhost")]
        [HttpPost]
        public IActionResult CheckVhost([FromForm]VhostAuthRequest request)
        {
            try
            {
                var userlog = string.Format("user : {0}, ip : {1}", request.UserName, request.Ip);
                Console.WriteLine(userlog);

                if (request.UserName == "authuser") //Sample checks you can put your custom logic over here
                    return AuthResult.Deny();
            }
            catch (Exception ex)
            {
                //check or log error
            }

            return AuthResult.Allow();
        }

        [Route("resource")]
        [HttpPost]
        public IActionResult CheckResource([FromForm]ResourceAuthRequest request)
        {
            try
            {
                var userlog = $"user : {request.UserName}, vhost : {request.Vhost}, resource : {request.Resource}, " +
                              $"name : {request.Name}, permission : {request.Permission}";
                Console.WriteLine(userlog);

                if (request.UserName == "authuser") //Sample checks you can put your custom logic over here
                    return AuthResult.Deny();
            }
            catch (Exception ex)
            {
                //check or log error
            }

            return AuthResult.Allow();
        }

        [Route("topic")]
        [HttpPost]
        public IActionResult CheckTopic([FromForm]TopicAuthRequest request)
        {
            try
            {
                var userlog = $"user : {request.UserName}, vhost : {request.Vhost}, resource : {request.Resource}, " +
                              $"name : {request.Name}, permission : {request.Permission}";
                Console.WriteLine(userlog);

                if (request.UserName == "authuser") //Sample checks you can put your custom logic over here
                    return AuthResult.Deny();
            }
            catch (Exception ex)
            {
                //check or log error
            }

            return AuthResult.Allow();
        }
    }
}