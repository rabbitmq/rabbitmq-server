using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Text;
using System.Web.Http;

namespace WebApiHttpAuthService.Controllers
{
    [RoutePrefix("auth")]
    public class AuthController : ApiController
    {
        // Note: the following is necessary to ensure that no
        // BOM is part of the response
        private static readonly UTF8Encoding encoding = new UTF8Encoding(false);

        [Route("user")]
        [HttpPost]
        public HttpResponseMessage user(FormDataCollection form)
        {
            string content = "allow administrator management";
            try
            {
                if (form != null)
                {
                    string username = form.Get("username");
                    string password = form.Get("password");

                    if(username=="authuser") //Sample check you can put your custom logic over here
                        content = "deny";

                    string userlog = string.Format("user :{0}, password :{1}", username, password);
                }
            }
            catch(Exception ex)
            {
                //check or log error
            }

            var resp = new HttpResponseMessage(HttpStatusCode.OK);
            resp.Content = new StringContent(content, encoding, "text/plain");
            return resp;
        }

        [Route("vhost")]
        [HttpPost]
        public HttpResponseMessage vhost(FormDataCollection form)
        {
            string content = "allow";
            try
            {
                if (form != null)
                {
                    string username = form.Get("username");
                    string ip = form.Get("ip");

                    if (username == "authuser") //Sample checks you can put your custom logic over here
                        content = "deny";

                    string userlog = string.Format("user :{0}, ip :{1}", username, ip);
                }
            }
            catch (Exception ex)
            {
                //check or log error
            }

            var resp = new HttpResponseMessage(HttpStatusCode.OK);
            resp.Content = new StringContent(content, encoding, "text/plain");
            return resp;
        }

        [Route("resource")]
        [HttpPost]
        public HttpResponseMessage resource(FormDataCollection form)
        {
            string content = "allow";

            try
            {
                if (form != null)
                {
                    string username = form.Get("username");
                    string vhost = form.Get("vhost");
                    string resource = form.Get("resource");
                    string name = form.Get("name");
                    string permission = form.Get("permission");

                    if (username == "authuser") //Sample checks you can put your custom logic over here
                        content = "deny";

                    string userlog = string.Format("user :{0}, vhost :{1}, resource :{2}, name: {3}, permission: {4}", username, vhost, resource, name, permission);
                   
                }
            }
            catch (Exception ex)
            {
                //check or log error
            }


            var resp = new HttpResponseMessage(HttpStatusCode.OK);
            resp.Content = new StringContent(content, encoding, "text/plain");
            return resp;
        }

        [Route("topic")]
        [HttpPost]
        public HttpResponseMessage topic(FormDataCollection form)
        {
            string content = "allow";
            try
            {
                if (form != null)
                {
                    string username = form.Get("username");
                    string vhost = form.Get("vhost");
                    string resource = form.Get("resource");
                    string name = form.Get("name");
                    string permission = form.Get("permission");
                    string routing_key = form.Get("routing_key");

                    if (username == "authuser") //Sample checks you can put your custom logic over here
                        content = "deny";

                    string userlog = string.Format("user :{0}, vhost :{1}, resource :{2}, name: {3}, permission: {4}, routing_key :{5}", username, vhost, resource, name, permission, routing_key);

                }
            }
            catch (Exception ex)
            {
                //check or log error
            }

            var resp = new HttpResponseMessage(HttpStatusCode.OK);
            resp.Content = new StringContent(content, encoding, "text/plain");
            return resp;
        }
                       


    }
}
