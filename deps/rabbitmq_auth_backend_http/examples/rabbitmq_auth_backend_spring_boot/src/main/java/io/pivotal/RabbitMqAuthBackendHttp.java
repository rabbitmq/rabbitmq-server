/**
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License
 * at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and
 * limitations under the License.
 *
 * The Original Code is RabbitMQ HTTP authentication.
 *
 * The Initial Developer of the Original Code is VMware, Inc.
 * Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
 */
    

package io.pivotal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@EnableAutoConfiguration
public class RabbitMqAuthBackendHttp {

    @RestController
    @RequestMapping(path = "/auth", method = { RequestMethod.GET, RequestMethod.POST})
    public static class AuthBackendHttpController {

        private final Map<String, User> users = new HashMap<String, User>() {{
            put("guest", new User("guest", "guest", Arrays.asList("administrator", "manager")));
        }};

        @RequestMapping("user")
        public String user(@RequestParam("username") String username, @RequestParam("password") String password) {
            User user = users.get(username);
            if(user != null && user.getPassword().equals(password)) {
                return "allow " + StringUtils.collectionToDelimitedString(user.getTags(), ",");
            } else {
                return "deny";
            }
        }

        @RequestMapping("vhost")
        public String vhost() {
            return "allow";
        }

        @RequestMapping("resource")
        public String resource() {
            return "allow";
        }

        @RequestMapping("topic")
        public String topic() {
            return "allow";
        }

    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RabbitMqAuthBackendHttp.class, args);
    }

}
