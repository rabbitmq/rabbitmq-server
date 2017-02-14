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
 * Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
 */

package com.rabbitmq.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.springframework.util.StringUtils.collectionToDelimitedString;

/**
 *
 */
@RestController
@RequestMapping(path = "/auth", method = { RequestMethod.GET, RequestMethod.POST })
public class AuthBackendHttpController {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuthBackendHttpController.class);

    private final Map<String, User> users = new HashMap<String, User>() {{
        put("guest", new User("guest", "guest", asList("administrator", "management")));
    }};

    @RequestMapping("user")
    public String user(@RequestParam("username") String username,
                       @RequestParam("password") String password) {
        LOGGER.info("Trying to authenticate user {}", username);
        User user = users.get(username);
        if (user != null && user.getPassword().equals(password)) {
            return "allow " + collectionToDelimitedString(user.getTags(), " ");
        } else {
            return "deny";
        }
    }

    @RequestMapping("vhost")
    public String vhost(VirtualHostCheck check) {
        LOGGER.info("Checking vhost access with {}", check);
        return "allow";
    }

    @RequestMapping("resource")
    public String resource(ResourceCheck check) {
        LOGGER.info("Checking resource access with {}", check);
        return "allow";
    }

    @RequestMapping("topic")
    public String topic(TopicCheck check) {
        LOGGER.info("Checking topic access with {}", check);
        return check.getRouting_key().startsWith("a") ? "allow" : "deny";
    }
}
