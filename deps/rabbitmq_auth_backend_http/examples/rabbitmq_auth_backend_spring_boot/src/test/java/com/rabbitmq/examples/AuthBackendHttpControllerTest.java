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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@RunWith(SpringRunner.class)
@WebMvcTest(AuthBackendHttpController.class)
public class AuthBackendHttpControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test public void authenticationAuthorisation() throws Exception {
        this.mvc.perform(get("/auth/user").param("username", "guest").param("password", "guest"))
            .andExpect(status().isOk()).andExpect(content().string("allow administrator management"));

        this.mvc.perform(get("/auth/user").param("username", "guest").param("password", "wrong"))
            .andExpect(status().isOk()).andExpect(content().string("deny"));

        this.mvc.perform(get("/auth/vhost").param("username", "guest").param("vhost", "/"))
            .andExpect(status().isOk()).andExpect(content().string("allow"));

        this.mvc.perform(get("/auth/resource")
            .param("username", "guest")
            .param("vhost", "/")
            .param("resource", "exchange")
            .param("name", "amq.topic")
            .param("permission", "write"))
            .andExpect(status().isOk()).andExpect(content().string("allow"));

        this.mvc.perform(get("/auth/topic")
            .param("username", "guest")
            .param("vhost", "/")
            .param("resource", "exchange")
            .param("name", "amq.topic")
            .param("permission", "write")
            .param("routing_key","a.b"))
            .andExpect(status().isOk()).andExpect(content().string("allow"));

        this.mvc.perform(get("/auth/topic")
            .param("username", "guest")
            .param("vhost", "/")
            .param("resource", "exchange")
            .param("name", "amq.topic")
            .param("permission", "write")
            .param("routing_key","b.b"))
            .andExpect(status().isOk()).andExpect(content().string("deny"));
    }

}
