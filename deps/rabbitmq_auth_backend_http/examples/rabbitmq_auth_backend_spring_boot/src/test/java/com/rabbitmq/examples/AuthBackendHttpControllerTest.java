/**
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0. If a copy of
 * the MPL was not distributed with this file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * <p>Copyright (c) 2017-2023 VMware, Inc. or its affiliates. All rights reserved.
 */
package com.rabbitmq.examples;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(AuthBackendHttpController.class)
public class AuthBackendHttpControllerTest {

  @Autowired private MockMvc mvc;

  @Test
  public void authenticationAuthorisation() throws Exception {
    this.mvc
        .perform(get("/auth/user").param("username", "guest").param("password", "guest"))
        .andExpect(status().isOk())
        .andExpect(content().string("allow administrator management"));

    this.mvc
        .perform(get("/auth/user").param("username", "guest").param("password", "wrong"))
        .andExpect(status().isOk())
        .andExpect(content().string("deny"));

    this.mvc
        .perform(get("/auth/vhost").param("username", "guest").param("vhost", "/"))
        .andExpect(status().isOk())
        .andExpect(content().string("allow"));

    this.mvc
        .perform(
            get("/auth/resource")
                .param("username", "guest")
                .param("vhost", "/")
                .param("resource", "exchange")
                .param("name", "amq.topic")
                .param("permission", "write"))
        .andExpect(status().isOk())
        .andExpect(content().string("allow"));

    this.mvc
        .perform(
            get("/auth/topic")
                .param("username", "guest")
                .param("vhost", "/")
                .param("resource", "exchange")
                .param("name", "amq.topic")
                .param("permission", "write")
                .param("routing_key", "a.b"))
        .andExpect(status().isOk())
        .andExpect(content().string("allow"));

    this.mvc
        .perform(
            get("/auth/topic")
                .param("username", "guest")
                .param("vhost", "/")
                .param("resource", "exchange")
                .param("name", "amq.topic")
                .param("permission", "write")
                .param("routing_key", "b.b"))
        .andExpect(status().isOk())
        .andExpect(content().string("deny"));
  }
}
