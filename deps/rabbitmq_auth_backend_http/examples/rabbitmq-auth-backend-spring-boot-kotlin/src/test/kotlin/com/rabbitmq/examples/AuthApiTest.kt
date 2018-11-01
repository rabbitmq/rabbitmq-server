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
package com.rabbitmq.examples

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.http.MediaType
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@ExtendWith(SpringExtension::class)
@WebMvcTest
class AuthApiTest(@Autowired val mockMvc: MockMvc) {

    // user
    @Test
    fun `Check authentication for external users with GET`() {
        mockMvc.perform(get("/auth/user")
                .param("username", "foo")
                .param("password", "bar"))
                .andExpect(status().isOk)
    }

    @Test
    fun `Check authentication for external users with POST`() {
        mockMvc.perform(post("/auth/user").contentType(MediaType.APPLICATION_FORM_URLENCODED).content("username=foo&password=bar"))
                .andExpect(status().isOk)
    }

    // vhost
    @Test
    fun `Check vhost for external users with GET`() {
        mockMvc.perform(get("/auth/vhost")
                .param("username", "foo")
                .param("vhost", "bar"))
                .andExpect(status().isOk)
    }

    @Test
    fun `Check vhost for external users with POST`() {
        mockMvc.perform(post("/auth/vhost").contentType(MediaType.APPLICATION_FORM_URLENCODED).content("username=foo&vhost=bar"))
                .andExpect(status().isOk)
    }

    // resource
    @Test
    fun `Check resource_path for external users with GET`() {
        mockMvc.perform(get("/auth/resource")
                .param("username", "foo")
                .param("vhost", "bar")
                .param("resource", "yet")
                .param("name", "another")
                .param("permission", "word"))
                .andExpect(status().isOk)
    }

    @Test
    fun `Check resource_path for external users with POST`() {
        mockMvc.perform(post("/auth/resource").contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .content("username=foo&vhost=bar&resource=1&name=2&permission=3"))
                .andExpect(status().isOk)
    }

    // topic
    @Test
    fun `Check topic for external users with GET`() {
        mockMvc.perform(get("/auth/topic")
                .param("username", "foo")
                .param("vhost", "bar")
                .param("resource", "yet")
                .param("name", "another")
                .param("routing_key", "short")
                .param("permission", "word"))
                .andExpect(status().isOk)
    }

    @Test
    fun `Check topic for external users with POST`() {
        mockMvc.perform(post("/auth/topic").contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .content("username=foo&vhost=bar&resource=1&name=2&permission=3&routing_key=4"))
                .andExpect(status().isOk)
    }
}