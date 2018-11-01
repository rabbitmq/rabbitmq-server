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


    @Test
    fun `Check authentication for external users with GET`() {
        mockMvc.perform(get("/auth/user")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("username", "test-soc1")
                .param("password", "pass1"))
                .andExpect(status().isOk)
    }

    //@Test
    fun `Check authentication for external users with POST`() {
        mockMvc.perform(post("/auth/user").contentType(MediaType.APPLICATION_FORM_URLENCODED).content("username=guest&password=pass1"))
                .andExpect(status().isOk)
    }
}