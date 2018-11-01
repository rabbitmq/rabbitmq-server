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

import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*

/**
 * Controller for the RabbitMQ authentication/authorisation as described
 * in https://github.com/rabbitmq/rabbitmq-auth-backend-http
 */
@RequestMapping(path = ["/auth"])
@RestController
class AuthController {

    private val ALLOW = "allow"
    private val DENY = "deny"

    private val logger = LoggerFactory.getLogger(AuthController::class.java!!)

    /**
     * user_path
     */
    @GetMapping(value = ["/user"], produces = ["text/plain"])
    fun checkUserCredentialsViaGET(passwordCheck: PasswordCheck): String {
        logger.info("checkUserCredentialsViaGET username: ${passwordCheck.username}")
        if (passwordCheck.username == "foo" && passwordCheck.password == "bar") {
            return ALLOW
        } else {
            return DENY
        }
    }

    @PostMapping(value = ["/user"], produces = ["text/plain"], consumes = [MediaType.APPLICATION_FORM_URLENCODED_VALUE])
    fun checkUserCredentialsViaPOST(@RequestParam map: Map<String, String>): String {
        logger.info("checkUserCredentialsViaPOST username: ${map}")
        if (map["username"] == "foo" && map["password"] == "bar") {
            return ALLOW
        } else {
            return DENY
        }
    }

    /**
     * vhost_path
     */
    @RequestMapping(value = ["/vhost"], produces = ["text/plain"])
    fun checkVhost(question: VirtualHostCheck): String {
        logger.info("checkVhost: $question")
        return "allow"
    }

    /**
     * resource_path
     */
    @RequestMapping(value = ["/resource"], produces = ["text/plain"])
    fun checkResource(question: ResourceCheck): String {
        logger.info("checkResource: $question")
        return "allow"
    }

    /**
     * topic_path
     */
    @RequestMapping(value = ["/topic"], produces = ["text/plain"])
    fun checkTopic(question: TopicCheck): String {
        logger.info("checkTopic: $question")
        return "allow"
    }

}

data class PasswordCheck(val username: String, val password: String)
data class VirtualHostCheck(val username: String, val vhost: String)
data class ResourceCheck(val username: String, val vhost: String, val resource: String, val name: String, val permission: String)
data class TopicCheck(val username: String, val vhost: String, val resource: String, val name: String, val permission: String, val routing_key: String)
