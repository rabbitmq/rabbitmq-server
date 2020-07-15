/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
 */
package com.rabbitmq.examples;

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.RestController

/**
 * Controller for the RabbitMQ authentication/authorisation as described
 * in https://github.com/rabbitmq/rabbitmq-auth-backend-http
 */
@RequestMapping(path = ["/auth"], method = [RequestMethod.GET, RequestMethod.POST])
@RestController
class AuthController {

    private val ALLOW = "allow"
    private val DENY = "deny"

    private val logger = LoggerFactory.getLogger(AuthController::class.java!!)

    /**
     * user_path
     */
    @RequestMapping(value = ["/user"], produces = ["text/plain"])
    fun checkUserCredentials(passwordCheck: PasswordCheck): String {
        logger.info("checkUserCredentials username: ${passwordCheck.username}")
        if (passwordCheck.username == "guest" && passwordCheck.password == "guest") {
            return "$ALLOW administrator management"
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
        return ALLOW
    }

    /**
     * resource_path
     */
    @RequestMapping(value = ["/resource"], produces = ["text/plain"])
    fun checkResource(question: ResourceCheck): String {
        logger.info("checkResource: $question")
        return ALLOW
    }

    /**
     * topic_path
     */
    @RequestMapping(value = ["/topic"], produces = ["text/plain"])
    fun checkTopic(question: TopicCheck): String {
        logger.info("checkTopic: $question")
        return if (question.routing_key.startsWith("a", false)) ALLOW else DENY
    }

}

data class PasswordCheck(val username: String, val password: String)
data class VirtualHostCheck(val username: String, val vhost: String)
data class ResourceCheck(val username: String, val vhost: String, val resource: String, val name: String, val permission: String)
data class TopicCheck(val username: String, val vhost: String, val resource: String, val name: String, val permission: String, val routing_key: String)
