/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
 */
package com.rabbitmq.examples

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RabbitmqAuthBackendSpringBootKotlinApplication

fun main(args: Array<String>) {
    runApplication<RabbitmqAuthBackendSpringBootKotlinApplication>(*args)
}
