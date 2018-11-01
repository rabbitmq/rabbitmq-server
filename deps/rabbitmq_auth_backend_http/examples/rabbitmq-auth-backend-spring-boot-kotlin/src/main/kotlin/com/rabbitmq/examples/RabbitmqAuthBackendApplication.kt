package com.rabbitmq.examples

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RabbitmqAuthBackendSpringBootKotlinApplication

fun main(args: Array<String>) {
    runApplication<RabbitmqAuthBackendSpringBootKotlinApplication>(*args)
}
