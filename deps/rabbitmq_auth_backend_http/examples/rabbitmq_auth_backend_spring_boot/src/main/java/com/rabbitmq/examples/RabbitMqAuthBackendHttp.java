/**
 * The contents of this file are subject to the Mozilla Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License
 * at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and
 * limitations under the License.
 * <p>
 * The Original Code is RabbitMQ HTTP authentication.
 * <p>
 * The Initial Developer of the Original Code is VMware, Inc.
 * Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
 */

package com.rabbitmq.examples;

import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.ui.ModelMap;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.context.request.WebRequestInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.stream.Collectors;

@EnableAutoConfiguration
@SpringBootApplication
public class RabbitMqAuthBackendHttp {

    public static void main(String[] args) {
        SpringApplication.run(RabbitMqAuthBackendHttp.class, args);
    }

    // to enable: ./mvnw spring-boot:run -Dspring-boot.run.profiles=debug
    @Profile("debug")
    @Configuration
    static class DebugConfiguration implements WebMvcConfigurer {

        @Override
        public void addInterceptors(InterceptorRegistry registry) {

            registry.addWebRequestInterceptor(new WebRequestInterceptor() {
                @Override
                public void preHandle(WebRequest request) {
                    LoggerFactory.getLogger(DebugConfiguration.class).info(
                            "HTTP request parameters: {}",
                            request.getParameterMap()
                                    .entrySet().stream()
                                    .map(entry -> entry.getKey() + " = " + StringUtils.arrayToCommaDelimitedString(entry.getValue()))
                                    .collect(Collectors.toList())
                    );
                }

                @Override
                public void postHandle(WebRequest request, ModelMap model) {

                }

                @Override
                public void afterCompletion(WebRequest request, Exception ex) {

                }
            });
        }

    }

}
