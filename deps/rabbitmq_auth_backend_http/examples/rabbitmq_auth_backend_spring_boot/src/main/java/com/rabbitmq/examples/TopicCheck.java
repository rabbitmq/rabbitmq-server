/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
 */

package com.rabbitmq.examples;

public class TopicCheck extends ResourceCheck {

    private String routing_key;

    public String getRouting_key() {
        return routing_key;
    }

    public void setRouting_key(String routing_key) {
        this.routing_key = routing_key;
    }

    @Override
    public String toString() {
        return "TopicCheck{" +
            "routing_key='" + routing_key + '\'' +
            "} " + super.toString();
    }
}
