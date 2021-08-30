/**
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
 */

package com.rabbitmq.examples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class User {

    private final String username, password;

    private final Collection<String> tags;

    public User(String username, String password) {
        this(username, password, Collections.<String>emptyList());
    }

    public User(String username, String password, Collection<String> tags) {
        this.username = username;
        this.password = password;
        this.tags = tags;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Collection<String> getTags() {
        return new ArrayList<String>(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        User user = (User) o;

        return username.equals(user.username);
    }

    @Override
    public int hashCode() {
        return username.hashCode();
    }
}
