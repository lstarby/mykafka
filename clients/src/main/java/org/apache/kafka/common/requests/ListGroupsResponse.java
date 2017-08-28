/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListGroupsResponse extends AbstractResponse {

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String GROUPS_KEY_NAME = "groups";
    public static final String GROUP_ID_KEY_NAME = "group_id";
    public static final String PROTOCOL_TYPE_KEY_NAME = "protocol_type";

    /**
     * Possible error codes:
     *
     * COORDINATOR_NOT_AVAILABLE (15)
     * AUTHORIZATION_FAILED (29)
     */

    private final Errors error;
    private final int throttleTimeMs;
    private final List<Group> groups;

    public ListGroupsResponse(Errors error, List<Group> groups) {
        this(DEFAULT_THROTTLE_TIME, error, groups);
    }

    public ListGroupsResponse(int throttleTimeMs, Errors error, List<Group> groups) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.groups = groups;
    }

    public ListGroupsResponse(Struct struct) {
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        this.error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        this.groups = new ArrayList<>();
        for (Object groupObj : struct.getArray(GROUPS_KEY_NAME)) {
            Struct groupStruct = (Struct) groupObj;
            String groupId = groupStruct.getString(GROUP_ID_KEY_NAME);
            String protocolType = groupStruct.getString(PROTOCOL_TYPE_KEY_NAME);
            this.groups.add(new Group(groupId, protocolType));
        }
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<Group> groups() {
        return groups;
    }

    public Errors error() {
        return error;
    }

    public static class Group {
        private final String groupId;
        private final String protocolType;

        public Group(String groupId, String protocolType) {
            this.groupId = groupId;
            this.protocolType = protocolType;
        }

        public String groupId() {
            return groupId;
        }

        public String protocolType() {
            return protocolType;
        }

    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.LIST_GROUPS.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        List<Struct> groupList = new ArrayList<>();
        for (Group group : groups) {
            Struct groupStruct = struct.instance(GROUPS_KEY_NAME);
            groupStruct.set(GROUP_ID_KEY_NAME, group.groupId);
            groupStruct.set(PROTOCOL_TYPE_KEY_NAME, group.protocolType);
            groupList.add(groupStruct);
        }
        struct.set(GROUPS_KEY_NAME, groupList.toArray());
        return struct;
    }

    public static ListGroupsResponse fromError(Errors error) {
        return fromError(DEFAULT_THROTTLE_TIME, error);
    }

    public static ListGroupsResponse fromError(int throttleTimeMs, Errors error) {
        return new ListGroupsResponse(throttleTimeMs, error, Collections.<Group>emptyList());
    }

    public static ListGroupsResponse parse(ByteBuffer buffer, short version) {
        return new ListGroupsResponse(ApiKeys.LIST_GROUPS.parseResponse(version, buffer));
    }

}
