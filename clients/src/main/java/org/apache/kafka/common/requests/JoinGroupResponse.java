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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinGroupResponse extends AbstractResponse {

    private static final String ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * INCONSISTENT_GROUP_PROTOCOL (23)
     * UNKNOWN_MEMBER_ID (25)
     * INVALID_SESSION_TIMEOUT (26)
     * GROUP_AUTHORIZATION_FAILED (30)
     */

    private static final String GENERATION_ID_KEY_NAME = "generation_id";
    private static final String GROUP_PROTOCOL_KEY_NAME = "group_protocol";
    private static final String LEADER_ID_KEY_NAME = "leader_id";
    private static final String MEMBER_ID_KEY_NAME = "member_id";
    private static final String MEMBERS_KEY_NAME = "members";

    private static final String MEMBER_METADATA_KEY_NAME = "member_metadata";

    public static final String UNKNOWN_PROTOCOL = "";
    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_MEMBER_ID = "";

    private final int throttleTimeMs;
    private final Errors error;
    private final int generationId;
    private final String groupProtocol;
    private final String memberId;
    private final String leaderId;
    private final Map<String, ByteBuffer> members;

    public JoinGroupResponse(Errors error,
                             int generationId,
                             String groupProtocol,
                             String memberId,
                             String leaderId,
                             Map<String, ByteBuffer> groupMembers) {
        this(DEFAULT_THROTTLE_TIME, error, generationId, groupProtocol, memberId, leaderId, groupMembers);
    }

    public JoinGroupResponse(int throttleTimeMs,
            Errors error,
            int generationId,
            String groupProtocol,
            String memberId,
            String leaderId,
            Map<String, ByteBuffer> groupMembers) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.generationId = generationId;
        this.groupProtocol = groupProtocol;
        this.memberId = memberId;
        this.leaderId = leaderId;
        this.members = groupMembers;
    }

    public JoinGroupResponse(Struct struct) {
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        members = new HashMap<>();

        for (Object memberDataObj : struct.getArray(MEMBERS_KEY_NAME)) {
            Struct memberData = (Struct) memberDataObj;
            String memberId = memberData.getString(MEMBER_ID_KEY_NAME);
            ByteBuffer memberMetadata = memberData.getBytes(MEMBER_METADATA_KEY_NAME);
            members.put(memberId, memberMetadata);
        }
        error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        groupProtocol = struct.getString(GROUP_PROTOCOL_KEY_NAME);
        memberId = struct.getString(MEMBER_ID_KEY_NAME);
        leaderId = struct.getString(LEADER_ID_KEY_NAME);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    public int generationId() {
        return generationId;
    }

    public String groupProtocol() {
        return groupProtocol;
    }

    public String memberId() {
        return memberId;
    }

    public String leaderId() {
        return leaderId;
    }

    public boolean isLeader() {
        return memberId.equals(leaderId);
    }

    public Map<String, ByteBuffer> members() {
        return members;
    }

    public static JoinGroupResponse parse(ByteBuffer buffer, short version) {
        return new JoinGroupResponse(ApiKeys.JOIN_GROUP.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.JOIN_GROUP.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);

        struct.set(ERROR_CODE_KEY_NAME, error.code());
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(GROUP_PROTOCOL_KEY_NAME, groupProtocol);
        struct.set(MEMBER_ID_KEY_NAME, memberId);
        struct.set(LEADER_ID_KEY_NAME, leaderId);

        List<Struct> memberArray = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> entries : members.entrySet()) {
            Struct memberData = struct.instance(MEMBERS_KEY_NAME);
            memberData.set(MEMBER_ID_KEY_NAME, entries.getKey());
            memberData.set(MEMBER_METADATA_KEY_NAME, entries.getValue());
            memberArray.add(memberData);
        }
        struct.set(MEMBERS_KEY_NAME, memberArray.toArray());

        return struct;
    }
}
