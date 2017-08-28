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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateAclsResponse extends AbstractResponse {
    private final static String CREATION_RESPONSES = "creation_responses";

    public static class AclCreationResponse {
        private final ApiError error;

        public AclCreationResponse(ApiError error) {
            this.error = error;
        }

        public ApiError error() {
            return error;
        }

        @Override
        public String toString() {
            return "(" + error + ")";
        }
    }

    private final int throttleTimeMs;

    private final List<AclCreationResponse> aclCreationResponses;

    public CreateAclsResponse(int throttleTimeMs, List<AclCreationResponse> aclCreationResponses) {
        this.throttleTimeMs = throttleTimeMs;
        this.aclCreationResponses = aclCreationResponses;
    }

    public CreateAclsResponse(Struct struct) {
        this.throttleTimeMs = struct.getInt(THROTTLE_TIME_KEY_NAME);
        this.aclCreationResponses = new ArrayList<>();
        for (Object responseStructObj : struct.getArray(CREATION_RESPONSES)) {
            Struct responseStruct = (Struct) responseStructObj;
            ApiError error = new ApiError(responseStruct);
            this.aclCreationResponses.add(new AclCreationResponse(error));
        }
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_ACLS.responseSchema(version));
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        List<Struct> responseStructs = new ArrayList<>();
        for (AclCreationResponse response : aclCreationResponses) {
            Struct responseStruct = struct.instance(CREATION_RESPONSES);
            response.error.write(responseStruct);
            responseStructs.add(responseStruct);
        }
        struct.set(CREATION_RESPONSES, responseStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public List<AclCreationResponse> aclCreationResponses() {
        return aclCreationResponses;
    }

    public static CreateAclsResponse parse(ByteBuffer buffer, short version) {
        return new CreateAclsResponse(ApiKeys.CREATE_ACLS.responseSchema(version).read(buffer));
    }
}
