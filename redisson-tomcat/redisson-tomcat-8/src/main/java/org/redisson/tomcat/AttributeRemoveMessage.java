/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.tomcat;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class AttributeRemoveMessage extends AttributeMessage {

    private String name;
    
    public AttributeRemoveMessage() {
        super();
    }

    public AttributeRemoveMessage(String sessionId, String name) {
        super(sessionId);
        this.name = name;
    }

    public AttributeRemoveMessage(String nodeId, String sessionId, String name) {
        super(nodeId, sessionId);
        this.name = name;
    }

    public String getName() {
        return name;
    }
    
}
