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
public class AttributeUpdateMessage extends AttributeMessage {

    private String name;
    private Object value;

    public AttributeUpdateMessage() {
    }
    
    public AttributeUpdateMessage(String sessionId, String name, Object value) {
        super(sessionId);
        this.name = name;
        this.value = value;
    }

    public AttributeUpdateMessage(String nodeId, String sessionId, String name, Object value) {
        super(nodeId, sessionId);
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }
    
    public Object getValue() {
        return value;
    }
    
}
