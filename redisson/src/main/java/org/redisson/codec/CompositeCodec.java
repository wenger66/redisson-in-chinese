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
package org.redisson.codec;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class CompositeCodec implements Codec {

    private final Codec mapKeyCodec;
    private final Codec mapValueCodec;
    private final Codec valueCodec;
    
    public CompositeCodec(Codec mapKeyCodec, Codec mapValueCodec) {
        this(mapKeyCodec, mapValueCodec, null);
    }
    
    public CompositeCodec(Codec mapKeyCodec, Codec mapValueCodec, Codec valueCodec) {
        super();
        this.mapKeyCodec = mapKeyCodec;
        this.mapValueCodec = mapValueCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public Decoder<Object> getMapValueDecoder() {
        return mapValueCodec.getMapKeyDecoder();
    }

    @Override
    public Encoder getMapValueEncoder() {
        return mapValueCodec.getMapValueEncoder();
    }

    @Override
    public Decoder<Object> getMapKeyDecoder() {
        return mapKeyCodec.getMapKeyDecoder();
    }

    @Override
    public Encoder getMapKeyEncoder() {
        return mapKeyCodec.getMapKeyEncoder();
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return valueCodec.getValueDecoder();
    }

    @Override
    public Encoder getValueEncoder() {
        return valueCodec.getValueEncoder();
    }

    @Override
    public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
    }

}
