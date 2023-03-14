/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.consistency;

import com.alibaba.nacos.naming.pojo.Record;

/**
 * Data listener public interface.
 * Service类实现了RecordListener接口。这个接口是一个数据监听的接口。即Service类本身还是一个监听器，用于监听指定数据的变更或删除。
 * @author nacos
 */
// 泛型指定了当前监听器正在监听的数据类型
public interface RecordListener<T extends Record> {
    
    /**
     * Determine if the listener was registered with this key.
     * 判断当前监听器是否监听着指定key的数据。
     * @param key candidate key
     * @return true if the listener was registered with this key
     */
    boolean interests(String key);
    
    /**
     * Determine if the listener is to be removed by matching the 'key'.
     * 判断当前监听器是否已经不再监听当前指定key的数据
     * @param key key to match
     * @return true if match success
     */
    boolean matchUnlistenKey(String key);
    
    /**
     * Action to do if data of target key has changed.
     * 若指定key的数据发生了变更，则触发该方法的执行
     * @param key   target key
     * @param value data of the key
     * @throws Exception exception
     */
    void onChange(String key, T value) throws Exception;
    
    /**
     * Action to do if data of target key has been removed.
     * 若指定key的数据被删除，则触发该方法的执行
     * @param key target key
     * @throws Exception exception
     */
    void onDelete(String key) throws Exception;
}
