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

package com.alibaba.nacos.naming.pojo;

/**
 * Record to transfer and store in Nacos cluster.
   RecordListener接口的泛型为指定了该监听器所要监听的实体类型。这个类型是一个Record接口的子接口。
   Record是一个在Nacos集群中传输和存储的记录。
 * @author nkorange
 * @since 1.0.0
 */
public interface Record {
    
    /**
     * get the checksum of this record, usually for record comparison.
     *
     * @return checksum of record
     */
    String getChecksum();
}
