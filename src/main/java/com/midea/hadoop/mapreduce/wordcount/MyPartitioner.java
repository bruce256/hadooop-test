/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.midea.hadoop.mapreduce.wordcount;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition keys by their {@link Object#hashCode()}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class MyPartitioner<K, V> extends Partitioner<K, V> {
	
	/**
	 * 取首字母在字母表里的序号做分区号，否则就是26
	 *
	 * @param key
	 * @param value
	 * @param numReduceTasks
	 * @return
	 */
	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		char c = key.toString().charAt(0);
		if (c >= 'a' && c <= 'z') return c - 'a';
		if (c >= 'A' && c <= 'Z') return c - 'A';
		// 第27个区的下标就是26
		return 26;
	}
	
}
