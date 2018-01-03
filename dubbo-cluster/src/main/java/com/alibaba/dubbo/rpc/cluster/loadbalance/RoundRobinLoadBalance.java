/*
 * Copyright 1999-2011 Alibaba Group.
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
package com.alibaba.dubbo.rpc.cluster.loadbalance;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.utils.AtomicPositiveInteger;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;

/**
 * Round robin load balance.
 *
 * @author qian.lei
 * @author william.liangf
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

	public static final String NAME = "roundrobin";

	private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

	private static final class IntegerWrapper {
		public IntegerWrapper(int value) {
			this.value = value;
		}

		private int value;

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public void decrement() {
			this.value--;
		}
	}
	//轮训是针对方法级别的，并不是所有服务调用
	protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
		String key = invokers.get(0).getUrl().getServiceKey() + "." + invocation.getMethodName();
		int length = invokers.size(); // 总个数
		int maxWeight = 0; // 最大权重
		int minWeight = Integer.MAX_VALUE; // 最小权重
		// invoker->weight
		final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Invoker<T>, IntegerWrapper>();
		int weightSum = 0;
		for (int i = 0; i < length; i++) {
			int weight = getWeight(invokers.get(i), invocation);
			maxWeight = Math.max(maxWeight, weight); // 累计最大权重
			minWeight = Math.min(minWeight, weight); // 累计最小权重
			if (weight > 0) {
				invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
				weightSum += weight;
			}
		}
		AtomicPositiveInteger sequence = sequences.get(key);
		if (sequence == null) {
			sequences.putIfAbsent(key, new AtomicPositiveInteger());
			sequence = sequences.get(key);
		}
		//currentSequence代表某个方法是第多少次被调用的，例如第1W次
		int currentSequence = sequence.getAndIncrement();
		if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
			// 可以把weightSu理解成一个大圈，mod就代表大圈中具体的轮训数值
			int mod = currentSequence % weightSum;
			//weightSum < maxWeight*length
			for (int i = 0; i < maxWeight; i++) {
				for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
					final Invoker<T> k = each.getKey();
					final IntegerWrapper v = each.getValue();
					//这里的逻辑比较抽象，本质上就是谁的权重越大，轮询到谁的次数就越多
					if (mod == 0 && v.getValue() > 0) {
						return k;
					}
					if (v.getValue() > 0) {
						v.decrement();
						mod--;
					}
				}
			}
		}
		// 取模轮循
		return invokers.get(currentSequence % length);
	}

}