/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Also see ReferenceCountExchangeClient
 */
public class ReferenceCountManagedChannel extends ManagedChannel {

    // channel 的引用计数
    private final AtomicInteger referenceCount = new AtomicInteger(0);

    private ManagedChannel grpcChannel;

    /**
     *
     * @param delegated grpcChannel的管理对象
     */
    public ReferenceCountManagedChannel(ManagedChannel delegated) {
        this.grpcChannel = delegated;
    }

    /**
     * The reference count of current ExchangeClient, connection will be closed if all invokers destroyed.
     */
    public int incrementAndGetCount() {
        return referenceCount.incrementAndGet();
    }

    /**
     * 关闭该 grpcChannel; 仅当该 grpcChannel 的客户端引用计数<=0时，才真正关闭
     * @return
     */
    @Override
    public ManagedChannel shutdown() {
        if (referenceCount.decrementAndGet() <= 0) {
            return grpcChannel.shutdown();
        }
        return grpcChannel;
    }

    /**
     * 判断 grpcChannel 是关闭
     * @return
     */
    @Override
    public boolean isShutdown() {
        return grpcChannel.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return grpcChannel.isTerminated();
    }

    @Override
    public ManagedChannel shutdownNow() {
        // TODO
        return shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return grpcChannel.awaitTermination(timeout, unit);
    }

    /**
     * 构建一个 ClientCall 对象，此时并未触发远程调用行为
     * @param methodDescriptor
     * @param callOptions
     * @param <RequestT>
     * @param <ResponseT>
     * @return
     */
    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return grpcChannel.newCall(methodDescriptor, callOptions);
    }

    /**
     * 当前的 grpcChannel 连接到的目的地，通常为 "host:port"
     * @return
     */
    @Override
    public String authority() {
        return grpcChannel.authority();
    }
}
