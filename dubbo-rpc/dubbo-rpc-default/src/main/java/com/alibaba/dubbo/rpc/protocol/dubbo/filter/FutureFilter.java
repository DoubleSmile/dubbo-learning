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
package com.alibaba.dubbo.rpc.protocol.dubbo.filter;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;
import com.alibaba.dubbo.rpc.*;
import com.alibaba.dubbo.rpc.protocol.dubbo.FutureAdapter;
import com.alibaba.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Future;

/**
 * EventFilter
 * @author chao.liuc
 * @author william.liangf
 */
@Activate(group = Constants.CONSUMER)
public class FutureFilter implements Filter {

    protected static final Logger logger = LoggerFactory.getLogger(FutureFilter.class);

    public Result invoke(final Invoker<?> invoker, final Invocation invocation) throws RpcException {
    	final boolean isAsync = RpcUtils.isAsync(invoker.getUrl(), invocation);
        // 这里主要处理回调逻辑，主要区分三个时间：oninvoke：调用前触发，onreturn：调用后触发 onthrow：出现异常情况时候触发
    	fireInvokeCallback(invoker, invocation);
        //需要在调用前配置好是否有返回值，已供invoker判断是否需要返回future.
        Result result = invoker.invoke(invocation);
        if (isAsync) {
            asyncCallback(invoker, invocation);
        } else {
            syncCallback(invoker, invocation, result);
        }
        return result;
    }

    private void syncCallback(final Invoker<?> invoker, final Invocation invocation, final Result result) {
        if (result.hasException()) {
            fireThrowCallback(invoker, invocation, result.getException());
        } else {
            fireReturnCallback(invoker, invocation, result.getValue());
        }
    }
    
    private void asyncCallback(final Invoker<?> invoker, final Invocation invocation) {
        Future<?> f = RpcContext.getContext().getFuture();
        if (f instanceof FutureAdapter) {
            ResponseFuture future = ((FutureAdapter<?>)f).getFuture();
            future.setCallback(new ResponseCallback() {
                public void done(Object rpcResult) {
                    if (rpcResult == null){
                        logger.error(new IllegalStateException("invalid result value : null, expected "+Result.class.getName()));
                        return;
                    }
                    ///must be rpcResult
                    if (! (rpcResult instanceof Result)){
                        logger.error(new IllegalStateException("invalid result type :" + rpcResult.getClass() + ", expected "+Result.class.getName()));
                        return;
                    }
                    Result result = (Result) rpcResult;
                    if (result.hasException()) {
                        fireThrowCallback(invoker, invocation, result.getException());
                    } else {
                        fireReturnCallback(invoker, invocation, result.getValue());
                    }
                }
                public void caught(Throwable exception) {
                    fireThrowCallback(invoker, invocation, exception);
                }
            });
        }
    }

    private void fireInvokeCallback(final Invoker<?> invoker, final Invocation invocation) {
        final Method onInvokeMethod = (Method)StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_INVOKE_METHOD_KEY));
        final Object onInvokeInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_INVOKE_INSTANCE_KEY));
        
        if (onInvokeMethod == null  &&  onInvokeInst == null ){
            return ;
        }
        if (onInvokeMethod == null  ||  onInvokeInst == null ){
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() +" has a onreturn callback config , but no such "+(onInvokeMethod == null ? "method" : "instance")+" found. url:"+invoker.getUrl());
        }
        //由于JDK的安全检查耗时较多.所以通过setAccessible(true)的方式关闭安全检查就可以达到提升反射速度的目的
        if (onInvokeMethod != null && ! onInvokeMethod.isAccessible()) {
            onInvokeMethod.setAccessible(true);
        }
        //从之类可以看出oninvoke的方法参数要与调用的方法参数一致
        Object[] params = invocation.getArguments();
        try {
            onInvokeMethod.invoke(onInvokeInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }

    //代码解析见fireThrowCallback
    private void fireReturnCallback(final Invoker<?> invoker, final Invocation invocation, final Object result) {
        final Method onReturnMethod = (Method)StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_RETURN_METHOD_KEY));
        final Object onReturnInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_RETURN_INSTANCE_KEY));

        if (onReturnMethod == null  &&  onReturnInst == null ){
            return ;
        }
        
        if (onReturnMethod == null  ||  onReturnInst == null ){
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() +" has a onreturn callback config , but no such "+(onReturnMethod == null ? "method" : "instance")+" found. url:"+invoker.getUrl());
        }
        if (onReturnMethod != null && ! onReturnMethod.isAccessible()) {
            onReturnMethod.setAccessible(true);
        }
        
        Object[] args = invocation.getArguments();
        Object[] params ;
        Class<?>[] rParaTypes = onReturnMethod.getParameterTypes() ;
        if (rParaTypes.length >1 ) {
            if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)){
                params = new Object[2];
                params[0] = result;
                params[1] = args ;
            }else {
                params = new Object[args.length + 1];
                params[0] = result;
                System.arraycopy(args, 0, params, 1, args.length);
            }
        } else {
            params = new Object[] { result };
        }
        try {
            onReturnMethod.invoke(onReturnInst, params);
        } catch (InvocationTargetException e) {
            fireThrowCallback(invoker, invocation, e.getTargetException());
        } catch (Throwable e) {
            fireThrowCallback(invoker, invocation, e);
        }
    }
    
    private void fireThrowCallback(final Invoker<?> invoker, final Invocation invocation, final Throwable exception) {
        final Method onthrowMethod = (Method)StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_THROW_METHOD_KEY));
        final Object onthrowInst = StaticContext.getSystemContext().get(StaticContext.getKey(invoker.getUrl(), invocation.getMethodName(), Constants.ON_THROW_INSTANCE_KEY));

        if (onthrowMethod == null  &&  onthrowInst == null ){
            return ;
        }
        if (onthrowMethod == null  ||  onthrowInst == null ){
            throw new IllegalStateException("service:" + invoker.getUrl().getServiceKey() +" has a onthrow callback config , but no such "+(onthrowMethod == null ? "method" : "instance")+" found. url:"+invoker.getUrl());
        }
        if (onthrowMethod != null && ! onthrowMethod.isAccessible()) {
            onthrowMethod.setAccessible(true);
        }
        Class<?>[] rParaTypes = onthrowMethod.getParameterTypes() ;
        if (rParaTypes[0].isAssignableFrom(exception.getClass())){
            try {
                //因为onthrow方法的参数第一个值必须为异常信息，所以这里需要构造参数列表
                Object[] args = invocation.getArguments();
                Object[] params;
                
                if (rParaTypes.length >1 ) {
                    //原调用方法只有一个参数而且这个参数是数组（单独拎出来计算的好处是这样可以少复制一个数组）
                    if (rParaTypes.length == 2 && rParaTypes[1].isAssignableFrom(Object[].class)){
                        params = new Object[2];
                        params[0] = exception;
                        params[1] = args ;
                    }else {//原调用方法有多于一个参数
                        params = new Object[args.length + 1];
                        params[0] = exception;
                        System.arraycopy(args, 0, params, 1, args.length);
                    }
                } else {//原调用方法没有参数
                    params = new Object[] { exception };
                }
                onthrowMethod.invoke(onthrowInst,params);
            } catch (Throwable e) {
                logger.error(invocation.getMethodName() +".call back method invoke error . callback method :" + onthrowMethod + ", url:"+ invoker.getUrl(), e);
            } 
        } else {
            logger.error(invocation.getMethodName() +".call back method invoke error . callback method :" + onthrowMethod + ", url:"+ invoker.getUrl(), exception);
        }
    }
}