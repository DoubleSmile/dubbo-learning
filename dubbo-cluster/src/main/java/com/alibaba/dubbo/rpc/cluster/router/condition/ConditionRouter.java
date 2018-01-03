/*
 * Copyright 1999-2012 Alibaba Group.
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
package com.alibaba.dubbo.rpc.cluster.router.condition;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Router;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ConditionRouter
 * 
 * @author william.liangf
 */
public class ConditionRouter implements Router, Comparable<Router> {
    
    private static final Logger logger = LoggerFactory.getLogger(ConditionRouter.class);

    private final URL url;
    
    private final int priority;

    private final boolean force;

    private final Map<String, MatchPair> whenCondition;
    
    private final Map<String, MatchPair> thenCondition;

    public ConditionRouter(URL url) {
        this.url = url;
        //路由规则的优先级，用于排序，优先级越大越靠前执行，可不填，缺省为0。
        this.priority = url.getParameter(Constants.PRIORITY_KEY, 0);
        //当路由结果为空时，是否强制执行，如果不强制执行，路由结果为空的路由规则将自动失效，可不填，缺省为flase
        this.force = url.getParameter(Constants.FORCE_KEY, false);
        try {
            String rule = url.getParameterAndDecoded(Constants.RULE_KEY);
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            rule = rule.replace("consumer.", "").replace("provider.", "");
            int i = rule.indexOf("=>");
            // =>前的部分
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();
            // =>后的部分
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);
            // NOTE: When条件是允许为空的，外部业务来保证类似的约束条件
            this.whenCondition = when;
            this.thenCondition = then;
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation)
            throws RpcException {
        if (invokers == null || invokers.size() == 0) {
            return invokers;
        }
        try {
            //如果没有匹配到的前置条件后直接返回
            if (! matchWhen(url)) {
                return invokers;
            }
            List<Invoker<T>> result = new ArrayList<Invoker<T>>();
            if (thenCondition == null) {
            	logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                return result;
            }
            for (Invoker<T> invoker : invokers) {
                if (matchThen(invoker.getUrl(), url)) {
                    result.add(invoker);
                }
            }
            if (result.size() > 0) {
                return result;
                //感觉强制执行的话返回一个空的List并没有卵用呀
            } else if (force) {
            	logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(Constants.RULE_KEY));
            	return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        return invokers;
    }

    public URL getUrl() {
        return url;
    }

    public int compareTo(Router o) {
        if (o == null || o.getClass() != ConditionRouter.class) {
            return 1;
        }
        ConditionRouter c = (ConditionRouter) o;
        return this.priority == c.priority ? url.toFullString().compareTo(c.url.toFullString()) : (this.priority > c.priority ? 1 : -1);
    }

    public boolean matchWhen(URL url) {
        return matchCondition(whenCondition, url, null);
    }

    /**
     * @param url providerUrl
     * @param param consumerUrl
     * @return
     */
    public boolean matchThen(URL url, URL param) {
        return thenCondition != null && matchCondition(thenCondition, url, param);
    }
    
    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param) {
        Map<String, String> sample = url.toMap();
        for (Map.Entry<String, String> entry : sample.entrySet()) {
            String key = entry.getKey();
            //如果没有对应的pair值的就直接返回true，说明没有加任何路由的条件
            MatchPair pair = condition.get(key);
            if (pair != null && ! pair.isMatch(entry.getValue(), param)) {
                return false;
            }
        }
        return true;
    }
    
    private static Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");

    public static void main(String[] args) {
        URL url = URL.valueOf("condition://0.0.0.0/com.foo.BarService?category=routers&dynamic=false&rule=" +
                URL.encode("host=10.20.153.10=>host=10.20.153.11"));
        ConditionRouter router = new ConditionRouter(url);

    }
    /**
     * 将对应的rule信息解析为对应的MatchPair
     * host=10.20.153.10解析出来就是一个host：MatchPair，Matcher的matches内容为10.20.153.10
     * host!=10.20.153.10解析出来就是一个host：MatchPair，Matcher的mismatches内容为10.20.153.10
     * 可以理解为MatcherPair就是区分matches和mismatches的具体聚合类，拿到这个Matcher就拿到表达式初步解析后的数据
     * @param rule
     * @return
     * @throws ParseException
     */
    private static Map<String, MatchPair> parseRule(String rule)
            throws ParseException {
        Map<String, MatchPair> condition = new HashMap<String, MatchPair>();
        if(StringUtils.isBlank(rule)) {
            return condition;
        }        
        // 匹配或不匹配Key-Value对
        MatchPair pair = null;
        // 多个Value值
        Set<String> values = null;
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);
        //例如：host=10.20.153.10 第一次匹配的group1=''，group2='host'，第二次匹配的group1='='，group2='10.20.153.10'
        while (matcher.find()) { // 逐个匹配
            String separator = matcher.group(1);
            String content = matcher.group(2);
            // 表达式开始
            if (separator == null || separator.length() == 0) {
                pair = new MatchPair();
                //'host':new MatchPair()
                condition.put(content, pair);
            }
            // KV开始
            else if ("&".equals(separator)) {
                if (condition.get(content) == null) {
                    pair = new MatchPair();
                    condition.put(content, pair);
                } else {
                    condition.put(content, pair);
                }
            }
            // 匹配=号部分
            else if ("=".equals(separator)) {
                if (pair == null)
                    throw new RuntimeException();
                values = pair.matches;
                values.add(content);
            }
            // 匹配!=号部分
            else if ("!=".equals(separator)) {
                if (pair == null)
                    throw new RuntimeException();
                values = pair.mismatches;
                values.add(content);
            }
            // ,号直接跟着前面的=或者!=走
            else if (",".equals(separator)) {
                if (values == null || values.size() == 0)
                    throw new RuntimeException();
                values.add(content);
            } else {
                throw new RuntimeException();
            }
        }
        return condition;
    }

    private static final class MatchPair {
        //存放=后面的内容
        final Set<String> matches = new HashSet<String>();
        //存放!=后面的内容
        final Set<String> mismatches = new HashSet<String>();
        public boolean isMatch(String value, URL param) {
            for (String match : matches) {
                if (! UrlUtils.isMatchGlobPattern(match, value, param)) {
                    return false;
                }
            }
            for (String mismatch : mismatches) {
                if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                    return false;
                }
            }
            return true;
        }
    }
}