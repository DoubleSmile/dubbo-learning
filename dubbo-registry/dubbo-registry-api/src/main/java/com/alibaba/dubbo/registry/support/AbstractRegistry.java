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
package com.alibaba.dubbo.registry.support;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NamedThreadFactory;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;

/**
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 * 
 * @author chao.liuc
 * @author william.liangf
 */
public abstract class AbstractRegistry implements Registry {

    // 日志输出
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // URL地址分隔符，用于文件缓存中，服务提供者URL分隔
    private static final char URL_SEPARATOR = ' ';

    // URL地址分隔正则表达式，用于解析文件缓存中服务提供者URL列表
    private static final String URL_SPLIT = "\\s+";

    private URL registryUrl;

    // 本地磁盘缓存文件
    private File file;

    // 本地磁盘缓存，其中特殊的key值.registies记录注册中心列表，其它均为notified服务提供者列表
    private final Properties properties = new Properties();

    // 文件缓存定时写入
    private final ExecutorService registryCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));

    //是否是同步保存文件
    private final boolean syncSaveFile ;
    
    private final AtomicLong lastCacheChanged = new AtomicLong();
    //已经注册的服务列表，本质还是用来作为缓存
    private final Set<URL> registered = new ConcurrentHashSet<URL>();
    //服务订阅者-变更通知的映射，一个服务订阅者对应多个变更通知的监听器
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<URL, Set<NotifyListener>>();

    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<URL, Map<String, List<URL>>>();

    public AbstractRegistry(URL url) {
        setUrl(url);
        // 启动文件保存定时器
        syncSaveFile = url.getParameter(Constants.REGISTRY_FILESAVE_SYNC_KEY, false);
        String filename = url.getParameter(Constants.FILE_KEY, System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getHost() + ".cache");
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if(! file.exists() && file.getParentFile() != null && ! file.getParentFile().exists()){
                if(! file.getParentFile().mkdirs()){
                    throw new IllegalArgumentException("Invalid registry store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
        }
        this.file = file;//创建文件
        loadProperties();//将file的内容读入Properties列表
        notify(url.getBackupUrls()); //通知所有的备份URL
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public URL getUrl() {
        return registryUrl;
    }

    public Set<URL> getRegistered() {
        return registered;
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return subscribed;
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return notified;
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged(){
        return lastCacheChanged;
    }

    private class SaveProperties implements Runnable{
        private long version;
        private SaveProperties(long version){
            this.version = version;
        }
        public void run() {
            doSaveProperties(version);
        }
    }
    
    public void doSaveProperties(long version) {
        if(version < lastCacheChanged.get()){
            return;
        }
        if (file == null) {
            return;
        }
        Properties newProperties = new Properties();
        // 保存之前先读取一遍，防止多个注册中心之间冲突
        InputStream in = null;
        try {
            if (file.exists()) {
                in = new FileInputStream(file);
                //首先读入file文件
                newProperties.load(in);
            }
        } catch (Throwable e) {
            logger.warn("Failed to load registry store file, cause: " + e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        }     
        // 保存
        try {
            //然后读入properties文件
			newProperties.putAll(properties);
            File lockfile = new File(file.getAbsolutePath() + ".lock"); //创建一个锁文件
            if (!lockfile.exists()) {
            	lockfile.createNewFile();
            }
            RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
            try {
                FileChannel channel = raf.getChannel();
                try {//加锁是为了防止多个进程同时操作一个文件，但是每一次操作之前都是根据lockFile的锁情况来判断的
                    FileLock lock = channel.tryLock();
                	if (lock == null) {
                        throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                    }
                	// 保存
                    try {
                    	if (! file.exists()) {
                            file.createNewFile();
                        }
                        FileOutputStream outputFile = new FileOutputStream(file);  
                        try {
                            //将新保存的properties文件统一写入file文件
                            newProperties.store(outputFile, "Dubbo Registry Cache");
                        } finally {
                        	outputFile.close();
                        }
                    } finally {
                    	lock.release();
                    }
                } finally {
                    channel.close();
                }
            } finally {
                raf.close();
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {
                return;
            } else {
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry store file, cause: " + e.getMessage(), e);
        }
    }

    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry store file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }
    //将URL中的cache内容取出来
    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0 && key.equals(url.getServiceKey())
                    && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                    && value != null && value.length() > 0) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<URL>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }
    //这里是对RegistryService里面定义方法的实现，根据Consumer端提供的URL去查询自己订阅的URL
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<URL>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (! Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);//如果协议不为空的话就将该URL加入到result中去
                    }
                }
            }
        } else {//如果服务消费者没有任何订阅的话
            final AtomicReference<List<URL>> reference = new AtomicReference<List<URL>>();
            NotifyListener listener = new NotifyListener() {
                public void notify(List<URL> urls) {
                    reference.set(urls);
                }
            };
            subscribe(url, listener); // 订阅逻辑保证第一次notify后再返回
            List<URL> urls = reference.get();
            if (urls != null && urls.size() > 0) {
                for (URL u : urls) {
                    if (! Constants.EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Register: " + url);
        }
        registered.add(url);
    }

    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Unregister: " + url);
        }
        registered.remove(url);
    }
    //在URL已经对应的监听器集合中添加一个新的listener
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Subscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners == null) {
            subscribed.putIfAbsent(url, new ConcurrentHashSet<NotifyListener>());
            listeners = subscribed.get(url);
        }
        listeners.add(listener);
    }
    //将listener从订阅者对应的listener集合中移除
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()){
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }
    //估计是用来错误回复的方法，当发生问题的时候因为内部的缓存列表都还存在，所以就是读出缓存列表的内容再次进行注册和订阅操作
    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (! recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            //重新用来注册一下
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (! recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }

            //重新用来订阅一下
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(url, listener);
                }
            }
        }
    }
    //过滤空的协议？估计是后面需要的一个工具方法
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (urls == null || urls.size() == 0) {
            List<URL> result = new ArrayList<URL>(1);
            result.add(url.setProtocol(Constants.EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    protected void notify(List<URL> urls) {
        if(urls == null || urls.isEmpty()) return;
        
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();
            //判断(消费者URL)url和(提供者URL)urls.get(0)是否匹配
            if(! UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }
            
            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " +  urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }
    //通知所有匹配的providerUrl
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((urls == null || urls.size() == 0) 
                && ! Constants.ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }
        Map<String, List<URL>> result = new HashMap<String, List<URL>>();
        for (URL u : urls) {
            //如果(consumerUrl)url和(providerUrl)u匹配的话
            if (UrlUtils.isMatch(url, u)) {
                //取到providerUrl的category
            	String category = u.getParameter(Constants.CATEGORY_KEY, Constants.DEFAULT_CATEGORY);
            	List<URL> categoryList = result.get(category);
                //根据category区分不同的url
            	if (categoryList == null) {
            		categoryList = new ArrayList<URL>();
            		result.put(category, categoryList);
            	}
            	categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified = notified.get(url);
        if (categoryNotified == null) {
            notified.putIfAbsent(url, new ConcurrentHashMap<String, List<URL>>());
            categoryNotified = notified.get(url);
        }
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            saveProperties(url);
            listener.notify(categoryList);
        }
    }

    /**
     * 这么做的好处就是，即使zk挂了。本地还是有一份备份数据可以使用
     * @param url
     */
    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }
        
        try {
            StringBuilder buf = new StringBuilder();
            Map<String, List<URL>> categoryNotified = notified.get(url);
            //将所有对应的URL组合成字符串
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            //将一个接口对应的所有的category全部存入buf，然后写入properties文件
            properties.setProperty(url.getServiceKey(), buf.toString());
            long version = lastCacheChanged.incrementAndGet();
            //是否同步保存文件
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.execute(new SaveProperties(version));
            }//这个doSaveProperties方法的内容就是把刚才的properties文件内容再次写入到file缓存文件去
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    public void destroy() {
        if (logger.isInfoEnabled()){
            logger.info("Destroy registry:" + getUrl());
        }
        //取消所有的注册关系
        Set<URL> destroyRegistered = new HashSet<URL>(getRegistered());
        if (! destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<URL>(getRegistered())) {
                if (url.getParameter(Constants.DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        //取消所有的订阅关系
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (! destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " +t.getMessage(), t);
                    }
                }
            }
        }
    }

    public String toString() {
        return getUrl().toString();
    }

}