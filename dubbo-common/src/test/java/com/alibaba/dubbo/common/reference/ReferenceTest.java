package com.alibaba.dubbo.common.reference;

import com.alibaba.dubbo.common.serialize.ObjectInput;
import junit.framework.TestCase;

import java.lang.ref.Reference;

/**
 * Created by hzlvyanfeng on 2016/6/15.
 */
public class ReferenceTest extends TestCase {

    public void testReference(){
        Object obj= new Object();

        System.out.println(obj instanceof Reference);
    }
}
