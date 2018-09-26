package com.spsoft.common.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>名称：IdWorker.java</p>Ø
 * <p>描述：分布式自增长ID，其中，加入了IP段信息</p>
 * <pre>
 *     Twitter的 Snowflake　JAVA实现方案
 * </pre>
 * 核心代码为其IdWorker这个类实现，其原理结构如下，我分别用一个0表示一位，用—分割开部分的作用：
 * 1||0---0000000000 0000000000 0000000000 0000000000 0 --- 00000 ---00000 ---000000000000
 * 在上面的字符串中，第一位为未使用（实际上也可作为long的符号位），接下来的41位为毫秒级时间，
 * 然后5位datacenter标识位，5位机器ID（并不算标识符，实际是为线程标识），
 * 然后12位该毫秒内的当前毫秒内的计数，加起来刚好64位，为一个Long型。
 * 这样的好处是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞（由datacenter和机器ID作区分），
 * 并且效率较高，经测试，snowflake每秒能够产生26万ID左右，完全满足需要。
 * <p>
 * 
 * 64位ID (42(毫秒)+5(机器ID)+5(业务编码)+12(重复累加))
 *
 * @author Polim
 */

public class IdWorker {
	
	
	private static Map<String,Object> map = new HashMap<>();
	
	private IdWorker(){
        //InetAddress.
		this.workerId = getWorkerId(getIP());
		this.datacenterId = 0;
	}

    private IdWorker(int partition){
        //InetAddress.
        this.workerId = partition;//getWorkerId(getIP());
        this.datacenterId = 0;
    }

    public static String getIP(){
        try {
            for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces(); en.hasMoreElements();) {
                NetworkInterface intf = en.nextElement();
                for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses(); enumIpAddr.hasMoreElements();)
                {
                    InetAddress inetAddress = enumIpAddr.nextElement();
                    if (!inetAddress.isLoopbackAddress() && (inetAddress instanceof Inet4Address))
                    {
                        String ip = inetAddress.getHostAddress();
                        if (!"127.0.0.1".equals(ip)) {
                            return ip;
                        }
                        //return inetAddress.getHostAddress().toString();
                    }
                }
            }
        }
        catch (SocketException ex){
            ex.printStackTrace();
        }
        return null;
    }
	
	  
	// 时间起始标记点，作为基准，一般取系统的最近时间（一旦确定不能变动）
    private final static long twepoch = 1480166465631L;//DateUtils.valueOf("2017-01-01", "yyyy-MM-dd").getTime(); //1480166465631L
    // 机器标识位数
    private final static long workerIdBits = 16L;
    // 数据中心标识位数
    private final static long datacenterIdBits = 2L;//3
    // 机器ID最大值
    private final static long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 数据中心ID最大值
    private final static long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
    // 毫秒内自增位
    private final static long sequenceBits = 7L;//127
    // 机器ID偏左移12位
    private final static long workerIdShift = sequenceBits;
    // 数据中心ID左移17位
    private final static long datacenterIdShift = sequenceBits + workerIdBits;
    // 时间毫秒左移22位
    private final static long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;

    private final static long sequenceMask = -1L ^ (-1L << sequenceBits);
    /* 上次生产id时间戳 */
    private static long lastTimestamp = -1L;
    // 0，并发控制
    private long sequence = 0L;

    private long workerId = 0;
    // 数据标识id部分
    private long datacenterId = 0;
    
    /**
     * @param workerId
     *            工作机器ID
     * @param datacenterId
     *            序列号
     */
   /* public IdWorker(long workerId, long datacenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }
    */

	private static ThreadLocal<Long> currId = new ThreadLocal<>();
    
    
    /**
     * 获取下一个ID并放入线程变量
     *
     * @return
     */
    public synchronized long nextIdThreadLocal() {
        
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        if (lastTimestamp == timestamp) {
            // 当前毫秒内，则+1
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        // ID偏移组合生成最终的ID，并返回ID
        long nextId = ((timestamp - twepoch) << timestampLeftShift)
                | (datacenterId << datacenterIdShift)
                | (workerId << workerIdShift) | sequence;
        currId.set(nextId);
        return nextId;
    }

    /**
     * 获取下一个ID并放入线程变量
     *
     * @return
     */
    public synchronized long nextId() {
        
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
        	
       	 //throw new SpsoftException("1",String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp),SpsoftExceptionLevel.SEVERITY);
      throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        if (lastTimestamp == timestamp) {
            // 当前毫秒内，则+1
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        // ID偏移组合生成最终的ID，并返回ID
        long nextId = ((timestamp - twepoch) << timestampLeftShift)
                | (datacenterId << datacenterIdShift)
                | (workerId << workerIdShift) | sequence;
        return nextId;
    }

    /**
     * 获取下一个ID并放入线程变量，workId为分区
     *
     * @return
     */
    public synchronized long nextId(int partition) {

        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {

            //throw new SpsoftException("1",String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp),SpsoftExceptionLevel.SEVERITY);
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        if (lastTimestamp == timestamp) {
            // 当前毫秒内，则+1
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        // ID偏移组合生成最终的ID，并返回ID
        long nextId = ((timestamp - twepoch) << timestampLeftShift)
                | (datacenterId << datacenterIdShift)
                | (partition << workerIdShift) | sequence;
        return nextId;
    }
    
    
    /**
     * 获取id后销毁，注意自己销毁
     * @return
     */
    public static long getCurrIdAndRemove() {
    	Long id= currId.get();
    	currId.remove();
    	return id;
    }

    
    /**
     * 获取id后不会销毁，注意自己销毁
     * @return
     */
    public static long getCurrId() {
    	return currId.get();
    }
    
    
    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }


    /**
     * <p>
     * 数据标识id部分
     * </p>
     */
    private long getWorkerId(String ipAddress) {
    	String[] ips = ipAddress.split("\\.");
        return  Integer.parseInt(ips[2])<<8 | Integer.parseInt(ips[3]);
    }
    
    /**
     * 区域id  范围为0~2
     * @param env
     * @return

    private long getDatacenterId(Environment env) {
    	//String regionId = env.getProperty("fw.regionId");
    	//return Long.parseLong(regionId==null || !regionId.matches("^[0-2]{1}$")?"0":regionId);
        return 0;
	}*/

    /**
     * 获取ID生成实例
     * @param c
     * @return
     */
    public static synchronized IdWorker getInstance(String c) {
    	 IdWorker idwork;
    	 String ct;
    	 ct = StringUtils.isBlank(c)? c : c.toUpperCase();
    	 if (map.get(ct) == null) {  
    		 idwork = new IdWorker(); 
    		 map.put(ct, idwork);
    	 }else{
    		 idwork = (IdWorker) map.get(ct);
    	 }
    	 return idwork; 
    }

    public static synchronized IdWorker getInstance(String c, int partition) {
        IdWorker idwork;
        String ct = StringUtils.isBlank(c)? c : c.toUpperCase().concat("_"+partition);
        if (map.get(ct) == null) {
            idwork = new IdWorker(partition);
            map.put(ct, idwork);
        }else{
            idwork = (IdWorker) map.get(ct);
        }
        return idwork;
    }




}