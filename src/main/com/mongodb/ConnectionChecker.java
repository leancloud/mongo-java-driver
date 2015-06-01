package com.mongodb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionChecker {
	
	class ConnectionLimitException extends RuntimeException {
		
		public ConnectionLimitException(String message) {
			super(message);
		}
	}
	
	private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getRootLogger();
	
	class ValueEqualAtomicInteger extends AtomicInteger {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean equals(Object v) {
			if (v instanceof ValueEqualAtomicInteger) {
				return ((ValueEqualAtomicInteger)v).get() == this.get();
			} 
			return false;
		}

		@Override
		public int hashCode() {
			return Integer.valueOf(this.get()).hashCode();
		}

		public ValueEqualAtomicInteger() {
			super();
		}

		public ValueEqualAtomicInteger(int initialValue) {
			super(initialValue);
		}
	}

	private int LIMIT = 20;
	
	public ConnectionChecker(int limit) {
		this.LIMIT = limit;
	}
	
	private ConcurrentHashMap<String, ValueEqualAtomicInteger> counterMap = new ConcurrentHashMap<String, ValueEqualAtomicInteger>(); 
	
	public void beforeGet(String appid) {
		ValueEqualAtomicInteger count = counterMap.putIfAbsent(appid, new ValueEqualAtomicInteger(1));
		if(count != null) {
			int now = count.incrementAndGet();
			if (now >= 10) {
				log.info("MONGO_CONN_LIMIT " + now + " " + appid);
			}
			if (now >= LIMIT) {
				throw new ConnectionLimitException("MONGO_CONN_LIMIT: " + LIMIT + " " + appid);
			}
		}
	}
	
	public void afterReturn(String appid) {
		ValueEqualAtomicInteger count = counterMap.get(appid);
		if(count != null) {
			if(count.decrementAndGet() <= 0) {
				counterMap.remove(appid, new ValueEqualAtomicInteger(0));
			}
		}
	}
	
	public static void main(String[] args) {
	}
}
