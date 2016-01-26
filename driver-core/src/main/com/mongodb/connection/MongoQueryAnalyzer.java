package com.mongodb.connection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BsonDocument;
import org.bson.BsonRegularExpression;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoQueryAnalyzer {
	
	public static class ConnectionLimitException extends RuntimeException {
		
		private static final long serialVersionUID = 1L;

		public ConnectionLimitException(String message) {
			super(message);
		}
	}
	
	static class ValueEqualAtomicInteger extends AtomicInteger {

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
	
	private static ConcurrentHashMap<String, ValueEqualAtomicInteger> counterMap = new ConcurrentHashMap<String, ValueEqualAtomicInteger>(); 
	
	public static void beforeGet(String appid) {
		ValueEqualAtomicInteger count = counterMap.putIfAbsent(appid, new ValueEqualAtomicInteger(1));
		if(count != null) {
			int now = count.incrementAndGet();
			if (now >= connLimit && !appid.equals("avoscloud_quartz")) {
				throw new ConnectionLimitException("MONGO_CONN_LIMIT: " + now + " " + appid);
			}
		}
	}
	
	public static void afterReturn(String appid) {
		ValueEqualAtomicInteger count = counterMap.get(appid);
		if(count != null) {
			if(count.decrementAndGet() <= 0) {
				counterMap.remove(appid, new ValueEqualAtomicInteger(0));
			}
		}
	}

	public static void main(String[] args) {
		
	}
	
	private static Logger logger = LoggerFactory.getLogger("com.mongodb.connection.MongoQueryAnalyzer.logger");
	private static Logger queryLogger = LoggerFactory.getLogger("com.mongodb.connection.MongoQueryAnalyzer.queryLogger");
	
	private static long queryThreshold = 500;
	private static int connLimit = 30;

	static {
		
		String threshold = System.getenv("MONGO_SLOW_QUERY_THRESHOLD");
		if (threshold != null) {
			queryThreshold = Long.parseLong(threshold);
		}
		String connectionLimit = System.getenv("MONGO_QUERY_CONN_LIMIT");
		if (connectionLimit != null) {
			connLimit = Integer.parseInt(connectionLimit);
		}

		new Thread(new RecordQueryCountThread()).start();
	}

	private static class QueryRecord {

		public AtomicInteger count = new AtomicInteger();
		public AtomicLong time = new AtomicLong();
	}

	private static class RecordQueryCountThread implements Runnable {

		@Override
		public void run() {

			while (true) {

				try {
					Thread.sleep(1000);
					ConcurrentHashMap<String, QueryRecord> map = queryRecordMap;
					queryRecordMap = new ConcurrentHashMap<String, QueryRecord>();
					for (Entry<String, QueryRecord> entry : map.entrySet()) {
						queryLogger.info(entry.getKey() + " " + entry.getValue().time + " " + entry.getValue().count + " N");
					}
				} catch (Exception ex) {
					logger.error("LOG SLOW QUERY ERROR", ex);
				}
			}
		}
	}

	private static void recordQuery(String query, long time) {
		QueryRecord record = new QueryRecord();
		QueryRecord oldRecord = queryRecordMap.putIfAbsent(query, record);
		if (oldRecord != null)
			record = oldRecord;
		record.count.incrementAndGet();
		record.time.addAndGet(time);
	}

	private static volatile ConcurrentHashMap<String, QueryRecord> queryRecordMap = new ConcurrentHashMap<String, QueryRecord>();
	
	public static String joinString(final Object[] array, final char separator) {
		int startIndex = 0;
		int endIndex = array.length;
		final int noOfItems = endIndex - startIndex;
		if (noOfItems <= 0) {
			return "";
		}
		final StringBuilder buf = new StringBuilder(noOfItems * 16);
		for (int i = startIndex; i < endIndex; i++) {
			if (i > startIndex) {
				buf.append(separator);
			}
			if (array[i] != null) {
				buf.append(array[i]);
			}
		}
		return buf.toString();
	}
	
	private static Set<String> extractOrQueries(BsonDocument query) {
		Set<String> fields = new HashSet<String>();
		for(Entry<String, BsonValue> e :query.entrySet()) {
			String key = e.getKey();
			BsonValue value = e.getValue();
			String f = key.toString();
			if (f.equals("$and")) {
    			if (value instanceof List) {
    				Set<String> set = new HashSet<String> ();
    				for (Object obj : (List) value) {
    					if (obj instanceof BsonDocument) {
    						set.addAll(extractOrQueries((BsonDocument) obj));
    					}
    				}
    				fields.addAll(set);
    			}
			} else if (f.equals("$or")) {
				if (value instanceof List) {
					Set<String> set = new HashSet<String>();
    				for (Object obj : (List) value) {
    					if (obj instanceof BsonDocument) {
    						set.add(parseQueryString((BsonDocument) obj));
    					}
    				}
    				fields.addAll(set);
				}
			}
		}
		return fields;
	}
	
	private static String parseQueryString(BsonDocument query) {
    	
    	Set<String> fields = new HashSet<String>();
    	
    	for(Entry<String, BsonValue> e : query.entrySet()) {
    		
    		String key = e.getKey();
    		BsonValue value = e.getValue();
    		String f = key.toString();
    		
    		if (f.equals("$and")) {
    			if (value instanceof List) {
    				Set<String> set = new HashSet<String> ();
    				for (Object obj : (List) value) {
    					if (obj instanceof BsonDocument) {
    						set.add(parseQueryString((BsonDocument) obj));
    					}
    				}
    				fields.add(joinString(set.toArray(), ','));
    			}
    			
    		} else if (f.startsWith("$")) {
    			
    		} else {    			
        		if(value instanceof BsonDocument) {
        			BsonDocument v = (BsonDocument) value;
        			if(v.containsKey("$gt")) {
        				f += ">";
        			} else if(v.containsKey("$lt")) {
        				f += "<";
        			} else if(v.containsKey("$ne")) {
        				f += "<>";
        			} else if(v.containsKey("$gte")) {
        				f += ">=";
        			} else if(v.containsKey("$lte")) {
        				f += "<=";
        			} else if(v.containsKey("$in")) {
        				f += "<in>";
        			} else if(v.containsKey("$regex")) {
        				f += "<regex>";
        			}
        		} else if ( value instanceof BsonRegularExpression) {
        			f += "<regex>";
        		}
        		fields.add(f);
    		}
    	}
    	
    	return joinString(fields.toArray(), ',');    	
    }
	
	public static void innerLog (String cmdType, String namespace, BsonDocument query, long time) {
		
		BsonDocument q = query;
		
		if (query.containsKey("$query")) {
			q = (BsonDocument) query.get("$query");
		} else if (query.containsKey("query")) {
			q = (BsonDocument) query.get("query");
		}

		String orderby = null;

		if (query.containsKey("$orderby")) {
			BsonDocument o = (BsonDocument) query.get("$orderby");
			ArrayList<String> orders = new ArrayList<String>();
			for (String k : o.keySet()) {
				BsonValue v = o.get(k);
				if (v.isNumber()) {
					orders.add(k + ":" + v.asNumber().longValue());
				}
			}
			orderby = joinString(orders.toArray(new String[0]), ',');
		}

		if (q.containsKey("_id") && time < queryThreshold) {
			return;
	    }

		String[] dbCollection = namespace.split("\\.");

		if (dbCollection.length != 2) {
			return;
		}
		
		if ("$cmd".equals(dbCollection[1])) {
			if (query.containsKey("count")) {
				namespace = dbCollection[0] + "." + query.get("count");
				cmdType = "count";
			} else {
				return;
			}
		}

		String queryString = "";

		try {
			queryString = parseQueryString(q);
		} catch (Exception ex) {
			logger.error(ex.getMessage());
		}

		if (orderby != null) {
			queryString += "|" + orderby;
		}
		
		Set<String> orQueries = extractOrQueries(q); 
		
		if(logger.isTraceEnabled()) {
			logger.trace(query.toJson());
			logger.trace(cmdType + " " + namespace + " " + queryString + " " + time);
			logger.trace(orQueries.toString());
		}

		if (time < queryThreshold) {
			recordQuery(cmdType + " " + namespace + " " + queryString, time);
			for(String orQ : orQueries) {
				recordQuery(cmdType + " " + namespace + " " + orQ, time);;
			}
		} else {
			queryLogger.info(cmdType + " " + namespace + " " + queryString + " " + time + " 1 S");
			for(String orQ : orQueries) {
				queryLogger.info(cmdType + " " + namespace + " " + orQ + " " + time + " 1 S");
			}
			logger.info("SLOW_QUERY " + time + " " + namespace + " " + query);
		}
	}
	
	public static void logQuery(String cmdType, String namespace, BsonDocument query, long time) {		
		try {
			innerLog(cmdType, namespace, query, time);
		} catch (Exception ex) {
			logger.error("Query Log Error:", ex);
		}
	}
}
