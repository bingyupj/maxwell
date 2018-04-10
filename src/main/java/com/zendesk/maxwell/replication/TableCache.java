package com.zendesk.maxwell.replication;

import java.util.HashMap;

import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableCache {
	static Gson gson = new Gson();
	static final Logger LOGGER = LoggerFactory.getLogger(TableCache.class);
	private final HashMap<Long, Table> tableMapCache = new HashMap<>();
	private final HashMap<Long, String> blacklistedTableCache = new HashMap<>();

	public void processEvent(Schema schema, MaxwellFilter filter, Long tableId, String dbName, String tblName) {

		boolean isMatch = dbName!=null && filter.matches(dbName,tblName);
		LOGGER.debug("is match=" + isMatch +" tableId="+tableId+" dbname="+dbName+" tblName="+tblName  + " filter=" + gson.toJson(filter));
		if (!isMatch){
			return;
		}

		/*if (dbName!=null && filter.matches(dbName,tblName)){
			return;
		}*/
		/*if (!tblName.contains("auto_vending_shop") && !tblName.startsWith("maxwell")){
			return;
		}*/
		if ( !tableMapCache.containsKey(tableId) ) {
			if ( filter != null && filter.isTableBlacklisted(dbName, tblName) ) {
				blacklistedTableCache.put(tableId, tblName);
				return;
			}

			Database db = schema.findDatabase(dbName);
			if ( db == null )

				throw new RuntimeException("Couldn't find database " + dbName);
			else {
				Table tbl = db.findTable(tblName);

				if (tbl == null)
					throw new RuntimeException("Couldn't find table " + tblName + " in database " + dbName);
				else
					tableMapCache.put(tableId, tbl);
			}
		}

	}

	// open-replicator keeps a very similar cache, but we can't get access to it.
	public void processEvent(Schema schema, MaxwellFilter filter, TableMapEvent event) {
		String dbName = new String(event.getDatabaseName().getValue());
		String tblName = new String(event.getTableName().getValue());

		processEvent(schema, filter, event.getTableId(), dbName, tblName);
	}

	public Table getTable(Long tableId) {
		return tableMapCache.get(tableId);
	}

	public boolean isTableBlacklisted(Long tableId) {
		return blacklistedTableCache.containsKey(tableId);
	}

	public String getBlacklistedTableName(Long tableId) {
		return blacklistedTableCache.get(tableId);
	}

	public void clear() {
		tableMapCache.clear();
		blacklistedTableCache.clear();
	}
}
