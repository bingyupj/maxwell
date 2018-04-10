package com.zendesk.maxwell.schema.ddl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.replication.TableCache;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ResolvedTableAlter extends ResolvedSchemaChange {

	static Gson gson = new Gson();
	static AtomicInteger i= new AtomicInteger(0);
	static final Logger LOGGER = LoggerFactory.getLogger(ResolvedTableAlter.class);

	public String database;
	public String table;

	@JsonProperty("old")
	public Table oldTable;

	@JsonProperty("def")
	public Table newTable;

	public ResolvedTableAlter() { }
	public ResolvedTableAlter(String database, String table, Table oldTable, Table newTable) {
		this();
		this.database = database;
		this.table = table;
		this.oldTable = oldTable;
		this.newTable = newTable;
	}

	@Override
	public void apply(Schema schema) throws InvalidSchemaError {
		//LOGGER.info("ResolvedTableAlter apply=" + i.getAndIncrement());
		Database oldDatabase = schema.findDatabaseOrThrow(this.database);
		Table table = oldDatabase.findTableOrThrow(this.table);

		Database newDatabase;
		if ( this.database.equals(newTable.database) )
			newDatabase = oldDatabase;
		else
			newDatabase = schema.findDatabaseOrThrow(newTable.database);

		oldDatabase.removeTable(this.table);
		newDatabase.addTable(newTable);
	}

	@Override
	public String databaseName() {
		return database;
	}

	@Override
	public String tableName() {
		return table;
	}

	@Override
	public boolean shouldOutput(MaxwellFilter filter) {
		boolean flag = MaxwellFilter.matches(filter, database, oldTable.getName()) &&
				MaxwellFilter.matches(filter, database, newTable.getName());
		//LOGGER.info("ResolvedTableAlter flag=" + flag +" i=" + i.getAndIncrement() +" filter=" + gson.toJson(filter));
		return flag;
	}
}
