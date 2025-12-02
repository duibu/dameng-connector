/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.devlive.connector.dameng.antlr;

import java.sql.Types;
import java.util.Arrays;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import io.debezium.antlr.AntlrDdlParser;
import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.DataTypeResolver;
import io.debezium.antlr.DataTypeResolver.DataTypeEntry;
import io.debezium.ddl.parser.oracle.generated.PlSqlLexer;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.SystemVariables;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.TableFilter;

import org.devlive.connector.dameng.DamengTypes;
import org.devlive.connector.dameng.DamengValueConverters;
import org.devlive.connector.dameng.antlr.listener.DamengDdlParserListener;

/**
 * This is the main Dameng Antlr DDL parser
 */
public class DamengDdlParser extends AntlrDdlParser<PlSqlLexer, PlSqlParser> {

    private final TableFilter tableFilter;
    private final DamengValueConverters converters;
    private final DataTypeResolver dataTypeResolver = initializeDataTypeResolver();

    private String catalogName;
    private String schemaName;

    public DamengDdlParser() {
        this(null, TableFilter.includeAll());
    }

    public DamengDdlParser(DamengValueConverters valueConverters) {
        this(true, valueConverters, TableFilter.includeAll());
    }

    public DamengDdlParser(DamengValueConverters valueConverters, TableFilter tableFilter) {
        this(true, valueConverters, tableFilter);
    }

    public DamengDdlParser(boolean throwErrorsFromTreeWalk, DamengValueConverters converters, TableFilter tableFilter) {
        this(throwErrorsFromTreeWalk, false, false, converters, tableFilter);
    }

    public DamengDdlParser(boolean throwErrorsFromTreeWalk, boolean includeViews, boolean includeComments,
                           DamengValueConverters converters, TableFilter tableFilter) {
        super(throwErrorsFromTreeWalk, includeViews, includeComments);
        this.converters = converters;
        this.tableFilter = tableFilter;
    }

    @Override
    public void parse(String ddlContent, Tables databaseTables) {
        String strippedDdl = ddlContent.strip();
        if (!strippedDdl.endsWith(";")) {
            strippedDdl = strippedDdl + ";";
        }
        super.parse(strippedDdl, databaseTables);
    }

    @Override
    public ParseTree parseTree(PlSqlParser parser) {
        return parser.sql_script();
    }

    @Override
    protected AntlrDdlParserListener createParseTreeWalkerListener() {
        return new DamengDdlParserListener(catalogName, schemaName, this);
    }

    @Override
    protected PlSqlLexer createNewLexerInstance(CharStream charStreams) {
        return new PlSqlLexer(charStreams);
    }

    @Override
    protected PlSqlParser createNewParserInstance(CommonTokenStream commonTokenStream) {
        return new PlSqlParser(commonTokenStream);
    }

    @Override
    protected boolean isGrammarInUpperCase() {
        return true;
    }

    @Override
    public DataTypeResolver dataTypeResolver() {
        return dataTypeResolver;
    }

    private DataTypeResolver initializeDataTypeResolver() {
        // todo, register all and use in ColumnDefinitionParserListener
        DataTypeResolver.Builder dataTypeResolverBuilder = new DataTypeResolver.Builder();

        dataTypeResolverBuilder.registerDataTypes(
                PlSqlParser.Native_datatype_elementContext.class.getCanonicalName(), Arrays.asList(
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.INT),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.INTEGER),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.SMALLINT),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.NUMERIC),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.DECIMAL),
                        new DataTypeEntry(Types.NUMERIC, PlSqlParser.NUMBER),

                        new DataTypeEntry(Types.TIMESTAMP, PlSqlParser.DATE),
                        new DataTypeEntry(DamengTypes.TIMESTAMPLTZ, PlSqlParser.TIMESTAMP),
                        new DataTypeEntry(DamengTypes.TIMESTAMPTZ, PlSqlParser.TIMESTAMP),
                        new DataTypeEntry(Types.TIMESTAMP, PlSqlParser.TIMESTAMP),

                        new DataTypeEntry(Types.VARCHAR, PlSqlParser.VARCHAR2),
                        new DataTypeEntry(Types.VARCHAR, PlSqlParser.VARCHAR),
                        new DataTypeEntry(Types.NVARCHAR, PlSqlParser.NVARCHAR2),
                        new DataTypeEntry(Types.CHAR, PlSqlParser.CHAR),
                        new DataTypeEntry(Types.NCHAR, PlSqlParser.NCHAR),

                        new DataTypeEntry(DamengTypes.BINARY_FLOAT, PlSqlParser.BINARY_FLOAT),
                        new DataTypeEntry(DamengTypes.BINARY_DOUBLE, PlSqlParser.BINARY_DOUBLE),
                        new DataTypeEntry(Types.FLOAT, PlSqlParser.FLOAT),
                        new DataTypeEntry(Types.FLOAT, PlSqlParser.REAL),
                        new DataTypeEntry(Types.BLOB, PlSqlParser.BLOB),
                        new DataTypeEntry(Types.CLOB, PlSqlParser.CLOB)));
        return dataTypeResolverBuilder.build();
    }

    @Override
    protected SystemVariables createNewSystemVariablesInstance() {
        // todo implement
        return null;
    }

    @Override
    public void setCurrentDatabase(String databaseName) {
        this.catalogName = databaseName;
    }

    @Override
    public void setCurrentSchema(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public SystemVariables systemVariables() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Runs a function if all given object are not null.
     *
     * @param function function to run; may not be null
     * @param nullableObjects object to be tested, if they are null.
     */
    public void runIfNotNull(Runnable function, Object... nullableObjects) {
        for (Object nullableObject : nullableObjects) {
            if (nullableObject == null) {
                return;
            }
        }
        function.run();
    }

    public DamengValueConverters getConverters() {
        return converters;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }
}
