package com.landoop.jdbc

import com.landoop.jdbc.domain.GenericJdbcData
import org.apache.avro.SchemaBuilder
import java.sql.*
import java.util.*

class LsqlJdbcDatabaseMetaData : DatabaseMetaData {
  protected val TABLE_TYPES = arrayListOf<String>("TABLE", "SYSTEM TABLE")
  private var connection: LsqlJdbcConnection

  constructor(connection: LsqlJdbcConnection) {
    this.connection = connection
  }

  @Throws(SQLException::class)
  override fun allProceduresAreCallable(): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun allTablesAreSelectable(): Boolean {
    return true
  }

  @Throws(SQLException::class)
  override fun getURL(): String {
    return "${Constants.JdbcPrefix}/default"
  }

  @Throws(SQLException::class)
  override fun getUserName(): String {
    return connection.userName
  }

  @Throws(SQLException::class)
  override fun isReadOnly(): Boolean = false

  @Throws(SQLException::class)
  override fun nullsAreSortedHigh(): Boolean = false

  @Throws(SQLException::class)
  override fun nullsAreSortedLow(): Boolean = false

  @Throws(SQLException::class)
  override fun nullsAreSortedAtStart(): Boolean = false

  @Throws(SQLException::class)
  override fun nullsAreSortedAtEnd(): Boolean = false

  @Throws(SQLException::class)
  override fun getDatabaseProductName(): String {
    return "LSQL for Apache Kafka"
  }

  @Throws(SQLException::class)
  override fun getDatabaseProductVersion(): String = Constants.getVersion()

  @Throws(SQLException::class)
  override fun getDriverName(): String = "LSQL JDBC Driver"


  @Throws(SQLException::class)
  override fun getDriverVersion(): String = LsqlJdbcDriver.getVersion()

  override fun getDriverMajorVersion(): Int = Constants.getVersionMajor()

  override fun getDriverMinorVersion(): Int = Constants.getVersionMinor()

  @Throws(SQLException::class)
  override fun usesLocalFiles(): Boolean = false

  @Throws(SQLException::class)
  override fun usesLocalFilePerTable(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsMixedCaseIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesUpperCaseIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesLowerCaseIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesMixedCaseIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsMixedCaseQuotedIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesUpperCaseQuotedIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesLowerCaseQuotedIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun storesMixedCaseQuotedIdentifiers(): Boolean = false

  @Throws(SQLException::class)
  override fun getIdentifierQuoteString(): String = "`"

  @Throws(SQLException::class)
  override fun getSQLKeywords(): String {
    return """
      AVRO, JSON, STRING, _ktype,_vtype, _key, _partition, _offset, _topic,_ts, _value
    """.trimIndent()
  }

  @Throws(SQLException::class)
  override fun getNumericFunctions(): String? = null

  @Throws(SQLException::class)
  override fun getStringFunctions(): String = ""

  @Throws(SQLException::class)
  override fun getSystemFunctions(): String = ""

  @Throws(SQLException::class)
  override fun getTimeDateFunctions(): String = ""

  @Throws(SQLException::class)
  override fun getSearchStringEscape(): String? = null

  @Throws(SQLException::class)
  override fun getExtraNameCharacters(): String? = null

  @Throws(SQLException::class)
  override fun supportsAlterTableWithAddColumn(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsAlterTableWithDropColumn(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsColumnAliasing(): Boolean = true

  @Throws(SQLException::class)
  override fun nullPlusNonNullIsNull(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsConvert(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsConvert(fromType: Int, toType: Int): Boolean = false

  @Throws(SQLException::class)
  override fun supportsTableCorrelationNames(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsDifferentTableCorrelationNames(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsExpressionsInOrderBy(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOrderByUnrelated(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsGroupBy(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsGroupByUnrelated(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsGroupByBeyondSelect(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsLikeEscapeClause(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsMultipleResultSets(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsMultipleTransactions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsNonNullableColumns(): Boolean = true

  @Throws(SQLException::class)
  override fun supportsMinimumSQLGrammar(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCoreSQLGrammar(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsExtendedSQLGrammar(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsANSI92EntryLevelSQL(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsANSI92IntermediateSQL(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsANSI92FullSQL(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsIntegrityEnhancementFacility(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOuterJoins(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsFullOuterJoins(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsLimitedOuterJoins(): Boolean = false

  @Throws(SQLException::class)
  override fun getSchemaTerm(): String? = null

  @Throws(SQLException::class)
  override fun getProcedureTerm(): String {
    return "Function"
  }

  @Throws(SQLException::class)
  override fun getCatalogTerm(): String? = null

  @Throws(SQLException::class)
  override fun isCatalogAtStart(): Boolean = false

  @Throws(SQLException::class)
  override fun getCatalogSeparator(): String? = null

  @Throws(SQLException::class)
  override fun supportsSchemasInDataManipulation(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSchemasInProcedureCalls(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSchemasInTableDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSchemasInIndexDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCatalogsInDataManipulation(): Boolean = false


  @Throws(SQLException::class)
  override fun supportsCatalogsInProcedureCalls(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCatalogsInTableDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCatalogsInIndexDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsPositionedDelete(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsPositionedUpdate(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSelectForUpdate(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsStoredProcedures(): Boolean = true

  @Throws(SQLException::class)
  override fun supportsSubqueriesInComparisons(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSubqueriesInExists(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsSubqueriesInIns(): Boolean = true

  @Throws(SQLException::class)
  override fun supportsSubqueriesInQuantifieds(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsCorrelatedSubqueries(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsUnion(): Boolean = true

  @Throws(SQLException::class)
  override fun supportsUnionAll(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOpenCursorsAcrossCommit(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOpenCursorsAcrossRollback(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOpenStatementsAcrossCommit(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsOpenStatementsAcrossRollback(): Boolean = false

  @Throws(SQLException::class)
  override fun getMaxBinaryLiteralLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxCharLiteralLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnsInGroupBy(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnsInIndex(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnsInOrderBy(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnsInSelect(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxColumnsInTable(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxConnections(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxCursorNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxIndexLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxSchemaNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxProcedureNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxCatalogNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxRowSize(): Int = 0

  @Throws(SQLException::class)
  override fun doesMaxRowSizeIncludeBlobs(): Boolean = false


  @Throws(SQLException::class)
  override fun getMaxStatementLength(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxStatements(): Int = 0

  @Throws(SQLException::class)
  override fun getMaxTableNameLength(): Int = 1024

  @Throws(SQLException::class)
  override fun getMaxTablesInSelect(): Int = 1

  @Throws(SQLException::class)
  override fun getMaxUserNameLength(): Int = 0

  @Throws(SQLException::class)
  override fun getDefaultTransactionIsolation(): Int = java.sql.Connection.TRANSACTION_NONE

  @Throws(SQLException::class)
  override fun supportsTransactions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsTransactionIsolationLevel(level: Int): Boolean = false

  @Throws(SQLException::class)
  override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsDataManipulationTransactionsOnly(): Boolean = false

  @Throws(SQLException::class)
  override fun dataDefinitionCausesTransactionCommit(): Boolean = false

  @Throws(SQLException::class)
  override fun dataDefinitionIgnoredInTransactions(): Boolean = true

  @Throws(SQLException::class)
  override fun getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet {
    val schema = SchemaBuilder.builder().record("procedures")
        .fields()
        .nullableString("PROCEDURE_CAT", null)
        .nullableString("PROCEDURE_SCHEM", null)
        .nullableString("PROCEDURE_NAME", null)
        .nullableString("REMARKS", null)
        .nullableInt("PROCEDURE_TYPE", DatabaseMetaData.procedureResultUnknown)
        .nullableString("REMARKS", null)
        .endRecord()

    val data = GenericJdbcData(emptyArray(), schema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getProcedureColumns(catalog: String,
                                   schemaPattern: String,
                                   procedureNamePattern: String,
                                   columnNamePattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getTables(catalog: String, schemaPattern: String, tableNamePattern: String?, types: Array<String>?): ResultSet {
    database.activateOnCurrentThread()
    val classes = database.getMetadata().getSchema().getClasses()

    val resultSet = OInternalResultSet()

    val tableTypes = if (types != null) Arrays.asList(*types) else TABLE_TYPES
    for (cls in classes) {
      val className = cls.getName()
      val type: String

      if (OMetadataInternal.SYSTEM_CLUSTER.contains(cls.getName().toLowerCase(Locale.ENGLISH)))
        type = "SYSTEM TABLE"
      else
        type = "TABLE"

      if (tableTypes.contains(type) && (tableNamePattern == null || tableNamePattern == "%" || tableNamePattern
          .equals(className, ignoreCase = true))) {

        val doc = OResultInternal()

        doc.setProperty("TABLE_CAT", database.getName())
        doc.setProperty("TABLE_SCHEM", database.getName())
        doc.setProperty("TABLE_NAME", className)
        doc.setProperty("TABLE_TYPE", type)
        doc.setProperty("REMARKS", null as Any?)
        doc.setProperty("TYPE_NAME", null as Any?)
        doc.setProperty("REF_GENERATION", null as Any?)
        resultSet.add(doc)
      }
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getSchemas(): ResultSet {
    val schema = SchemaBuilder.builder().record("procedures")
        .fields()
        .nullableString("TABLE_SCHEM", null)
        .nullableString("TABLE_CATALOG", null)
        .endRecord()

    val data = GenericJdbcData(arrayOf(arrayOf<Any>(Constants.DatabaseName, Constants.DatabaseName)), schema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getCatalogs(): ResultSet {
    val schema = SchemaBuilder.builder().record("catalogs")
        .fields()
        .nullableString("TABLE_CAT", null)
        .endRecord()

    val data = GenericJdbcData(arrayOf(arrayOf<Any>(Constants.DatabaseName)), schema, "")

    return LsqlJdbcResultSet(null, data)
  }


  @Throws(SQLException::class)
  override fun getTableTypes(): ResultSet {
    val schema = SchemaBuilder.builder().record("tabletypes")
        .fields()
        .nullableString("TABLE_TYPE", null)
        .endRecord()

    val data = GenericJdbcData(TABLE_TYPES.map { it -> arrayOf<Any>(it) }.toTypedArray(), schema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getColumns(catalog: String,
                          schemaPattern: String,
                          tableNamePattern: String,
                          columnNamePattern: String?): ResultSet {
    database.activateOnCurrentThread()

    val resultSet = OInternalResultSet()

    val schema = database.getMetadata().getSchema()

    for (clazz in schema.getClasses()) {
      if (OrientJdbcUtils.like(clazz.getName(), tableNamePattern)) {
        for (prop in clazz.properties()) {
          if (columnNamePattern == null) {
            resultSet.add(getPropertyAsDocument(clazz, prop))
          } else {
            if (OrientJdbcUtils.like(prop.getName(), columnNamePattern)) {
              resultSet.add(getPropertyAsDocument(clazz, prop))
            }
          }
        }

      }
    }
    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getColumnPrivileges(catalog: String,
                                   schema: String,
                                   table: String,
                                   columnNamePattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getTablePrivileges(catalog: String,
                                  schemaPattern: String,
                                  tableNamePattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getBestRowIdentifier(catalog: String,
                                    schema: String,
                                    table: String,
                                    scope: Int,
                                    nullable: Boolean): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getVersionColumns(catalog: String,
                                 schema: String,
                                 table: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet {
    val avroSchema = SchemaBuilder.builder().record("primarykeys")
        .fields()
        .nullableString("TABLE_CAT", null)
        .nullableString("TABLE_SCHEM", null)
        .nullableString("TABLE_NAME", null)
        .nullableInt("KEY_SEQ", 0)
        .nullableString("PK_NAME", null)
        .endRecord()

    val data = GenericJdbcData(emptyArray(), avroSchema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getImportedKeys(catalog: String, schema: String, table: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  private fun getEmptyResultSet(): ResultSet {
    val schema = SchemaBuilder.builder().record("empty").fields().endRecord()

    val data = GenericJdbcData(emptyArray(), schema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getExportedKeys(catalog: String, schema: String, table: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getCrossReference(parentCatalog: String,
                                 parentSchema: String,
                                 parentTable: String,
                                 foreignCatalog: String,
                                 foreignSchema: String,
                                 foreignTable: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getTypeInfo(): ResultSet {

    /*var res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.BINARY.toString())
    res.setProperty("DATA_TYPE", Types.BINARY)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)*/


    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getIndexInfo(catalog: String,
                            schema: String,
                            table: String,
                            unique: Boolean,
                            approximate: Boolean): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun supportsResultSetType(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean = false

  @Throws(SQLException::class)
  override fun ownUpdatesAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun ownDeletesAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun ownInsertsAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun othersUpdatesAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun othersDeletesAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun othersInsertsAreVisible(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun updatesAreDetected(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun deletesAreDetected(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun insertsAreDetected(type: Int): Boolean = false

  @Throws(SQLException::class)
  override fun supportsBatchUpdates(): Boolean = false

  @Throws(SQLException::class)
  override fun getUDTs(catalog: String,
                       schemaPattern: String,
                       typeNamePattern: String,
                       types: IntArray): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getConnection(): Connection = connection

  @Throws(SQLException::class)
  override fun supportsSavepoints(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsNamedParameters(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsMultipleOpenResults(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsGetGeneratedKeys(): Boolean = false

  @Throws(SQLException::class)
  override fun getSuperTypes(catalog: String,
                             schemaPattern: String,
                             typeNamePattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getSuperTables(catalog: String,
                              schemaPattern: String,
                              tableNamePattern: String): ResultSet {

    val avroSchema = SchemaBuilder.builder().record("primarykeys")
        .fields()
        .nullableString("TABLE_CAT", null)
        .nullableString("TABLE_SCHEM", null)
        .nullableString("TABLE_NAME", null)
        .nullableString("SUPERTABLE_CAT", null)
        .nullableString("SUPERTABLE_SCHEM", null)
        .nullableString("SUPERTABLE_NAME", null)
        .endRecord()

    val data = GenericJdbcData(emptyArray(), avroSchema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getAttributes(catalog: String,
                             schemaPattern: String,
                             typeNamePattern: String,
                             attributeNamePattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun supportsResultSetHoldability(holdability: Int): Boolean = false

  @Throws(SQLException::class)
  override fun getResultSetHoldability(): Int = 0

  @Throws(SQLException::class)
  override fun getDatabaseMajorVersion(): Int {
    return Constants.getVersionMajor()
  }

  @Throws(SQLException::class)
  override fun getDatabaseMinorVersion(): Int {
    return Constants.getVersionMinor()
  }

  @Throws(SQLException::class)
  override fun getJDBCMajorVersion(): Int = 0

  @Throws(SQLException::class)
  override fun getJDBCMinorVersion(): Int = 0

  @Throws(SQLException::class)
  override fun getSQLStateType(): Int = 0

  @Throws(SQLException::class)
  override fun locatorsUpdateCopy(): Boolean = false

  @Throws(SQLException::class)
  override fun supportsStatementPooling(): Boolean = false

  @Throws(SQLException::class)
  override fun getRowIdLifetime(): RowIdLifetime? = null

  @Throws(SQLException::class)
  override fun getSchemas(catalog: String, schemaPattern: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  @Throws(SQLException::class)
  override fun autoCommitFailureClosesAllResultSets(): Boolean = false

  @Throws(SQLException::class)
  override fun getClientInfoProperties(): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun getFunctions(catalog: String,
                            schemaPattern: String,
                            functionNamePattern: String): ResultSet {

    val avroSchema = SchemaBuilder.builder().record("primarykeys")
        .fields()
        .nullableString("FUNCTION_CAT", null)
        .nullableString("FUNCTION_SCHEM", null)
        .nullableString("FUNCTION_NAME", null)
        .nullableString("REMARKS", null)
        .nullableInt("FUNCTION_TYPE", DatabaseMetaData.procedureResultUnknown)
        .nullableString("SPECIFIC_NAME", null)
        .endRecord()

    val data = GenericJdbcData(emptyArray(), avroSchema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet {
    val avroSchema = SchemaBuilder.builder().record("primarykeys")
        .fields()
        .nullableString("FUNCTION_CAT", null)
        .nullableString("FUNCTION_SCHEM", null)
        .nullableString("FUNCTION_NAME", null)
        .nullableString("COLUMN_NAME", null)
        .nullableInt("COLUMN_TYPE", DatabaseMetaData.procedureColumnIn)
        .nullableInt("DATA_TYPE", java.sql.Types.OTHER)
        .nullableString("SPECIFIC_NAME", null)
        .endRecord()

    val data = GenericJdbcData(emptyArray(), avroSchema, "")

    return LsqlJdbcResultSet(null, data)
  }

  @Throws(SQLException::class)
  override fun getPseudoColumns(arg0: String, arg1: String, arg2: String, arg3: String): ResultSet = getEmptyResultSet()

  @Throws(SQLException::class)
  override fun generatedKeyAlwaysReturned(): Boolean = false

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T? = null

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean = false
}
