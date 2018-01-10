package com.landoop.jdbc4

import com.landoop.rest.RestClient
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.RowIdLifetime
import java.sql.SQLException

class LsqlDatabaseMetaData(private val conn: Connection,
                           private val client: RestClient,
                           private val uri: String,
                           private val user: String) : DatabaseMetaData, Logging {

  override fun supportsSubqueriesInQuantifieds(): Boolean = false

  override fun supportsGetGeneratedKeys(): Boolean = false

  override fun supportsCoreSQLGrammar(): Boolean = false

  override fun getMaxColumnsInIndex(): Int = 0

  override fun insertsAreDetected(type: Int): Boolean = false
  override fun deletesAreDetected(type: Int): Boolean = false

  override fun supportsIntegrityEnhancementFacility(): Boolean = false

  override fun getConnection(): Connection = conn

  override fun getAttributes(catalog: String?,
                             schemaPattern: String?,
                             typeNamePattern: String?,
                             attributeNamePattern: String?): ResultSet {
  }

  override fun getDatabaseProductVersion(): String = "0" // todo

  override fun supportsOpenStatementsAcrossRollback(): Boolean = false

  override fun getDatabaseProductName(): String = Constants.ProductName

  override fun getMaxProcedureNameLength(): Int = 0

  override fun getCatalogTerm(): String = "catalog"

  override fun supportsCatalogsInDataManipulation(): Boolean = false

  override fun getMaxUserNameLength(): Int = 0

  // todo set proper version
  override fun getJDBCMajorVersion(): Int = 0

  override fun getTimeDateFunctions(): String = ""

  // is this right ?
  override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  override fun autoCommitFailureClosesAllResultSets(): Boolean = false

  override fun getMaxColumnsInSelect(): Int = 0

  override fun getCatalogs(): ResultSet {
  }

  override fun storesLowerCaseQuotedIdentifiers(): Boolean = false

  override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

  override fun supportsCatalogsInTableDefinitions(): Boolean = false

  override fun getMaxColumnsInOrderBy(): Int = 0

  override fun getDriverMinorVersion(): Int = 0 // todo

  override fun storesLowerCaseIdentifiers(): Boolean = false
  override fun storesUpperCaseIdentifiers(): Boolean = false

  override fun supportsSchemasInIndexDefinitions(): Boolean = false

  override fun getMaxStatementLength(): Int = 0

  override fun supportsTransactions(): Boolean = false

  override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {
    return ResultSet.TYPE_FORWARD_ONLY == type && ResultSet.CONCUR_READ_ONLY == concurrency
  }

  override fun isReadOnly(): Boolean = true

  override fun usesLocalFiles(): Boolean = false

  override fun supportsResultSetType(type: Int): Boolean = ResultSet.TYPE_FORWARD_ONLY == type

  override fun getMaxConnections(): Int = 0

  override fun getTables(catalog: String?,
                         schemaPattern: String?,
                         tableNamePattern: String?,
                         types: Array<out String>?): ResultSet {
  }

  override fun supportsMultipleResultSets(): Boolean = true

  override fun dataDefinitionIgnoredInTransactions(): Boolean = false

  override fun getFunctions(catalog: String?,
                            schemaPattern: String?,
                            functionNamePattern: String?): ResultSet {
  }

  override fun getSearchStringEscape(): String = "`"

  override fun getMaxTableNameLength(): Int = 0

  override fun dataDefinitionCausesTransactionCommit(): Boolean = false

  override fun supportsOpenStatementsAcrossCommit(): Boolean = false

  override fun ownInsertsAreVisible(type: Int): Boolean = false

  override fun getSchemaTerm(): String = "schema"

  override fun isCatalogAtStart(): Boolean = false

  override fun getFunctionColumns(catalog: String?, schemaPattern: String?, functionNamePattern: String?, columnNamePattern: String?): ResultSet {
  }

  override fun supportsTransactionIsolationLevel(level: Int): Boolean = false

  override fun nullsAreSortedAtStart(): Boolean = false
  override fun nullsAreSortedAtEnd(): Boolean = false
  override fun nullsAreSortedHigh(): Boolean = false
  override fun nullsAreSortedLow(): Boolean = false

  override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
  }

  override fun getProcedureTerm(): String = "Function"

  override fun supportsANSI92IntermediateSQL(): Boolean = false

  override fun getDatabaseMajorVersion(): Int = 0 //  todo add database version numbers

  override fun supportsOuterJoins(): Boolean = false

  override fun <T : Any?> unwrap(iface: Class<T>): T {
    try {
      return iface.cast(this)
    } catch (cce: ClassCastException) {
      throw SQLException("Unable to unwrap instance as " + iface.toString())
    }
  }

  override fun supportsLikeEscapeClause(): Boolean = false
  override fun supportsPositionedUpdate(): Boolean = false
  override fun supportsMixedCaseIdentifiers(): Boolean = false
  override fun supportsLimitedOuterJoins(): Boolean = false

  override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL

  override fun getSystemFunctions(): String = ""

  override fun getMaxRowSize(): Int = 0

  override fun supportsOpenCursorsAcrossRollback(): Boolean = false

  override fun getTableTypes(): ResultSet {
  }

  override fun getMaxTablesInSelect(): Int = 0

  override fun getURL(): String = uri

  override fun supportsNamedParameters(): Boolean = false

  override fun supportsConvert(): Boolean = false
  override fun supportsConvert(fromType: Int, toType: Int): Boolean = false

  override fun getMaxStatements(): Int = 0

  override fun getProcedureColumns(catalog: String?, schemaPattern: String?, procedureNamePattern: String?, columnNamePattern: String?): ResultSet {
  }

  override fun allTablesAreSelectable(): Boolean = true

  override fun getJDBCMinorVersion(): Int = 0 // todo

  override fun getCatalogSeparator(): String = "."

  override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
  }

  override fun getMaxBinaryLiteralLength(): Int = 0

  override fun getTypeInfo(): ResultSet {
  }

  override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet {
  }

  override fun supportsMultipleOpenResults(): Boolean = false

  override fun getDatabaseMinorVersion(): Int = 0 // todo

  override fun supportsMinimumSQLGrammar(): Boolean = false

  override fun getMaxColumnsInGroupBy(): Int {
  }

  override fun getNumericFunctions(): String {
  }

  override fun getExtraNameCharacters(): String {
  }

  override fun getMaxCursorNameLength(): Int {
  }


  override fun supportsSchemasInDataManipulation(): Boolean {
  }

  override fun getSchemas(): ResultSet {
  }

  override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
  }

  override fun supportsCorrelatedSubqueries(): Boolean = false

  override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE

  override fun locatorsUpdateCopy(): Boolean = false

  override fun getColumns(catalog: String?, schemaPattern: String?, tableNamePattern: String?, columnNamePattern: String?): ResultSet {
  }

  override fun getCrossReference(parentCatalog: String?, parentSchema: String?, parentTable: String?, foreignCatalog: String?, foreignSchema: String?, foreignTable: String?): ResultSet {
  }

  override fun ownDeletesAreVisible(type: Int): Boolean = false
  override fun othersUpdatesAreVisible(type: Int): Boolean = false
  override fun ownUpdatesAreVisible(type: Int): Boolean = false

  override fun supportsStatementPooling(): Boolean = false

  override fun supportsCatalogsInIndexDefinitions(): Boolean = false

  override fun getUDTs(catalog: String?, schemaPattern: String?, typeNamePattern: String?, types: IntArray?): ResultSet {
  }

  override fun getStringFunctions(): String = ""

  override fun getMaxColumnsInTable(): Int = 0

  override fun supportsColumnAliasing(): Boolean = true

  override fun supportsSchemasInProcedureCalls(): Boolean = false

  override fun getClientInfoProperties(): ResultSet = LsqlResultSet.empty()

  override fun usesLocalFilePerTable(): Boolean = false

  override fun getIdentifierQuoteString(): String = "`"

  override fun supportsFullOuterJoins(): Boolean = false

  override fun supportsOrderByUnrelated(): Boolean = false

  override fun supportsSchemasInTableDefinitions(): Boolean = false

  override fun supportsCatalogsInProcedureCalls(): Boolean = false

  override fun getUserName(): String = user

  override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean): ResultSet {
    return LsqlResultSet.empty()
  }

  override fun supportsTableCorrelationNames(): Boolean = false

  override fun getMaxIndexLength(): Int = 0

  override fun supportsSubqueriesInExists(): Boolean = false

  override fun getMaxSchemaNameLength(): Int = 0

  override fun supportsANSI92EntryLevelSQL(): Boolean = false

  override fun getDriverVersion(): String = "todo" // todo

  override fun getPseudoColumns(catalog: String?,
                                schemaPattern: String?,
                                tableNamePattern: String?,
                                columnNamePattern: String?): ResultSet {
  }

  override fun supportsMixedCaseQuotedIdentifiers(): Boolean = false

  override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet {
  }

  override fun getDriverMajorVersion(): Int = 0// todo

  override fun supportsANSI92FullSQL(): Boolean = false

  override fun supportsAlterTableWithAddColumn(): Boolean = false

  override fun supportsResultSetHoldability(holdability: Int): Boolean = false

  override fun getColumnPrivileges(catalog: String?,
                                   schema: String?,
                                   table: String?,
                                   columnNamePattern: String?): ResultSet {
  }

  override fun getImportedKeys(catalog: String?,
                               schema: String?, table: String?): ResultSet {
  }

  override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_VALID_OTHER

  override fun getDriverName(): String = "LSQL JDBC Driver"

  override fun doesMaxRowSizeIncludeBlobs(): Boolean = false

  override fun supportsGroupBy(): Boolean = false
  override fun supportsGroupByUnrelated(): Boolean = false

  override fun getIndexInfo(catalog: String?,
                            schema: String?,
                            table: String?,
                            unique: Boolean,
                            approximate: Boolean): ResultSet {
  }

  override fun supportsSubqueriesInIns(): Boolean = true

  override fun supportsStoredProcedures(): Boolean = true

  override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet = LsqlResultSet.empty()

  override fun supportsPositionedDelete(): Boolean = false

  override fun supportsAlterTableWithDropColumn(): Boolean = false

  override fun supportsExpressionsInOrderBy(): Boolean = false

  override fun getMaxCatalogNameLength(): Int = 0

  override fun supportsExtendedSQLGrammar(): Boolean = false

  override fun othersInsertsAreVisible(type: Int): Boolean = false

  override fun updatesAreDetected(type: Int): Boolean = false

  override fun supportsDataManipulationTransactionsOnly(): Boolean = false

  override fun supportsSubqueriesInComparisons(): Boolean = false

  override fun supportsSavepoints(): Boolean = false

  override fun getSQLKeywords(): String {
    return "AVRO, JSON, STRING, _ktype,_vtype, _key, _partition, _offset, _topic,_ts, _value"
  }

  override fun getMaxColumnNameLength(): Int = 0

  override fun nullPlusNonNullIsNull(): Boolean = false

  override fun supportsGroupByBeyondSelect(): Boolean = false

  override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

  override fun allProceduresAreCallable(): Boolean = false

  override fun getSuperTables(catalog: String?,
                              schemaPattern: String?,
                              tableNamePattern: String?): ResultSet {
  }

  override fun generatedKeyAlwaysReturned(): Boolean = false

  override fun isWrapperFor(iface: Class<*>): Boolean = iface.isInstance(this)

  override fun storesUpperCaseQuotedIdentifiers(): Boolean = false

  override fun getMaxCharLiteralLength(): Int = 0

  override fun othersDeletesAreVisible(type: Int): Boolean = false

  override fun supportsNonNullableColumns(): Boolean = true

  override fun supportsUnionAll(): Boolean = false
  override fun supportsUnion(): Boolean = false

  override fun supportsDifferentTableCorrelationNames(): Boolean = false

  override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false

  override fun supportsSelectForUpdate(): Boolean = false

  override fun supportsMultipleTransactions(): Boolean = false

  override fun storesMixedCaseQuotedIdentifiers(): Boolean = false

  override fun supportsOpenCursorsAcrossCommit(): Boolean = false
  override fun storesMixedCaseIdentifiers(): Boolean = false

  override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
    return LsqlResultSet.empty()
  }

  override fun supportsBatchUpdates(): Boolean = false

  override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT
}