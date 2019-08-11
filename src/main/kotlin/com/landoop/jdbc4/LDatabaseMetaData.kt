package com.landoop.jdbc4

import arrow.core.getOrHandle
import com.landoop.jdbc4.client.LensesClient
import com.landoop.jdbc4.client.domain.Table
import com.landoop.jdbc4.resultset.ListResultSet
import com.landoop.jdbc4.resultset.filter
import com.landoop.jdbc4.row.ArrayRow
import com.landoop.jdbc4.row.Row
import com.landoop.jdbc4.util.Logging
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.apache.avro.Schema
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.RowIdLifetime
import java.sql.SQLException

class LDatabaseMetaData(private val conn: Connection,
                        private val client: LensesClient,
                        private val uri: String,
                        private val user: String) : DatabaseMetaData, Logging, IWrapper {

  override fun getUserName(): String = user
  override fun getURL(): String = uri

  override fun supportsGetGeneratedKeys(): Boolean = false
  override fun supportsCoreSQLGrammar(): Boolean = false

  override fun insertsAreDetected(type: Int): Boolean = false
  override fun deletesAreDetected(type: Int): Boolean = false

  override fun supportsIntegrityEnhancementFacility(): Boolean = false

  override fun getConnection(): Connection = conn

  override fun getAttributes(catalog: String?,
                             schemaPattern: String?,
                             typeNamePattern: String?,
                             attributeNamePattern: String?): ResultSet = RowResultSet.emptyOf(Schemas.Attributes)

  override fun supportsCatalogsInDataManipulation(): Boolean = false

  // is this right ?
  override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = true

  override fun autoCommitFailureClosesAllResultSets(): Boolean = false


  override fun getCatalogs(): ResultSet = RowResultSet.emptyOf(Schemas.Catalogs)


  override fun supportsCatalogsInTableDefinitions(): Boolean = false

  override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {
    return ResultSet.TYPE_FORWARD_ONLY == type && ResultSet.CONCUR_READ_ONLY == concurrency
  }

  override fun isReadOnly(): Boolean = true

  override fun usesLocalFiles(): Boolean = false

  override fun supportsResultSetType(type: Int): Boolean = ResultSet.TYPE_FORWARD_ONLY == type
  override fun supportsSchemasInIndexDefinitions(): Boolean = false

  @ObsoleteCoroutinesApi
  private suspend fun fetchTables(tableNamePattern: String?,
                                  types: Array<out String>?): ResultSet {

    val tableNameFilter: (ResultSet) -> Boolean = {
      when (tableNamePattern) {
        null -> true
        else -> it.getString(3).matches(tableNamePattern.replace("%", ".*").toRegex())
      }
    }

    val typeFilter: (ResultSet) -> Boolean = {
      types == null || types.isEmpty() || types.contains(it.getString(4))
    }

    return client.execute("SHOW TABLES", ShowTablesMapper)
        .getOrHandle { throw SQLException("Error retrieving tables: $it") }
        .filter(tableNameFilter)
        .filter(typeFilter)
  }

  override fun getTables(catalog: String?,
                         schemaPattern: String?,
                         tableNamePattern: String?,
                         types: Array<out String>?): ResultSet = runBlocking {
    fetchTables(tableNamePattern, types)
  }

  override fun getColumns(catalog: String?,
                          schemaPattern: String?,
                          tableNamePattern: String?,
                          columnNamePattern: String?): ResultSet {

    fun fieldToRow(table: Table, field: Schema.Field, pos: Int): Row {
      val array: Array<Any?> = arrayOf(
          null,
          null,
          table.name,
          field.name(),
          AvroSchemas.sqlType(field.schema()),
          AvroSchemas.normalizedName(field.schema()),
          0, // todo
          0,
          field.schema().scale(), // DECIMAL_DIGITS
          10, // NUM_PREC_RADIX
          if (field.schema().isNullable) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
          null, // REMARKS
          null, // COLUMN_DEF unused
          null, // SQL_DATA_TYPE unused
          null, // SQL_DATETIME_SUB unused
          0, // CHAR_OCTET_LENGTH
          pos + 1, // ORDINAL_POSITION
          if (field.schema().isNullable) "YES" else "NO", // IS_NULLABLE
          null, // SCOPE_CATALOG
          null, // SCOPE_SCHEMA
          null, // SCOPE_TABLE
          null, // SOURCE_DATA_TYPE
          "NO", // IS_AUTOINCREMENT
          "" // IS_GENERATEDCOLUMN
      )
      assert(array.size == Schemas.Columns.fields.size) { "Array has ${array.size} but should have ${Schemas.Columns.fields.size}" }
      return ArrayRow(array)
    }

//    val rows: List<Row> = fetchTables(tableNamePattern, null).flatMap { topic ->
//      when (topic.valueSchema) {
//        null, "" -> emptyList()
//        else -> {
//          val schema = Schema.Parser().parse(topic.valueSchema)!!
//          when (schema.type) {
//            Schema.Type.RECORD -> schema.fields.withIndex().map { (k, field) -> fieldToRow(topic, field, k) }
//            else -> emptyList()
//          }
//        }
//      }
//    }
//    return RowResultSet(null, Schemas.Columns, rows)

    TODO()
  }

  override fun supportsMultipleResultSets(): Boolean = true


  override fun getFunctions(catalog: String?,
                            schemaPattern: String?,
                            functionNamePattern: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.Functions)
  }

  override fun getSearchStringEscape(): String = "`"

  override fun getMaxTableNameLength(): Int = 0


  override fun supportsOpenStatementsAcrossCommit(): Boolean = false

  override fun ownInsertsAreVisible(type: Int): Boolean = false

  override fun getCatalogTerm(): String = "Catalog"
  override fun getSchemaTerm(): String = "Schema"
  override fun getProcedureTerm(): String = "Function"

  override fun isCatalogAtStart(): Boolean = false

  override fun getFunctionColumns(catalog: String?,
                                  schemaPattern: String?,
                                  functionNamePattern: String?,
                                  columnNamePattern: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.FunctionColumns)
  }

  override fun getPrimaryKeys(catalog: String?,
                              schema: String?,
                              table: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.PrimaryKeys)
  }

  override fun supportsANSI92IntermediateSQL(): Boolean = false

  override fun supportsLikeEscapeClause(): Boolean = false

  override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL

  override fun getMaxRowSize(): Int = 0

  override fun supportsOpenCursorsAcrossRollback(): Boolean = false

  override fun getTableTypes(): ResultSet = runBlocking {
    client.execute("SHOW TABLE TYPES", ShowTableTypesMapper)
        .getOrHandle { throw SQLException("Error retrieving table types: $it") }
  }

  override fun supportsNamedParameters(): Boolean = false

  override fun supportsConvert(): Boolean = false
  override fun supportsConvert(fromType: Int, toType: Int): Boolean = false

  override fun getProcedureColumns(catalog: String?,
                                   schemaPattern: String?,
                                   procedureNamePattern: String?,
                                   columnNamePattern: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.ProcedureColumns)
  }

  override fun allTablesAreSelectable(): Boolean = true

  override fun getCatalogSeparator(): String = "."

  override fun getSuperTypes(catalog: String?,
                             schemaPattern: String?,
                             typeNamePattern: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.Supertypes)
  }

  override fun getMaxBinaryLiteralLength(): Int = 0

  override fun getTypeInfo(): ResultSet {
    val rows = TypeInfo.all.map {
      val array: Array<Any?> = arrayOf(
          it.name,
          it.dataType,
          it.precision,
          it.literalEscape,
          it.literalEscape,
          null,
          DatabaseMetaData.typeNullable,
          false,
          true,
          it.signed,
          false,
          false,
          null,
          it.minScale,
          it.maxScale,
          0,
          0,
          10)
      ArrayRow(array)
    }
    return ListResultSet(null, Schemas.TypeInfo, rows)
  }

  override fun getVersionColumns(catalog: String?,
                                 schema: String?,
                                 table: String?): ResultSet = RowResultSet.emptyOf(Schemas.VersionColumns)

  override fun supportsMultipleOpenResults(): Boolean = false

  override fun supportsMinimumSQLGrammar(): Boolean = false


  override fun getExtraNameCharacters(): String = ""

  override fun getMaxCursorNameLength(): Int = 0

  override fun supportsSchemasInDataManipulation(): Boolean = false

  override fun getSchemas(): ResultSet = RowResultSet.emptyOf(Schemas.Schemas)

  override fun getSchemas(catalog: String?,
                          schemaPattern: String?): ResultSet = RowResultSet.emptyOf(Schemas.Schemas)


  override fun getMaxTablesInSelect(): Int = 0
  override fun getMaxStatements(): Int = 0
  override fun getMaxColumnsInTable(): Int = 0
  override fun getMaxSchemaNameLength(): Int = 0
  override fun getMaxIndexLength(): Int = 0
  override fun getMaxConnections(): Int = 0
  override fun getMaxStatementLength(): Int = 0
  override fun getMaxProcedureNameLength(): Int = 0
  override fun getMaxColumnsInSelect(): Int = 0
  override fun getMaxUserNameLength(): Int = 0
  override fun getMaxColumnsInIndex(): Int = 0

  override fun getCrossReference(parentCatalog: String?,
                                 parentSchema: String?,
                                 parentTable: String?,
                                 foreignCatalog: String?,
                                 foreignSchema: String?,
                                 foreignTable: String?): ResultSet = RowResultSet.emptyOf(Schemas.CrossReference)

  override fun supportsStatementPooling(): Boolean = false

  override fun supportsCatalogsInIndexDefinitions(): Boolean = false

  override fun getUDTs(catalog: String?,
                       schemaPattern: String?,
                       typeNamePattern: String?,
                       types: IntArray?): ResultSet {
    return RowResultSet.emptyOf(Schemas.UDT)
  }

  override fun supportsColumnAliasing(): Boolean = true

  override fun getClientInfoProperties(): ResultSet = RowResultSet.empty()

  override fun usesLocalFilePerTable(): Boolean = false


  override fun supportsSchemasInTableDefinitions(): Boolean = false

  override fun supportsSchemasInProcedureCalls(): Boolean = false
  override fun supportsCatalogsInProcedureCalls(): Boolean = false


  override fun getBestRowIdentifier(catalog: String?, schema: String?, table: String?, scope: Int, nullable: Boolean): ResultSet {
    return RowResultSet.empty()
  }

  override fun supportsTableCorrelationNames(): Boolean = false


  override fun supportsANSI92EntryLevelSQL(): Boolean = false

  override fun getPseudoColumns(catalog: String?,
                                schemaPattern: String?,
                                tableNamePattern: String?,
                                columnNamePattern: String?): ResultSet = RowResultSet.emptyOf(Schemas.PseudoColumns)

  override fun getProcedures(catalog: String?,
                             schemaPattern: String?,
                             procedureNamePattern: String?): ResultSet = RowResultSet.emptyOf(Schemas.Procedures)

  override fun supportsANSI92FullSQL(): Boolean = false

  override fun supportsAlterTableWithAddColumn(): Boolean = false

  override fun supportsResultSetHoldability(holdability: Int): Boolean = false

  override fun getColumnPrivileges(catalog: String?,
                                   schema: String?,
                                   table: String?,
                                   columnNamePattern: String?): ResultSet = RowResultSet.emptyOf(Schemas.ColumnPrivileges)

  override fun getImportedKeys(catalog: String?,
                               schema: String?, table: String?): ResultSet = RowResultSet.emptyOf(Schemas.ImportedKeys)

  override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_VALID_OTHER

  override fun doesMaxRowSizeIncludeBlobs(): Boolean = false

  override fun getIndexInfo(catalog: String?,
                            schema: String?,
                            table: String?,
                            unique: Boolean,
                            approximate: Boolean): ResultSet = RowResultSet.emptyOf(Schemas.IndexInfo)


  override fun supportsStoredProcedures(): Boolean = true

  override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet = RowResultSet.empty()

  override fun supportsPositionedDelete(): Boolean = false

  override fun supportsAlterTableWithDropColumn(): Boolean = false


  override fun getMaxCatalogNameLength(): Int = 0

  override fun supportsExtendedSQLGrammar(): Boolean = false


  override fun getMaxColumnNameLength(): Int = 0

  override fun nullPlusNonNullIsNull(): Boolean = false


  override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

  override fun allProceduresAreCallable(): Boolean = false

  override fun getSuperTables(catalog: String?,
                              schemaPattern: String?,
                              tableNamePattern: String?): ResultSet {
    return RowResultSet.emptyOf(Schemas.Supertables)
  }

  override fun generatedKeyAlwaysReturned(): Boolean = false

  override fun isWrapperFor(iface: Class<*>?): Boolean = _isWrapperFor(iface)
  override fun <T : Any?> unwrap(iface: Class<T>): T = _unwrap(iface)

  override fun getMaxCharLiteralLength(): Int = 0

  override fun supportsNonNullableColumns(): Boolean = true

  override fun supportsUnionAll(): Boolean = false
  override fun supportsUnion(): Boolean = false

  override fun supportsDifferentTableCorrelationNames(): Boolean = false

  override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false

  override fun supportsSelectForUpdate(): Boolean = false

  override fun supportsOpenCursorsAcrossCommit(): Boolean = false

  override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
    return RowResultSet.empty()
  }


  override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

  // subquery declarations

  override fun supportsSubqueriesInIns(): Boolean = true
  override fun supportsCorrelatedSubqueries(): Boolean = false
  override fun supportsSubqueriesInComparisons(): Boolean = false
  override fun supportsSubqueriesInExists(): Boolean = false
  override fun supportsSubqueriesInQuantifieds(): Boolean = false

  // order support

  override fun supportsExpressionsInOrderBy(): Boolean = false
  override fun supportsOrderByUnrelated(): Boolean = false
  override fun getMaxColumnsInOrderBy(): Int = 0

  // join support

  override fun supportsOuterJoins(): Boolean = false
  override fun supportsFullOuterJoins(): Boolean = false
  override fun supportsLimitedOuterJoins(): Boolean = false

  // group by functions

  override fun supportsGroupBy(): Boolean = false
  override fun supportsGroupByUnrelated(): Boolean = false
  override fun supportsGroupByBeyondSelect(): Boolean = false
  override fun getMaxColumnsInGroupBy(): Int = 0

  // null sorting

  override fun nullsAreSortedAtStart(): Boolean = false
  override fun nullsAreSortedAtEnd(): Boolean = false
  override fun nullsAreSortedHigh(): Boolean = false
  override fun nullsAreSortedLow(): Boolean = false

  // -- identifers

  override fun storesLowerCaseQuotedIdentifiers(): Boolean = false
  override fun storesLowerCaseIdentifiers(): Boolean = false
  override fun storesUpperCaseIdentifiers(): Boolean = false
  override fun supportsMixedCaseIdentifiers(): Boolean = false
  override fun getIdentifierQuoteString(): String = "`"
  override fun storesMixedCaseIdentifiers(): Boolean = false
  override fun storesUpperCaseQuotedIdentifiers(): Boolean = false
  override fun storesMixedCaseQuotedIdentifiers(): Boolean = false
  override fun supportsMixedCaseQuotedIdentifiers(): Boolean = false

  // keywords/functions supported by lenses

  override fun getSystemFunctions(): String = ""
  override fun getTimeDateFunctions(): String = ""
  override fun getStringFunctions(): String = ""
  override fun getNumericFunctions(): String = ""
  override fun getSQLKeywords(): String {
    return "AVRO, JSON, STRING, _ktype,_vtype, _key, _partition, _offset, _topic,_ts, _value"
  }

  // -- updates are not permitted on this read only driver

  override fun ownDeletesAreVisible(type: Int): Boolean = false
  override fun othersUpdatesAreVisible(type: Int): Boolean = false
  override fun ownUpdatesAreVisible(type: Int): Boolean = false
  override fun othersDeletesAreVisible(type: Int): Boolean = false
  override fun othersInsertsAreVisible(type: Int): Boolean = false
  override fun updatesAreDetected(type: Int): Boolean = false
  override fun supportsPositionedUpdate(): Boolean = false
  override fun locatorsUpdateCopy(): Boolean = false
  override fun supportsBatchUpdates(): Boolean = false

  // -- transactions are not supported

  override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_NONE
  override fun supportsTransactionIsolationLevel(level: Int): Boolean = level == Connection.TRANSACTION_NONE
  override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false
  override fun supportsOpenStatementsAcrossRollback(): Boolean = false
  override fun supportsTransactions(): Boolean = false
  override fun dataDefinitionIgnoredInTransactions(): Boolean = false
  override fun dataDefinitionCausesTransactionCommit(): Boolean = false
  override fun supportsMultipleTransactions(): Boolean = false
  override fun supportsDataManipulationTransactionsOnly(): Boolean = false
  override fun supportsSavepoints(): Boolean = false

// -- software / driver versionings

  override fun getDatabaseMinorVersion(): Int = Versions.databaseMinorVersion()
  override fun getDatabaseMajorVersion(): Int = Versions.databaseMajorVersion()
  override fun getDriverMinorVersion(): Int = Versions.driverMinorVersion()
  override fun getDriverMajorVersion(): Int = Versions.driverMajorVersion()
  override fun getDriverVersion(): String = "$driverMajorVersion.$driverMinorVersion"
  override fun getJDBCMinorVersion(): Int = 0
  override fun getJDBCMajorVersion(): Int = 4
  override fun getDatabaseProductName(): String = Constants.ProductName
  override fun getDriverName(): String = Constants.DriverName
  override fun getDatabaseProductVersion(): String = "$databaseMajorVersion.$databaseMinorVersion"
}