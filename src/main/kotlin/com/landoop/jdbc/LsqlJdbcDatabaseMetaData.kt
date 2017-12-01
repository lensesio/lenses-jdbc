package com.landoop.jdbc

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
  override fun isReadOnly(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun nullsAreSortedHigh(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun nullsAreSortedLow(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun nullsAreSortedAtStart(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun nullsAreSortedAtEnd(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getDatabaseProductName(): String {
    return "LSQL for Apache Kafka"
  }

  @Throws(SQLException::class)
  override fun getDatabaseProductVersion(): String {
    return Constants.getVersion()
  }

  @Throws(SQLException::class)
  override fun getDriverName(): String {
    return "LSQL JDBC Driver"
  }

  @Throws(SQLException::class)
  override fun getDriverVersion(): String {
    return LsqlJdbcDriver.getVersion()
  }

  override fun getDriverMajorVersion(): Int {
    return Constants.getVersionMajor()
  }

  override fun getDriverMinorVersion(): Int {
    return Constants.getVersionMinor()
  }

  @Throws(SQLException::class)
  override fun usesLocalFiles(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun usesLocalFilePerTable(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsMixedCaseIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesUpperCaseIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesLowerCaseIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesMixedCaseIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsMixedCaseQuotedIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesUpperCaseQuotedIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesLowerCaseQuotedIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun storesMixedCaseQuotedIdentifiers(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getIdentifierQuoteString(): String {
    return " "
  }

  @Throws(SQLException::class)
  override fun getSQLKeywords(): String {
    return "@rid,@class,@version,@size,@type,@this,CONTAINS,CONTAINSALL,CONTAINSKEY," + "CONTAINSVALUE,CONTAINSTEXT,MATCHES,TRAVERSE"
  }

  @Throws(SQLException::class)
  override fun getNumericFunctions(): String? {

    return null
  }

  @Throws(SQLException::class)
  override fun getStringFunctions(): String {

    return ""
  }

  @Throws(SQLException::class)
  override fun getSystemFunctions(): String {

    return ""
  }

  @Throws(SQLException::class)
  override fun getTimeDateFunctions(): String {
    return "date,sysdate"
  }

  @Throws(SQLException::class)
  override fun getSearchStringEscape(): String? {

    return null
  }

  @Throws(SQLException::class)
  override fun getExtraNameCharacters(): String? {
    return null
  }

  @Throws(SQLException::class)
  override fun supportsAlterTableWithAddColumn(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsAlterTableWithDropColumn(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsColumnAliasing(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun nullPlusNonNullIsNull(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsConvert(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsConvert(fromType: Int, toType: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsTableCorrelationNames(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsDifferentTableCorrelationNames(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsExpressionsInOrderBy(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOrderByUnrelated(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsGroupBy(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsGroupByUnrelated(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsGroupByBeyondSelect(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsLikeEscapeClause(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsMultipleResultSets(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsMultipleTransactions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsNonNullableColumns(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun supportsMinimumSQLGrammar(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCoreSQLGrammar(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsExtendedSQLGrammar(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsANSI92EntryLevelSQL(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsANSI92IntermediateSQL(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsANSI92FullSQL(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsIntegrityEnhancementFacility(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOuterJoins(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsFullOuterJoins(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsLimitedOuterJoins(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getSchemaTerm(): String? {

    return null
  }

  @Throws(SQLException::class)
  override fun getProcedureTerm(): String {
    return "Function"
  }

  @Throws(SQLException::class)
  override fun getCatalogTerm(): String? {

    return null
  }

  @Throws(SQLException::class)
  override fun isCatalogAtStart(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getCatalogSeparator(): String? {

    return null
  }

  @Throws(SQLException::class)
  override fun supportsSchemasInDataManipulation(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSchemasInProcedureCalls(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSchemasInTableDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSchemasInIndexDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSchemasInPrivilegeDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCatalogsInDataManipulation(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCatalogsInProcedureCalls(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCatalogsInTableDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCatalogsInIndexDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCatalogsInPrivilegeDefinitions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsPositionedDelete(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsPositionedUpdate(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSelectForUpdate(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsStoredProcedures(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun supportsSubqueriesInComparisons(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSubqueriesInExists(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsSubqueriesInIns(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun supportsSubqueriesInQuantifieds(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsCorrelatedSubqueries(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsUnion(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun supportsUnionAll(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOpenCursorsAcrossCommit(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOpenCursorsAcrossRollback(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOpenStatementsAcrossCommit(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsOpenStatementsAcrossRollback(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getMaxBinaryLiteralLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxCharLiteralLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnNameLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnsInGroupBy(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnsInIndex(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnsInOrderBy(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnsInSelect(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxColumnsInTable(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxConnections(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxCursorNameLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxIndexLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxSchemaNameLength(): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxProcedureNameLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxCatalogNameLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxRowSize(): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun doesMaxRowSizeIncludeBlobs(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getMaxStatementLength(): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxStatements(): Int {
    return 0
  }

  @Throws(SQLException::class)
  override fun getMaxTableNameLength(): Int {
    return 1024
  }

  @Throws(SQLException::class)
  override fun getMaxTablesInSelect(): Int {
    return 1
  }

  @Throws(SQLException::class)
  override fun getMaxUserNameLength(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getDefaultTransactionIsolation(): Int {
    return java.sql.Connection.TRANSACTION_NONE
  }

  @Throws(SQLException::class)
  override fun supportsTransactions(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun supportsTransactionIsolationLevel(level: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsDataManipulationTransactionsOnly(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun dataDefinitionCausesTransactionCommit(): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun dataDefinitionIgnoredInTransactions(): Boolean {
    return true
  }

  @Throws(SQLException::class)
  override fun getProcedures(catalog: String, schemaPattern: String, procedureNamePattern: String): ResultSet {
    val resultSet = OInternalResultSet()

    val functionLibrary = database.getMetadata().getFunctionLibrary()

    for (functionName in functionLibrary.getFunctionNames()) {

      if (OrientJdbcUtils.like(functionName, procedureNamePattern)) {
        val element = OResultInternal()
        element.setProperty("PROCEDURE_CAT", null as Any?)
        element.setProperty("PROCEDURE_SCHEM", null as Any?)
        element.setProperty("PROCEDURE_NAME", functionName)
        element.setProperty("REMARKS", "")
        element.setProperty("PROCEDURE_TYPE", DatabaseMetaData.procedureResultUnknown)
        element.setProperty("SPECIFIC_NAME", functionName)

        resultSet.add(element)
      }
    }

    return LsqlJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getProcedureColumns(catalog: String, schemaPattern: String, procedureNamePattern: String, columnNamePattern: String): ResultSet {
    throw SQLFeatureNotSupportedException()
  }

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
    database.activateOnCurrentThread()
    val resultSet = OInternalResultSet()

    val field = OResultInternal()
    field.setProperty("TABLE_SCHEM", database.getName())
    field.setProperty("TABLE_CATALOG", database.getName())

    resultSet.add(field)

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getCatalogs(): ResultSet {
    database.activateOnCurrentThread()

    val resultSet = OInternalResultSet()

    val field = OResultInternal()
    field.setProperty("TABLE_CAT", database.getName())

    resultSet.add(field)

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getTableTypes(): ResultSet {
    database.activateOnCurrentThread()

    val resultSet = OInternalResultSet()
    for (tableType in TABLE_TYPES) {
      val field = OResultInternal()
      field.setProperty("TABLE_TYPE", tableType)
      resultSet.add(field)
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getColumns(catalog: String, schemaPattern: String, tableNamePattern: String,
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
  override fun getColumnPrivileges(catalog: String, schema: String, table: String,
                                   columnNamePattern: String): ResultSet {
    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getTablePrivileges(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getBestRowIdentifier(catalog: String, schema: String, table: String, scope: Int, nullable: Boolean): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getVersionColumns(catalog: String, schema: String, table: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getPrimaryKeys(catalog: String, schema: String, table: String): ResultSet {
    database.activateOnCurrentThread()
    val classIndexes = database.getMetadata().getIndexManager().getClassIndexes(table)

    val uniqueIndexes = HashSet<E>()

    for (oIndex in classIndexes) {
      if (oIndex.getType().equals(INDEX_TYPE.UNIQUE.name()))
        uniqueIndexes.add(oIndex)
    }

    val resultSet = OInternalResultSet()

    for (unique in uniqueIndexes) {
      var keyFiledSeq = 1
      for (keyFieldName in unique.getDefinition().getFields()) {
        val res = OResultInternal()
        res.setProperty("TABLE_CAT", catalog)
        res.setProperty("TABLE_SCHEM", catalog)
        res.setProperty("TABLE_NAME", table)
        res.setProperty("COLUMN_NAME", keyFieldName)
        res.setProperty("KEY_SEQ", keyFiledSeq)
        res.setProperty("PK_NAME", unique.getName())
        keyFiledSeq++

        resultSet.add(res)
      }
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getImportedKeys(catalog: String, schema: String, table: String): ResultSet {

    database.activateOnCurrentThread()

    val aClass = database.getMetadata().getSchema().getClass(table)

    aClass.declaredProperties().stream().forEach { p -> p.getType() }
    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  private fun getEmptyResultSet(): ResultSet {
    database.activateOnCurrentThread()

    return OrientJdbcResultSet(OrientJdbcStatement(connection), OInternalResultSet(), ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getExportedKeys(catalog: String, schema: String, table: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getCrossReference(parentCatalog: String, parentSchema: String, parentTable: String, foreignCatalog: String,
                                 foreignSchema: String, foreignTable: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun getTypeInfo(): ResultSet {
    val resultSet = OInternalResultSet()

    var res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.BINARY.toString())
    res.setProperty("DATA_TYPE", Types.BINARY)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.BOOLEAN.toString())
    res.setProperty("DATA_TYPE", Types.BOOLEAN)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.BYTE.toString())
    res.setProperty("DATA_TYPE", Types.TINYINT)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.DATE.toString())
    res.setProperty("DATA_TYPE", Types.DATE)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.DATETIME.toString())
    res.setProperty("DATA_TYPE", Types.DATE)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.DECIMAL.toString())
    res.setProperty("DATA_TYPE", Types.DECIMAL)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.FLOAT.toString())
    res.setProperty("DATA_TYPE", Types.FLOAT)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.DOUBLE.toString())
    res.setProperty("DATA_TYPE", Types.DOUBLE)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.EMBEDDED.toString())
    res.setProperty("DATA_TYPE", Types.STRUCT)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.EMBEDDEDLIST.toString())
    res.setProperty("DATA_TYPE", Types.ARRAY)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.INTEGER.toString())
    res.setProperty("DATA_TYPE", Types.INTEGER)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.LINKLIST.toString())
    res.setProperty("DATA_TYPE", Types.ARRAY)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.LONG.toString())
    res.setProperty("DATA_TYPE", Types.BIGINT)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.STRING.toString())
    res.setProperty("DATA_TYPE", Types.VARCHAR)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    res = OResultInternal()
    res.setProperty("TYPE_NAME", OType.SHORT.toString())
    res.setProperty("DATA_TYPE", Types.SMALLINT)
    res.setProperty("NULLABLE", DatabaseMetaData.typeNullable)
    res.setProperty("CASE_SENSITIVE", true)
    res.setProperty("UNSIGNED_ATTRIBUTE", false)
    res.setProperty("SEARCHABLE", true)
    resultSet.add(res)

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getIndexInfo(catalog: String, schema: String, table: String, unique: Boolean, approximate: Boolean): ResultSet {
    database.activateOnCurrentThread()
    val metadata = database.getMetadata()
    if (!approximate) {
      metadata.getIndexManager().reload()
    }

    val classIndexes = metadata.getIndexManager().getClassIndexes(table)

    val indexes = HashSet<E>()

    for (oIndex in classIndexes) {
      if (!unique || oIndex.getType().equals(INDEX_TYPE.UNIQUE.name()))
        indexes.add(oIndex)
    }

    val resultSet = OInternalResultSet()
    for (idx in indexes) {
      val notUniqueIndex = !idx.getType().equals(INDEX_TYPE.UNIQUE.name())

      val fieldNames = idx.getDefinition().getFields().toString()

      val res = OResultInternal()
      res.setProperty("TABLE_CAT", catalog)
      res.setProperty("TABLE_SCHEM", schema)
      res.setProperty("TABLE_NAME", table)
      res.setProperty("NON_UNIQUE", notUniqueIndex)
      res.setProperty("INDEX_QUALIFIER", null as Any?)
      res.setProperty("INDEX_NAME", idx.getName())
      res.setProperty("TYPE", idx.getType())
      res.setProperty("ORDINAL_POSITION", 0)
      res.setProperty("COLUMN_NAME", fieldNames.substring(1, fieldNames.length - 1))
      res.setProperty("ASC_OR_DESC", "ASC")

      resultSet.add(res)
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun supportsResultSetType(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun ownUpdatesAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun ownDeletesAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun ownInsertsAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun othersUpdatesAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun othersDeletesAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun othersInsertsAreVisible(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun updatesAreDetected(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun deletesAreDetected(type: Int): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun insertsAreDetected(type: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsBatchUpdates(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getUDTs(catalog: String, schemaPattern: String, typeNamePattern: String, types: IntArray): ResultSet {
    throw NotImplementedError()
  }

  @Throws(SQLException::class)
  override fun getConnection(): Connection {
    return connection
  }

  @Throws(SQLException::class)
  override fun supportsSavepoints(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsNamedParameters(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsMultipleOpenResults(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsGetGeneratedKeys(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getSuperTypes(catalog: String, schemaPattern: String, typeNamePattern: String): ResultSet {
    throw NotImplementedError()
  }

  @Throws(SQLException::class)
  override fun getSuperTables(catalog: String, schemaPattern: String, tableNamePattern: String): ResultSet {
     val cls = database.getMetadata().getSchema().getClass(tableNamePattern)
    val resultSet = OInternalResultSet()

    if (cls != null && cls!!.getSuperClass() != null) {
      val res = OResultInternal()

      res.setProperty("TABLE_CAT", catalog)
      res.setProperty("TABLE_SCHEM", catalog)
      res.setProperty("TABLE_NAME", cls!!.getName())
      res.setProperty("SUPERTABLE_CAT", catalog)
      res.setProperty("SUPERTABLE_SCHEM", catalog)
      res.setProperty("SUPERTABLE_NAME", cls!!.getSuperClass().getName())
      resultSet.add(res)
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)
  }

  @Throws(SQLException::class)
  override fun getAttributes(catalog: String, schemaPattern: String, typeNamePattern: String, attributeNamePattern: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun supportsResultSetHoldability(holdability: Int): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getResultSetHoldability(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getDatabaseMajorVersion(): Int {
    return Constants.getVersionMajor()
  }

  @Throws(SQLException::class)
  override fun getDatabaseMinorVersion(): Int {
    return Constants.getVersionMinor()
  }

  @Throws(SQLException::class)
  override fun getJDBCMajorVersion(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getJDBCMinorVersion(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun getSQLStateType(): Int {

    return 0
  }

  @Throws(SQLException::class)
  override fun locatorsUpdateCopy(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun supportsStatementPooling(): Boolean {

    return false
  }

  @Throws(SQLException::class)
  override fun getRowIdLifetime(): RowIdLifetime? {

    return null
  }

  @Throws(SQLException::class)
  override fun getSchemas(catalog: String, schemaPattern: String): ResultSet {

    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun supportsStoredFunctionsUsingCallSyntax(): Boolean {

    return true
  }

  @Throws(SQLException::class)
  override fun autoCommitFailureClosesAllResultSets(): Boolean {
    return false
  }

  @Throws(SQLException::class)
  override fun getClientInfoProperties(): ResultSet {

    return getEmptyResultSet()

  }

  @Throws(SQLException::class)
  override fun getFunctions(catalog: String, schemaPattern: String, functionNamePattern: String): ResultSet {
    throw NotImplementedError()
/*
     val resultSet = OInternalResultSet()
    for (fName in database.getMetadata().getFunctionLibrary().getFunctionNames()) {
      val res = OResultInternal()
      res.setProperty("FUNCTION_CAT", null as Any?)
      res.setProperty("FUNCTION_SCHEM", null as Any?)
      res.setProperty("FUNCTION_NAME", fName)
      res.setProperty("REMARKS", "")
      res.setProperty("FUNCTION_TYPE", DatabaseMetaData.procedureResultUnknown)
      res.setProperty("SPECIFIC_NAME", fName)

      resultSet.add(res)
    }

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)*/
  }

  @Throws(SQLException::class)
  override fun getFunctionColumns(catalog: String, schemaPattern: String, functionNamePattern: String, columnNamePattern: String): ResultSet {
    throw NotImplementedError()
    /*database.activateOnCurrentThread()
    val resultSet = OInternalResultSet()

    val f = database.getMetadata().getFunctionLibrary().getFunction(functionNamePattern)

    for (p in f.getParameters()) {
      val res = OResultInternal()
      res.setProperty("FUNCTION_CAT", null as Any?)
      res.setProperty("FUNCTION_SCHEM", null as Any?)
      res.setProperty("FUNCTION_NAME", f.getName())
      res.setProperty("COLUMN_NAME", p)
      res.setProperty("COLUMN_TYPE", DatabaseMetaData.procedureColumnIn)
      res.setProperty("DATA_TYPE", java.sql.Types.OTHER)
      res.setProperty("SPECIFIC_NAME", f.getName())
      resultSet.add(res)

    }

    val res = OResultInternal()
    res.setProperty("FUNCTION_CAT", null as Any?)
    res.setProperty("FUNCTION_SCHEM", null as Any?)
    res.setProperty("FUNCTION_NAME", f.getName())
    res.setProperty("COLUMN_NAME", "return")
    res.setProperty("COLUMN_TYPE", DatabaseMetaData.procedureColumnReturn)
    res.setProperty("DATA_TYPE", java.sql.Types.OTHER)
    res.setProperty("SPECIFIC_NAME", f.getName())

    resultSet.add(res)

    return OrientJdbcResultSet(OrientJdbcStatement(connection), resultSet, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)*/
  }

  @Throws(SQLException::class)
  override fun getPseudoColumns(arg0: String, arg1: String, arg2: String, arg3: String): ResultSet {
    return getEmptyResultSet()
  }

  @Throws(SQLException::class)
  override fun generatedKeyAlwaysReturned(): Boolean {
    return false
  }

  private fun getPropertyAsDocument(clazz: OClass, prop: OProperty): OResultInternal {
    val type = prop.getType()
    val res = LsqlResultInternal()
    res.setProperty("TABLE_CAT", Constants.DatabaseName)
    res.setProperty("TABLE_SCHEM", Constants.DatabaseName)
    res.setProperty("TABLE_NAME", clazz.getName())
    res.setProperty("COLUMN_NAME", prop.getName())
    res.setProperty("DATA_TYPE", OrientJdbcResultSetMetaData.getSqlType(type))
    res.setProperty("TYPE_NAME", type.name())
    res.setProperty("COLUMN_SIZE", 1)
    res.setProperty("BUFFER_LENGTH", null)
    res.setProperty("DECIMAL_DIGITS", null)
    res.setProperty("NUM_PREC_RADIX", 10)
    res.setProperty("NULLABLE", if (!prop.isNotNull()) DatabaseMetaData.columnNoNulls else DatabaseMetaData.columnNullable)
    res.setProperty("REMARKS", prop.getDescription())
    res.setProperty("COLUMN_DEF", prop.getDefaultValue())
    res.setProperty("SQL_DATA_TYPE", null)
    res.setProperty("SQL_DATETIME_SUB", null)
    res.setProperty("CHAR_OCTET_LENGTH", null)
    res.setProperty("ORDINAL_POSITION", prop.getId())
    res.setProperty("IS_NULLABLE", if (prop.isNotNull()) "NO" else "YES")

    return res
  }

  @Throws(SQLException::class)
  override fun <T> unwrap(iface: Class<T>): T? {

    return null
  }

  @Throws(SQLException::class)
  override fun isWrapperFor(iface: Class<*>): Boolean {

    return false
  }
}
