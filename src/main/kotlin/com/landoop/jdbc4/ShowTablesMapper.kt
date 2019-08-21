package com.landoop.jdbc4

import com.landoop.jdbc4.row.ListRow
import com.landoop.jdbc4.row.Row
import java.sql.DatabaseMetaData
import java.sql.Types

/**
 * Each table description has the following columns:
 *  <OL>
 *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
 *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
 *  <LI><B>TABLE_NAME</B> String {@code =>} table name
 *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
 *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
 *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
 *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
 *  <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be <code>null</code>)
 *  <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be <code>null</code>)
 *  <LI><B>TYPE_NAME</B> String {@code =>} type name (may be <code>null</code>)
 *  <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated
 *                  "identifier" column of a typed table (may be <code>null</code>)
 *  <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
 *                  SELF_REFERENCING_COL_NAME are created. Values are
 *                  "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
 *  </OL>
 */
object ShowTablesMapper : (Row) -> Row {
  override fun invoke(row: Row): Row = ListRow(
      listOf(
          null,
          null,
          row.getString(1),
          row.getString(2),
          null,
          null,
          null,
          null,
          null,
          null
      )
  )
}

val SelectFieldsMapper: (Row) -> Row = {
  val values = listOf(
      null,
      null,
      it.getString(2),
      it.getString(1),
      Types.OTHER, // todo AvroSchemas.sqlType(field.schema()),
      it.getString(3),
      0, // todo
      0,
      0, // todo in lenses sql field.schema().scale(), // DECIMAL_DIGITS
      10, // NUM_PREC_RADIX
      DatabaseMetaData.columnNullableUnknown, //if (field.schema().isNullable) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
      it.getString(4), // REMARKS
      null, // COLUMN_DEF unused
      null, // SQL_DATA_TYPE unused
      null, // SQL_DATETIME_SUB unused
      0, // CHAR_OCTET_LENGTH
      0, // pos + 1,  // todo update in samsql // ORDINAL_POSITION
      "NO", // todo update in samsql if (field.schema().isNullable) "YES" else "NO", // IS_NULLABLE
      null, // SCOPE_CATALOG
      null, // SCOPE_SCHEMA
      null, // SCOPE_TABLE
      null, // SOURCE_DATA_TYPE
      "NO", // IS_AUTOINCREMENT
      "" // IS_GENERATEDCOLUMN
  )
  assert(values.size == Schemas.Columns.fields.size) { "List has ${values.size} but should have ${Schemas.Columns.fields.size}" }
  ListRow(values)
}