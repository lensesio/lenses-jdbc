package io.lenses.jdbc4.mappers

import io.lenses.jdbc4.Schemas
import io.lenses.jdbc4.row.ListRow
import io.lenses.jdbc4.row.Row
import java.sql.DatabaseMetaData
import java.sql.Types

val SelectFieldsMapper: (Row) -> Row = { row ->
  val values = listOf(
      null,
      null,
      row.getString(2),
      row.getString(1),
      Types.OTHER, // todo AvroSchemas.sqlType(field.schema()),
      row.getString(3),
      0, // todo
      0,
      0, // todo in lenses sql field.schema().scale(), // DECIMAL_DIGITS
      10, // NUM_PREC_RADIX
      DatabaseMetaData.columnNullableUnknown, //if (field.schema().isNullable) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
      row.getString(4), // REMARKS
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