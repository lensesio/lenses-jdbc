package io.lenses.jdbc4.mappers

import io.lenses.jdbc4.row.ListRow
import io.lenses.jdbc4.row.Row

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

