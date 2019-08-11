package com.landoop.jdbc4

import com.landoop.jdbc4.row.ArrayRow
import com.landoop.jdbc4.row.Row

/**
 * <P>The table type is:
 *  <OL>
 *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
 *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
 *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
 *  </OL>
 */
object ShowTableTypesMapper : (Row) -> Row {
  override fun invoke(row: Row): Row = ArrayRow(arrayOf(row.getString(1)))
}