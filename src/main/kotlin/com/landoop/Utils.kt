package com.landoop

import java.util.regex.Pattern

object Utils {
  fun like(str: String, expr: String): Boolean {
    var regex = buildExpr(expr)
    regex = regex.replace("_", ".").replace("%", ".*?")
    val p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE or Pattern.DOTALL)
    return p.matcher(str).matches()
  }

  fun buildExpr(s: String?): String {
    if (s == null) {
      throw IllegalArgumentException("String cannot be null")
    }

    val len = s.length
    if (len == 0) {
      return ""
    }

    val sb = StringBuilder(len * 2)
    for (i in 0 until len) {
      val c = s[i]
      if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
        sb.append("\\")
      }
      sb.append(c)
    }
    return sb.toString()
  }
}