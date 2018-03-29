package com.landoop.jdbc4.client.domain

data class LoginResponse(val success: Boolean,
                         val token: String)