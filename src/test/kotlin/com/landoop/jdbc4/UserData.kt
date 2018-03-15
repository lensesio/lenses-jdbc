package com.landoop.jdbc4

import java.util.*

interface UserData {

  val random: Random

  data class Address(val street: String, val streetnumber: Int, val zipcode: Int, val state: String)
  data class Personal(val firstname: String, val lastname: String, val birthday: String, val title: String)
  data class Geo(val lat: Double, val lon: Double)
  data class User(val id: Long, val email: String, val address: Address, val personal: Personal, val lastLoginGeo: Geo)

  val states: List<String>
    get() = listOf("Texas", "Utah", "Lousiana", "Hawaii", "New York", "California", "Oregon", "Iowa")

  val titles: List<String>
    get() = listOf("Mr", "Mrs", "Dr", "Miss", "Ms", "Prof")

  fun <T> randomElement(list: List<T>): T = list[Random().nextInt(list.size)]

  fun randomTitle(): String = randomElement(titles)
  fun randomGeo(): Geo = Geo(random.nextDouble(), random.nextDouble())
  fun randomState() = randomElement(states)
  fun randomAddress(): Address = Address("Baker Street", random.nextInt(400), random.nextInt(50000), randomState())
  fun randomPersonal(): Personal = Personal("Sammy", "Smith", Date().toString(), randomTitle())
  fun randomUser() = User(random.nextLong(), "someemail@landoop.com", randomAddress(), randomPersonal(), randomGeo())
}