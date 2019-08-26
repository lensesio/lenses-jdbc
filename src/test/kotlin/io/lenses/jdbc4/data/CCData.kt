package io.lenses.jdbc4.data

import java.util.*

interface CCData {

  val countries: List<String>
    get() = listOf("UK", "US", "DE", "ES", "FR")

  val currencies: List<String>
    get() = listOf("GBP", "USD", "AUD", "YEN", "EUR")

  val surnames: List<String>
    get() = listOf("picard", "riker", "troi", "crusher", "yar", "la forge", "son of mogh", "obrien", "soong")
  val firstnames: List<String>
    get() = listOf("jean luc", "william", "deanna", "beverley", "tasha", "geordi", "worf", "wesley", "miles", "data")

  data class CardData(val country: String,
                      val currency: String,
                      val surname: String,
                      val firstname: String,
                      val blocked: Boolean,
                      val number: String)

  fun <T> randomElement(list: List<T>): T = list[Random().nextInt(list.size)]

  fun randomCountry() = randomElement(countries)
  fun randomCurrency() = randomElement(currencies)
  fun randomFirstName() = randomElement(firstnames)
  fun randomSurname() = randomElement(surnames)
  fun randomCardNumber() = IntArray(16, { _ -> Random().nextInt(9) }).joinToString("")

  fun generateCC() = CardData(randomCountry(),
      randomCurrency(),
      randomSurname(),
      randomFirstName(),
      Random().nextBoolean(),
      randomCardNumber())

}