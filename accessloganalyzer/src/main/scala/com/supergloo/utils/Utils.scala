package com.supergloo.utils

import com.supergloo.models._

/**
  * https://www.supergloo.com
  */
object Utils {

  private val httpStatuses = List(
    "100", "101", "103",
    "200", "201", "202", "203", "204", "205", "206",
    "300", "301", "302", "303", "304", "305", "306", "307", "308",
    "400", "401", "402", "403", "404", "405", "406", "407", "408", "409", "410", "411", "412", "413", "414", "415", "416", "417",
    "500", "501", "502", "503", "504", "505", "511"
  )

  def populateHttpStatusList(): List[HttpStatus] = {
      httpStatuses map createHttpStatus
  }

  def createHttpStatus(status: String): HttpStatus = status match {
    case status if (status.startsWith("1")) => HttpInfoStatus(status)
    case status if (status.startsWith("2")) => HttpSuccessStatus(status)
    case status if (status.startsWith("3")) => HttpRedirectStatus(status)
    case status if (status.startsWith("4")) => HttpClientErrorStatus(status)
    case status if (status.startsWith("5")) => HttpServerErrorStatus(status)
  }

}
