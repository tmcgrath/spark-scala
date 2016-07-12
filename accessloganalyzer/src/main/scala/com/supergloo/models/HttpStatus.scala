package com.supergloo.models

/**
  * Created by sromic on 19/06/16.
  */
sealed abstract class HttpStatus(val status: String)

case class HttpInfoStatus(override val status: String) extends HttpStatus(status)
case class HttpSuccessStatus(override val status: String) extends HttpStatus(status)
case class HttpRedirectStatus(override val status: String) extends HttpStatus(status)
case class HttpClientErrorStatus(override val status: String) extends HttpStatus(status)
case class HttpServerErrorStatus(override val status: String) extends HttpStatus(status)