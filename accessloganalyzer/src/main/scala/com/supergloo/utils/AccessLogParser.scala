package com.supergloo.utils

import java.util.regex.Pattern

import com.supergloo.models.HttpStatus

/**
  * https://www.supergloo.com
  */
object AccessLogParser extends Serializable {
  import Utils._

  private val ddd = "\\d{1,3}"
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
  private val client = "(\\S+)"
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])"
  private val request = "\"(.*?)\""
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val accessLogRegex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  private val p = Pattern.compile(accessLogRegex)

  /**
    * Extract HTTP status code and create HttpStatus instance for given status code
    */
  def parseHttpStatusCode(logLine: String): Option[HttpStatus] = {
    val matcher = p.matcher(logLine)
    if(matcher.find) {
      Some(createHttpStatus(matcher.group(6)))
    }
    else {
      None
    }
  }

}
