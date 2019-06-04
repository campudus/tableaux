package com.campudus.tableaux.testtools

import com.campudus.tableaux.helper.VertxAccess
import io.vertx.scala.core.Vertx
import io.vertx.scala.ext.auth.jwt.{JWTAuth, JWTAuthOptions, JWTOptions}
import org.vertx.scala.core.json.{Json, JsonObject}

object TokenHelper {

  def apply(vertxAccess: VertxAccess): TokenHelper = {
    new TokenHelper(vertxAccess)
  }
}

class TokenHelper(vertxAccess: VertxAccess) extends VertxAccess {

  override val vertx: Vertx = vertxAccess.vertx

  val config = s"""
                  |{
                  |  "pubSecKeys": [
                  |    {
                  |      "algorithm": "RS256",
                  |      "publicKey": "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuuFUzR6uqEU80fNWA88F
                  |                    InoGtUgeQ7JEm1yzh8ii6zP0u+FzezLveJAhTO63YtbYddiyZZ+oQcA4ONbyBQLT
                  |                    rtC9X9Nbi/dhaygWFkZYLoNhGbOASrOCOIAStsU2pRfcOt/7WTxV6G/RaugO3/fA
                  |                    rfvs/8SZ54qS1g2fIHz4jhKepQj/SRxqvhTLSY6cQHEqiToxAVjONV1toLaHWDbV
                  |                    SA1mVZ9hbhdhE07DZaT/YS4EgjgrLTxQohqy7R9pqk6yJ6TsOcbQfbXVZpKv5BoO
                  |                    V+EtDjlvHxAyHH0Dg2pCK8HSmyEqKaNfG/R/HfYb8JC4tIJunEEhm/1fEt1EkxIZ
                  |                    qQIDAQAB",
                  |      "secretKey": "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC64VTNHq6oRTzR
                  |                    81YDzwUiega1SB5DskSbXLOHyKLrM/S74XN7Mu94kCFM7rdi1th12LJln6hBwDg4
                  |                    1vIFAtOu0L1f01uL92FrKBYWRlgug2EZs4BKs4I4gBK2xTalF9w63/tZPFXob9Fq
                  |                    6A7f98Ct++z/xJnnipLWDZ8gfPiOEp6lCP9JHGq+FMtJjpxAcSqJOjEBWM41XW2g
                  |                    todYNtVIDWZVn2FuF2ETTsNlpP9hLgSCOCstPFCiGrLtH2mqTrInpOw5xtB9tdVm
                  |                    kq/kGg5X4S0OOW8fEDIcfQODakIrwdKbISopo18b9H8d9hvwkLi0gm6cQSGb/V8S
                  |                    3USTEhmpAgMBAAECggEAa/Voa+bht0voStFsS1749GXSIj+7XBhMEgSHolWB6KZn
                  |                    J3Kip/VQ6jE5S5xMTMkY21uIE7UcGn/U+uERh1uOtlrYS9dp9329xY2u1Mdmgdhb
                  |                    6+EKqBzziXhTV0quuskB7PEf3vlAF7shG8Vbcn9JzDjRPSByWJRxJz9PQhFv9YJF
                  |                    vPHkeMwQk5h6Ud/MKIIEBnQd8g5Wq41k2EStOJYxm7fL+fawsdxp4eFmRFJSnOzp
                  |                    HPW1F2tzUdSEEtJfAUUhOF3db/R8A6QNuhYRqr5wPp8VEuqgiH9jBAFZdKqEjcKR
                  |                    l/ZVAJ9oH54YkEBbkXGW6kAEbIBFj349sQ63x0ZwAQKBgQD1wTHVAykuusP3K20A
                  |                    omdtGg+P5huUWzPmLKzGf1pjfbFxnywFncW4pkavagL7gFAPxnV4cQAtZRotjIXr
                  |                    tMX37Rir/+upv9EM1TIbLzmd8qKOJxhDB2BIRvYor34jO4V03jHIRMoJR/PMnap2
                  |                    voh1SgsKKHRjkWxXDM3kv2PsKQKBgQDCq80zxo4YqjBOHGLN8yPq+m4+gV3k1KTq
                  |                    N71xkFMxbsS89PcdHXMW+oT0AHF5FAQh8YLMnJYT1wJEJPivZ0r9RKR/9qiE//Ep
                  |                    aKVWCHpzm17IG+Z74xu54kNxme8In2zdpz5vLlwoGSWdfy4qTwTTcTw/4ZsfynB8
                  |                    W42LAepxgQKBgHwG05JwdPFLeqkcdneSfuYV9/KkrBiUar3ooA3Rqhl6DvqL3Vi8
                  |                    RlQpPpU6yGSLXlyHyTNOvEsssih4ugG6CwtT0lbD4viZgPScCBymGcr38EgTvO/f
                  |                    Ih14CrV/1AYN/Q19Mdyjst86O/VxQN2KzS18f9PRlOPHOck5AhRG7zP5AoGBALWZ
                  |                    8W64bnyB31gu0NlRVZNyFYAHzOCYolPAteCIA6PcsnmXiCNIAsJP59F7zF9oFcbY
                  |                    du2LsdFGRV3uo3N1x5XnABJDtseDv6Sic4KDnD/WlB/XLzcpEQdiFQqX0E5Z8wP/
                  |                    bZXoSJ47f0SijR1444agXtU1EDIi9rZ77dncaqmBAoGAF2n6MAXaBVL4LczDTW/F
                  |                    pLstcTYfjeN5ezcGUXw2Utp7eSdR2O2snleaPWgGSteRipTl3peZqFOF9p9qpVgf
                  |                    pPh7QWkPQQLmrJRBaLIY/I8PAuasXoSSc4YzQDhnIWMl/awdbNYvrKAltLJVKOEo
                  |                    I8kP0dxpz787fxJw5nCZ3xk="
                  |    }
                  |  ]
                  |}""".stripMargin.replaceAll("\n", " ")

  val options = JWTAuthOptions.fromJson(Json.fromObjectString(config))

  val provider = JWTAuth.create(vertx, options)

  def generateToken(claims: JsonObject): String = {
    val opt = JWTOptions()
      .setAlgorithm("RS256") // for Tests always use asynchronous RS256 algorithm
      .setNoTimestamp(true) // deactivated for testing purposes

    provider.generateToken(claims, opt)
  }
}
