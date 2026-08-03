package com.campudus.tableaux.testtools

import com.campudus.tableaux.helper.VertxAccess

import io.vertx.core.Vertx
import io.vertx.ext.auth.{JWTOptions, PubSecKeyOptions}
import io.vertx.ext.auth.jwt.{JWTAuth, JWTAuthOptions}
import org.vertx.scala.core.json.JsonObject

object TokenHelper {

  def apply(vertxAccess: VertxAccess): TokenHelper = {
    new TokenHelper(vertxAccess)
  }
}

class TokenHelper(vertxAccess: VertxAccess) extends VertxAccess {

  override val vertx: Vertx = vertxAccess.vertx

  // Same RSA key pair as conf-test.json's "realm-public-key" (the public half), so tokens generated here validate
  // against the Keycloak config the test suite boots with. vertx-auth-jwt's PubSecKeyOptions now requires a full
  // PEM buffer (BEGIN/END header) rather than the old bare-base64 publicKey/secretKey fields, and a single JWK
  // only ever holds one of the two halves, so the public and private keys are registered as separate PubSecKeys.
  private val publicKeyPem =
    "-----BEGIN PUBLIC KEY-----\n" +
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuuFUzR6uqEU80fNWA88FInoGtUgeQ7JEm1yzh8ii6zP0u+FzezLveJAh" +
      "TO63YtbYddiyZZ+oQcA4ONbyBQLTrtC9X9Nbi/dhaygWFkZYLoNhGbOASrOCOIAStsU2pRfcOt/7WTxV6G/RaugO3/fArfvs/8SZ" +
      "54qS1g2fIHz4jhKepQj/SRxqvhTLSY6cQHEqiToxAVjONV1toLaHWDbVSA1mVZ9hbhdhE07DZaT/YS4EgjgrLTxQohqy7R9pqk6y" +
      "J6TsOcbQfbXVZpKv5BoOV+EtDjlvHxAyHH0Dg2pCK8HSmyEqKaNfG/R/HfYb8JC4tIJunEEhm/1fEt1EkxIZqQIDAQAB\n" +
      "-----END PUBLIC KEY-----\n"

  private val privateKeyPem =
    "-----BEGIN PRIVATE KEY-----\n" +
      "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC64VTNHq6oRTzR81YDzwUiega1SB5DskSbXLOHyKLrM/S74XN7" +
      "Mu94kCFM7rdi1th12LJln6hBwDg41vIFAtOu0L1f01uL92FrKBYWRlgug2EZs4BKs4I4gBK2xTalF9w63/tZPFXob9Fq6A7f98Ct" +
      "++z/xJnnipLWDZ8gfPiOEp6lCP9JHGq+FMtJjpxAcSqJOjEBWM41XW2gtodYNtVIDWZVn2FuF2ETTsNlpP9hLgSCOCstPFCiGrLt" +
      "H2mqTrInpOw5xtB9tdVmkq/kGg5X4S0OOW8fEDIcfQODakIrwdKbISopo18b9H8d9hvwkLi0gm6cQSGb/V8S3USTEhmpAgMBAAEC" +
      "ggEAa/Voa+bht0voStFsS1749GXSIj+7XBhMEgSHolWB6KZnJ3Kip/VQ6jE5S5xMTMkY21uIE7UcGn/U+uERh1uOtlrYS9dp9329" +
      "xY2u1Mdmgdhb6+EKqBzziXhTV0quuskB7PEf3vlAF7shG8Vbcn9JzDjRPSByWJRxJz9PQhFv9YJFvPHkeMwQk5h6Ud/MKIIEBnQd" +
      "8g5Wq41k2EStOJYxm7fL+fawsdxp4eFmRFJSnOzpHPW1F2tzUdSEEtJfAUUhOF3db/R8A6QNuhYRqr5wPp8VEuqgiH9jBAFZdKqE" +
      "jcKRl/ZVAJ9oH54YkEBbkXGW6kAEbIBFj349sQ63x0ZwAQKBgQD1wTHVAykuusP3K20AomdtGg+P5huUWzPmLKzGf1pjfbFxnywF" +
      "ncW4pkavagL7gFAPxnV4cQAtZRotjIXrtMX37Rir/+upv9EM1TIbLzmd8qKOJxhDB2BIRvYor34jO4V03jHIRMoJR/PMnap2voh1" +
      "SgsKKHRjkWxXDM3kv2PsKQKBgQDCq80zxo4YqjBOHGLN8yPq+m4+gV3k1KTqN71xkFMxbsS89PcdHXMW+oT0AHF5FAQh8YLMnJYT" +
      "1wJEJPivZ0r9RKR/9qiE//EpaKVWCHpzm17IG+Z74xu54kNxme8In2zdpz5vLlwoGSWdfy4qTwTTcTw/4ZsfynB8W42LAepxgQKB" +
      "gHwG05JwdPFLeqkcdneSfuYV9/KkrBiUar3ooA3Rqhl6DvqL3Vi8RlQpPpU6yGSLXlyHyTNOvEsssih4ugG6CwtT0lbD4viZgPSc" +
      "CBymGcr38EgTvO/fIh14CrV/1AYN/Q19Mdyjst86O/VxQN2KzS18f9PRlOPHOck5AhRG7zP5AoGBALWZ8W64bnyB31gu0NlRVZNy" +
      "FYAHzOCYolPAteCIA6PcsnmXiCNIAsJP59F7zF9oFcbYdu2LsdFGRV3uo3N1x5XnABJDtseDv6Sic4KDnD/WlB/XLzcpEQdiFQqX" +
      "0E5Z8wP/bZXoSJ47f0SijR1444agXtU1EDIi9rZ77dncaqmBAoGAF2n6MAXaBVL4LczDTW/FpLstcTYfjeN5ezcGUXw2Utp7eSdR" +
      "2O2snleaPWgGSteRipTl3peZqFOF9p9qpVgfpPh7QWkPQQLmrJRBaLIY/I8PAuasXoSSc4YzQDhnIWMl/awdbNYvrKAltLJVKOEo" +
      "I8kP0dxpz787fxJw5nCZ3xk=\n" +
      "-----END PRIVATE KEY-----\n"

  val options = new JWTAuthOptions()
    .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(publicKeyPem))
    .addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(privateKeyPem))

  val provider = JWTAuth.create(vertx, options)

  def generateToken(claims: JsonObject, jwtOptionsOpt: Option[JWTOptions] = None): String = {
    val opt = new JWTOptions()
      .setAlgorithm("RS256") // for Tests always use asynchronous RS256 algorithm
      .setNoTimestamp(true) // deactivated for testing purposes

    provider.generateToken(claims, jwtOptionsOpt.getOrElse(opt))
  }
}
