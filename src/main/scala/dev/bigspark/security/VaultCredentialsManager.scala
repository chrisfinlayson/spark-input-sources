package dev.bigspark.security

import com.bettercloud.vault.VaultConfig
import com.bettercloud.vault.Vault
import com.bettercloud.vault.VaultException
import com.typesafe.scalalogging.LazyLogging
import scala.util.Try
import scala.concurrent.duration._
import scala.collection.JavaConverters._

class VaultCredentialsManager(
    vaultAddress: String,
    vaultToken: String,
    vaultPath: String,
    retryAttempts: Int = 3,
    cacheDuration: Duration = 1.hour
) extends LazyLogging {

  require(vaultAddress.nonEmpty, "Vault address cannot be empty")
  require(vaultToken.nonEmpty, "Vault token cannot be empty")
  require(vaultPath.nonEmpty, "Vault path cannot be empty")

  case class DatabaseCredentials(user: String, password: String)
  case class VaultCredentialsException(message: String, cause: Throwable = null) 
    extends Exception(message, cause)

  private case class CachedCredentials(
    credentials: DatabaseCredentials,
    expiresAt: Long
  )

  private var credentialsCache = Map[String, CachedCredentials]()

  private val config = new VaultConfig()
    .address(vaultAddress)
    .token(vaultToken)
    .build()

  private val vault = new Vault(config)

  def getCredentials(dbType: String): DatabaseCredentials = {
    getCachedCredentials(dbType).getOrElse {
      val credentials = getCredentialsWithRetry(dbType)
      cacheCredentials(dbType, credentials)
      credentials
    }
  }

  private def getCachedCredentials(dbType: String): Option[DatabaseCredentials] = {
    credentialsCache.get(dbType).flatMap { cached =>
      if (System.currentTimeMillis() < cached.expiresAt) {
        Some(cached.credentials)
      } else {
        credentialsCache -= dbType
        None
      }
    }
  }

  private def cacheCredentials(dbType: String, credentials: DatabaseCredentials): Unit = {
    credentialsCache += dbType -> CachedCredentials(
      credentials,
      System.currentTimeMillis() + cacheDuration.toMillis
    )
  }

  private def getCredentialsWithRetry(dbType: String): DatabaseCredentials = {
    Try {
      (1 to retryAttempts).foldLeft[Option[DatabaseCredentials]](None) { (result, attempt) =>
        result.orElse {
          try {
            Some(fetchCredentials(dbType))
          } catch {
            case e: Exception =>
              logger.warn(s"Attempt $attempt failed: ${e.getMessage}")
              if (attempt == retryAttempts) throw e
              Thread.sleep(1000 * attempt)
              None
          }
        }
      }.get
    }.getOrElse(throw VaultCredentialsException(s"Failed to retrieve credentials after $retryAttempts attempts"))
  }

  private def fetchCredentials(dbType: String): DatabaseCredentials = {
    try {
      val path = s"$vaultPath/data/$dbType-db"
      logger.debug(s"Fetching credentials from path: $path")
      
      val response = vault.logical()
        .read(path)
        .getData

      val responseMap = Option(response).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
      
      val data = responseMap.getOrElse("data", throw VaultCredentialsException(
        s"No credentials found for database type: $dbType"
      ))

      val dataMap = Option(data.asInstanceOf[java.util.Map[String, String]])
        .map(_.asScala.toMap)
        .getOrElse(Map.empty[String, String])

      if (!dataMap.contains("user") || !dataMap.contains("password")) {
        throw VaultCredentialsException(s"Invalid credential format for database type: $dbType")
      }

      DatabaseCredentials(
        user = dataMap("user"),
        password = dataMap("password")
      )
    } catch {
      case e: VaultException =>
        throw VaultCredentialsException(s"Failed to retrieve credentials from Vault: ${e.getMessage}", e)
      case e: Exception =>
        throw VaultCredentialsException(s"Unexpected error while retrieving credentials: ${e.getMessage}", e)
    }
  }
} 