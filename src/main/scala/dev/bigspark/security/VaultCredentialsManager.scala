package dev.bigspark.security

import com.bettercloud.vault.{Vault, VaultConfig}
import com.bettercloud.vault.response.LogicalResponse

class VaultCredentialsManager(vaultAddress: String, vaultToken: String, vaultPath: String) {
  private val config = new VaultConfig()
    .address(vaultAddress)
    .token(vaultToken)
    .build()

  private[security] val vault = new Vault(config)

  def getCredentials(database: String): (String, String) = {
    try {
      val response: LogicalResponse = vault.logical()
        .read(s"$vaultPath/$database")
      
      val username = response.getData.get("username")
      val password = response.getData.get("password")
      
      (username, password)
    } catch {
      case e: Exception => 
        throw new RuntimeException(s"Failed to retrieve credentials from Vault: ${e.getMessage}")
    }
  }
} 