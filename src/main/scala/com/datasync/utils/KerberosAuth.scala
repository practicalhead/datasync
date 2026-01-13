package com.datasync.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.LoggerFactory

/**
 * Kerberos authentication utilities for secure cluster access.
 *
 * Handles:
 * - Keytab-based authentication
 * - UGI (UserGroupInformation) configuration
 * - Cross-realm authentication setup
 */
object KerberosAuth {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Authenticate using a keytab file.
   *
   * @param principal Kerberos principal (e.g., user@REALM.COM)
   * @param keytabPath Path to the keytab file
   */
  def authenticateWithKeytab(principal: String, keytabPath: String): Unit = {
    logger.info(s"Authenticating as $principal using keytab $keytabPath")

    val conf = new Configuration()
    conf.set("hadoop.security.authentication", "kerberos")

    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)

    logger.info(s"Successfully authenticated as ${UserGroupInformation.getCurrentUser.getUserName}")
  }

  /**
   * Check if Kerberos is enabled in the current Hadoop configuration.
   */
  def isKerberosEnabled: Boolean = {
    val conf = new Configuration()
    "kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication", "simple"))
  }

  /**
   * Get the current authenticated user.
   */
  def getCurrentUser: String = {
    UserGroupInformation.getCurrentUser.getUserName
  }

  /**
   * Check and refresh credentials if needed.
   */
  def checkAndRefreshCredentials(): Unit = {
    val ugi = UserGroupInformation.getCurrentUser
    if (ugi.hasKerberosCredentials) {
      ugi.checkTGTAndReloginFromKeytab()
      logger.debug("Kerberos credentials refreshed")
    }
  }

  /**
   * Configure Hadoop for cross-realm Kerberos trust.
   *
   * @param sourceRealm Source cluster Kerberos realm
   * @param targetRealm Target cluster Kerberos realm
   */
  def configureCrossRealmTrust(sourceRealm: String, targetRealm: String): Unit = {
    logger.info(s"Configuring cross-realm trust: $sourceRealm <-> $targetRealm")

    val conf = new Configuration()
    conf.set("hadoop.security.auth_to_local",
      s"""
         |RULE:[1:$$1@$$0](.*@$sourceRealm)s/@.*//
         |RULE:[1:$$1@$$0](.*@$targetRealm)s/@.*//
         |RULE:[2:$$1@$$0](.*@$sourceRealm)s/@.*//
         |RULE:[2:$$1@$$0](.*@$targetRealm)s/@.*//
         |DEFAULT
         |""".stripMargin)

    UserGroupInformation.setConfiguration(conf)
  }

  /**
   * Build JDBC URL with Kerberos authentication parameters.
   *
   * @param baseUrl Base HiveServer2 JDBC URL
   * @param principal HiveServer2 service principal
   * @return JDBC URL with Kerberos authentication
   */
  def buildKerberosJdbcUrl(baseUrl: String, principal: String): String = {
    val separator = if (baseUrl.contains("?")) ";" else ";"
    s"$baseUrl${separator}principal=$principal;auth=kerberos"
  }
}
