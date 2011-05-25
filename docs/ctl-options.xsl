<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version='1.0'>
<xsl:output method="text" encoding="UTF-8" />

<xsl:template match="/">
  <xsl:text>%% Generated, do not edit!&#10;</xsl:text>

  <!-- Server status commands -->
  <xsl:text>-define(server_status_commands, [&#10;    </xsl:text>
  <xsl:for-each select="refentry/refsect1/refsect2[title='Server Status']/variablelist/varlistentry">
    <xsl:text>{</xsl:text>
    <xsl:value-of select="term/cmdsynopsis/command"/>
    <xsl:text>,&#10;        [{accepts_vhost, </xsl:text>
    <xsl:value-of select="contains(term/cmdsynopsis/arg/replaceable, 'vhostpath')"/>
    <xsl:text>}],&#10;        [</xsl:text>
    <xsl:for-each select="listitem/variablelist/varlistentry/term">
      <xsl:value-of select="."/>
      <xsl:if test="not(position() = last())">,&#10;         </xsl:if>
    </xsl:for-each>
    <xsl:text>]}</xsl:text>
    <xsl:if test="not(position() = last())">,&#10;    </xsl:if>
  </xsl:for-each>
  <xsl:text>]).</xsl:text>

</xsl:template>
</xsl:stylesheet>
