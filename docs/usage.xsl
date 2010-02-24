<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                xmlns:ng="http://docbook.org/docbook-ng"
                xmlns:db="http://docbook.org/ns/docbook"
                exclude-result-prefixes="exsl"
                version='1.0'>

<xsl:output method="text"
              encoding="UTF-8"
              indent="no"/>
<xsl:strip-space elements="arg"/> 

<xsl:template match="/">
<!-- Pull out cmdsynopsis to show the command usage line. -->Usage:
<xsl:value-of select="refentry/refsynopsisdiv/cmdsynopsis/command"/> 
<xsl:text> </xsl:text>
<xsl:for-each select="refentry/refsynopsisdiv/cmdsynopsis/arg">
  <xsl:if test="@choice='opt'">[</xsl:if>
  <xsl:apply-templates select="." />
  <xsl:if test="@choice='opt'">]</xsl:if>
  <xsl:text> </xsl:text>
</xsl:for-each>

<!-- List options (any variable list in a section called "Options"). --> 
Options:
<xsl:for-each select=".//*[title='Options']/variablelist">
  <xsl:for-each select="varlistentry">
    <xsl:text>    </xsl:text>
    <xsl:for-each select=".//term">
      <xsl:value-of select="."/>
      <xsl:if test="not(position() = last())">, </xsl:if>
    </xsl:for-each><xsl:text>&#10;</xsl:text>
  </xsl:for-each>
</xsl:for-each>

<!-- List commands (any first-level variable list in a section called "Commands"). --> 
Commands:
<xsl:for-each select=".//*[title='Commands']/refsect2/variablelist">
  <xsl:for-each select="varlistentry">
    <xsl:text>    </xsl:text>
    <xsl:apply-templates select="term"/>
    <xsl:text>&#10;</xsl:text>
  </xsl:for-each>
  <xsl:text>&#10;</xsl:text>
</xsl:for-each>

<!-- Any paragraphs which have been marked as role="usage" (principally for global flags). --> 
<xsl:for-each select="//para[@role='usage']">
<xsl:value-of select="normalize-space(.)"/><xsl:text>&#10;&#10;</xsl:text>
</xsl:for-each>

<!-- Any second-level variable lists (principally for options for subcommands). --> 
<xsl:for-each select=".//*[title='Commands']//variablelist//variablelist">
  <xsl:for-each select="varlistentry">&lt;<xsl:apply-templates select="term"/>&gt; - <xsl:apply-templates select="listitem"/>
      <xsl:text>&#10;</xsl:text>
  </xsl:for-each>
  <xsl:text>&#10;</xsl:text>
</xsl:for-each>

</xsl:template>

<xsl:template match="replaceable">&lt;<xsl:value-of select="normalize-space(.)"/>&gt;</xsl:template>

</xsl:stylesheet>
