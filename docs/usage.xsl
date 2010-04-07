<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                xmlns:ng="http://docbook.org/docbook-ng"
                xmlns:db="http://docbook.org/ns/docbook"
                exclude-result-prefixes="exsl"
                version='1.0'>

<xsl:param name="modulename"/>

<xsl:output method="text"
              encoding="UTF-8"
              indent="no"/>
<xsl:strip-space elements="*"/> 
<xsl:preserve-space elements="cmdsynopsis arg" />

<xsl:template match="/">
<!-- Pull out cmdsynopsis to show the command usage line. -->%% Generated, do not edit!
-module(<xsl:value-of select="$modulename" />).
-export([usage/0]).
usage() -> %QUOTE%Usage:
<xsl:value-of select="refentry/refsynopsisdiv/cmdsynopsis/command"/> 
<xsl:text> </xsl:text>
<xsl:for-each select="refentry/refsynopsisdiv/cmdsynopsis/arg">
  <xsl:apply-templates select="." />
  <xsl:text> </xsl:text>
</xsl:for-each>

<xsl:text>&#10;</xsl:text>

<!-- List options (any variable list in a section called "Options"). --> 
<xsl:for-each select=".//*[title='Options']/variablelist">
  <xsl:if test="position() = 1">&#10;Options:&#10;</xsl:if>
  <xsl:for-each select="varlistentry">
    <xsl:text>    </xsl:text>
    <xsl:for-each select=".//term">
      <xsl:value-of select="."/>
      <xsl:if test="not(position() = last())">, </xsl:if>
    </xsl:for-each><xsl:text>&#10;</xsl:text>
  </xsl:for-each>
</xsl:for-each>

<!-- Any paragraphs which have been marked as role="usage" (principally for global flags). --> 
<xsl:text>&#10;</xsl:text>
<xsl:for-each select=".//*[title='Options']//para[@role='usage']">
<xsl:value-of select="normalize-space(.)"/><xsl:text>&#10;&#10;</xsl:text>
</xsl:for-each>

<!-- List commands (any first-level variable list in a section called "Commands"). --> 
<xsl:for-each select=".//*[title='Commands']/variablelist | .//*[title='Commands']/refsect2/variablelist">
  <xsl:if test="position() = 1">Commands:&#10;</xsl:if>
  <xsl:for-each select="varlistentry">
    <xsl:text>    </xsl:text>
    <xsl:apply-templates select="term"/>
    <xsl:text>&#10;</xsl:text>
  </xsl:for-each>
  <xsl:text>&#10;</xsl:text>
</xsl:for-each>

<xsl:apply-templates select=".//*[title='Commands']/refsect2" mode="command-usage" />
%QUOTE%.
</xsl:template>

<!-- Option lists in command usage -->
<xsl:template match="varlistentry[@role='usage-has-option-list']" mode="command-usage">&lt;<xsl:value-of select="term/cmdsynopsis/arg[@role='usage-option-list']/replaceable"/>&gt; must be a member of the list [<xsl:for-each select="listitem/variablelist/varlistentry"><xsl:apply-templates select="term"/><xsl:if test="not(position() = last())">, </xsl:if></xsl:for-each>].<xsl:text>&#10;&#10;</xsl:text></xsl:template>

<!-- Usage paras in command usage -->
<xsl:template match="para[@role='usage']" mode="command-usage">
<xsl:value-of select="normalize-space(.)"/><xsl:text>&#10;&#10;</xsl:text>
</xsl:template>

<!-- Don't show anything else in command usage -->
<xsl:template match="text()" mode="command-usage"/>

<xsl:template match="arg[@choice='opt']">[<xsl:apply-templates/>]</xsl:template>
<xsl:template match="replaceable">&lt;<xsl:value-of select="."/>&gt;</xsl:template>

</xsl:stylesheet>
