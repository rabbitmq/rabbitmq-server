<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:doc="http://www.rabbitmq.com/namespaces/ad-hoc/doc"
                version='1.0'>

<xsl:param name="original"/>

<xsl:output method="xml" doctype-public="bug in xslt processor requires fake doctype" doctype-system="otherwise css isn't included" />

<xsl:template match="*"/>

<!-- Copy every element through -->
<xsl:template match="@*|node()">
  <xsl:copy><xsl:apply-templates select="@*|node()"/></xsl:copy>
</xsl:template>

<!-- Copy the root node, and munge the outer part of the page -->
<xsl:template match="/html">
<xsl:processing-instruction name="xml-stylesheet">type="text/xml" href="page.xsl"</xsl:processing-instruction>
<html xmlns:doc="http://www.rabbitmq.com/namespaces/ad-hoc/doc">
  <head>
    <title><xsl:value-of select="document($original)/refentry/refnamediv/refname"/><xsl:if test="document($original)/refentry/refmeta/manvolnum">(<xsl:value-of select="document($original)/refentry/refmeta/manvolnum"/>)</xsl:if> manual page</title>
  </head>
  <body>
    <doc:div>
      <xsl:choose>
      <xsl:when test="document($original)/refentry/refmeta/manvolnum">
      <p>
        This is the manual page for 
        <code><xsl:value-of select="document($original)/refentry/refnamediv/refname"/>(<xsl:value-of select="document($original)/refentry/refmeta/manvolnum"/>)</code>. 
      </p>
      <p>
        <a href="manpages.html">See a list of all manual pages</a>.
      </p>
      </xsl:when>
      <xsl:otherwise>
      <p>
        This is the documentation for 
        <code><xsl:value-of select="document($original)/refentry/refnamediv/refname"/></code>. 
      </p>
      </xsl:otherwise>
      </xsl:choose>
      <p>
         For more general documentation, please see the 
        <a href="admin-guide.html">administrator's guide</a>.
      </p>

      <doc:toc class="compact">
        <doc:heading>Table of Contents</doc:heading>
      </doc:toc>

      <xsl:apply-templates select="body/div[@class='refentry']"/>
    </doc:div>
  </body>
</html>
</xsl:template>

<!-- Specific instructions to revert the DocBook HTML to be more like our ad-hoc XML schema -->

<xsl:template match="div[@class='refsect1'] | div[@class='refnamediv'] | div[@class='refsynopsisdiv']">
  <doc:section name="{@title}">
    <xsl:apply-templates select="node()"/>
  </doc:section>
</xsl:template>

<xsl:template match="div[@class='refsect2']">
  <doc:subsection name="{@title}">
    <xsl:apply-templates select="node()"/>
  </doc:subsection>
</xsl:template>

<xsl:template match="h2 | h3">
  <doc:heading>
    <xsl:apply-templates select="node()"/>
  </doc:heading>
</xsl:template>

<xsl:template match="pre[@class='screen']">
  <pre class="sourcecode">
    <xsl:apply-templates select="node()"/>
  </pre>
</xsl:template>

<xsl:template match="div[@class='cmdsynopsis']">
  <div class="cmdsynopsis" id="{p/code[@class='command']}">
    <xsl:apply-templates select="node()"/>
  </div>
</xsl:template>

</xsl:stylesheet>

