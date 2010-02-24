<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                xmlns:ng="http://docbook.org/docbook-ng"
                xmlns:db="http://docbook.org/ns/docbook"
                exclude-result-prefixes="exsl ng db"
                version='1.0'>

<xsl:output doctype-public="-//OASIS//DTD DocBook XML V4.5//EN" doctype-system="http://www.docbook.org/xml/4.5/docbookx.dtd" />

<!-- Don't copy exmaples through in place -->
<xsl:template match="*[@role='example-prefix']"/>
<xsl:template match="*[@role='example']"/>

<!-- Copy everything through (with lower priority) -->
<xsl:template match="@*|node()">
  <xsl:copy><xsl:apply-templates select="@*|node()"/></xsl:copy>
</xsl:template>

<!-- Copy the root node, and add examples at the end-->
<xsl:template match="/refentry">
<refentry lang="en">
<xsl:for-each select="*">
  <xsl:copy><xsl:apply-templates select="@*|node()"/></xsl:copy>
</xsl:for-each>
  <refsect1>
    <title>Examples</title>
        <variablelist>
<xsl:for-each select="//screen[@role='example']">
            <varlistentry>
                <term><command><xsl:copy-of select="text()"/></command></term>
                <listitem>
                    <xsl:copy-of select="following-sibling::para[@role='example']"/>
                </listitem>
            </varlistentry>
</xsl:for-each>
        </variablelist>
  </refsect1>
</refentry>
</xsl:template>

</xsl:stylesheet>

