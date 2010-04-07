<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:exsl="http://exslt.org/common"
                xmlns:ng="http://docbook.org/docbook-ng"
                xmlns:db="http://docbook.org/ns/docbook"
                exclude-result-prefixes="exsl ng db"
                version='1.0'>

<xsl:output doctype-public="-//OASIS//DTD DocBook XML V4.5//EN" doctype-system="http://www.docbook.org/xml/4.5/docbookx.dtd" />

<!-- Don't copy examples through in place -->
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
<xsl:if test="//screen[@role='example']">
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
</xsl:if>
<!--
We need to handle multiline examples separately, since not using a
variablelist leads to slightly less nice formatting (the explanation doesn't get
indented)
-->
<xsl:for-each select="//screen[@role='example-multiline']">
<screen><emphasis role="bold"><xsl:copy-of select="text()"/></emphasis></screen>
<xsl:copy-of select="following-sibling::para[@role='example']"/>
</xsl:for-each>
  </refsect1>
</refentry>
</xsl:template>

<!--
 We show all the subcommands using XML that looks like this:

  <term>
    <cmdsynopsis>
      <command>list_connections</command> 
      <arg choice="opt">
      <replaceable>connectioninfoitem</replaceable>
       ...
      </arg>
    </cmdsynopsis>
  </term>

 However, while DocBook renders this sensibly for HTML, for some reason it
 doen't show anything inside <cmdsynopsis> at all for man pages. I think what
 we're doing is semantically correct so this is a bug in DocBook. The following
 rules essentially do what DocBook does when <cmdsynopsis> is not inside a 
 <term>.
-->

<xsl:template match="term/cmdsynopsis">
  <xsl:apply-templates mode="docbook-bug"/>
</xsl:template>

<xsl:template match="command" mode="docbook-bug">
  <emphasis role="bold"><xsl:apply-templates mode="docbook-bug"/></emphasis>
</xsl:template>

<xsl:template match="arg[@choice='opt']" mode="docbook-bug">
  [<xsl:apply-templates mode="docbook-bug"/>]
</xsl:template>

<xsl:template match="arg[@choice='req']" mode="docbook-bug">
  {<xsl:apply-templates mode="docbook-bug"/>}
</xsl:template>

<xsl:template match="replaceable" mode="docbook-bug">
  <emphasis><xsl:apply-templates mode="docbook-bug"/></emphasis>
</xsl:template>

</xsl:stylesheet>

