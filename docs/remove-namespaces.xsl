<?xml version='1.0'?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:doc="http://www.rabbitmq.com/namespaces/ad-hoc/doc"
                version='1.0'>

<xsl:output method="xml" />

  <!-- Copy every element through with local name only -->
  <xsl:template match="*">
    <xsl:element name="{local-name()}">
      <xsl:apply-templates select="@*|node()"/>
    </xsl:element>
  </xsl:template>

  <!-- Copy every attribute through -->
  <xsl:template match="@*"><xsl:copy/></xsl:template>
</xsl:stylesheet>
