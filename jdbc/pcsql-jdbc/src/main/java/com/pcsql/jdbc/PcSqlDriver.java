package com.pcsql.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
//注册 JDBC 驱动，接受 jdbc:pcsql:// 前缀并建立连接
public final class PcSqlDriver implements Driver {
    static {
        try { DriverManager.registerDriver(new PcSqlDriver()); } catch (SQLException e) { throw new ExceptionInInitializerError(e); }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:pcsql://");
        }

    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        try {
            return MinimalJdbc.open(url, info);
        } catch (Exception e) {
            throw new SQLException("Failed to connect: " + e.getMessage(), e);
        }
    }

    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public java.util.logging.Logger getParentLogger() { return java.util.logging.Logger.getLogger("com.pcsql.jdbc"); }
    @Override public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new java.sql.DriverPropertyInfo[0]; }
}