package com.pcsql.jdbc;

import java.io.IOException;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.*;
import java.util.*;
//支持 executeQuery/execute；自动去掉 SQL 前导块注释与末尾分号，并将“values 1 / select 1 from dual”归一到“select 1”
final class MinimalJdbc {

    static Connection open(String url, Properties info) throws IOException {
        String rest = url.substring("jdbc:pcsql://".length());
        String host = rest;
        int port = 40000; // default to server port 40000 instead of 3307
        int slash = rest.indexOf('/');
        if (slash >= 0) host = rest.substring(0, slash);
        int colon = host.indexOf(':');
        if (colon >= 0) { port = Integer.parseInt(host.substring(colon+1)); host = host.substring(0, colon); }
        String user = info.getProperty("user", "");
        String password = info.getProperty("password", "");
        Socket sock = new Socket();
        sock.connect(new InetSocketAddress(host, port), 5000);
        PcSqlProtocol.performHandshake(sock, user, password);
        return createConnection(sock);
    }

    private static Connection createConnection(Socket sock) {
        InvocationHandler h = (proxy, method, args) -> {
            String name = method.getName();
            if (name.equals("createStatement")) {
                return createStatement(sock);
            }
            if (name.equals("close")) { try { sock.close(); } catch (IOException ignored) {} return null; }
            if (name.equals("isClosed")) { return sock.isClosed(); }
            if (name.equals("getAutoCommit")) { return true; }
            if (name.equals("setAutoCommit") || name.equals("commit") || name.equals("rollback")) { return null; }
            if (name.equals("isValid")) { return !sock.isClosed(); }
            if (method.getReturnType().isPrimitive()) {
                if (method.getReturnType() == boolean.class) return false;
                return 0;
            }
            return null;
        };
        return (Connection) Proxy.newProxyInstance(MinimalJdbc.class.getClassLoader(), new Class[]{Connection.class}, h);
    }

    private static String normalizeSql(String sql) {
        if (sql == null) return "";
        String s = sql.trim();
        // strip leading block comments like /* ping */ or /*!...*/
        while (s.startsWith("/*")) {
            int end = s.indexOf("*/");
            if (end < 0) break; // malformed, stop
            s = s.substring(end + 2).trim();
        }
        // remove trailing semicolon
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1).trim();
        String sl = s.toLowerCase(Locale.ROOT);
        if (sl.equals("select 1 from dual")) return "select 1";
        if (sl.equals("values 1")) return "select 1"; // some clients use VALUES 1 as ping
        return s;
    }

    private static Statement createStatement(Socket sock) {
        final Object[] lastRs = new Object[1];
        final int[] lastUpdateCount = new int[]{-1};
        final boolean[] hasResultSet = new boolean[]{false};
        InvocationHandler h = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "executeQuery": {
                    String sql = normalizeSql((String) args[0]);
                    try {
                        PcSqlProtocol.QueryResult qr = PcSqlProtocol.sendQuery(sock, sql);
                        ResultSet rs = createResultSet(qr);
                        lastRs[0] = rs;
                        hasResultSet[0] = true;
                        lastUpdateCount[0] = -1; // result set present
                        return rs;
                    } catch (IOException e) {
                        throw new SQLException("I/O during executeQuery: " + e.getMessage(), e);
                    }
                }
                case "execute": {
                    String sql = normalizeSql((String) args[0]);
                    try {
                        PcSqlProtocol.QueryResult qr = PcSqlProtocol.sendQuery(sock, sql);
                        if (qr.columns != null && !qr.columns.isEmpty()) {
                            lastRs[0] = createResultSet(qr);
                            hasResultSet[0] = true;
                            lastUpdateCount[0] = -1; // per JDBC: when current result is a ResultSet, return -1
                            return true; // has a ResultSet
                        } else {
                            lastRs[0] = null;
                            hasResultSet[0] = false;
                            lastUpdateCount[0] = -1; // no update count to report; signal no more results
                            return false; // OK packet
                        }
                    } catch (IOException e) {
                        throw new SQLException("I/O during execute: " + e.getMessage(), e);
                    }
                }
                case "executeUpdate":
                case "executeLargeUpdate": {
                    throw new SQLFeatureNotSupportedException("Updates not supported");
                }
                case "getResultSet": return (ResultSet) lastRs[0];
                case "getUpdateCount": return lastUpdateCount[0];
                case "getMoreResults": {
                    // No server-side multi-result support; ensure client stops polling
                    lastRs[0] = null;
                    hasResultSet[0] = false;
                    lastUpdateCount[0] = -1;
                    return false;
                }
                case "close": lastRs[0] = null; lastUpdateCount[0] = -1; hasResultSet[0] = false; return null;
            }
            if (method.getReturnType().isPrimitive()) {
                if (method.getReturnType() == boolean.class) return false;
                return 0;
            }
            return null;
        };
        return (Statement) Proxy.newProxyInstance(MinimalJdbc.class.getClassLoader(), new Class[]{Statement.class}, h);
    }

    private static ResultSet createResultSet(PcSqlProtocol.QueryResult qr) {
        ClassLoader cl = MinimalJdbc.class.getClassLoader();
        InvocationHandler h = new InvocationHandler() {
            int row = -1;
            @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                switch (name) {
                    case "next": return ++row < qr.rows.size();
                    case "close": return null;
                    case "getString": {
                        if (args.length == 1) {
                            if (args[0] instanceof Integer) {
                                return qr.rows.get(row).get(((Integer) args[0]) - 1);
                            } else {
                                String label = (String) args[0];
                                int idx = 0; for (PcSqlProtocol.ColumnDef c : qr.columns) { if (label.equalsIgnoreCase(c.name)) return qr.rows.get(row).get(idx); idx++; }
                                return null;
                            }
                        }
                    }
                    case "getMetaData": return createRsMeta(qr);
                }
                if (method.getReturnType().isPrimitive()) {
                    if (method.getReturnType() == boolean.class) return false;
                    return 0;
                }
                return null;
            }
        };
        return (ResultSet) Proxy.newProxyInstance(cl, new Class[]{ResultSet.class}, h);
    }

    private static ResultSetMetaData createRsMeta(PcSqlProtocol.QueryResult qr) {
        ClassLoader cl = MinimalJdbc.class.getClassLoader();
        InvocationHandler h = (proxy, method, args) -> {
            String name = method.getName();
            switch (name) {
                case "getColumnCount": return qr.columns.size();
                case "getColumnLabel": return qr.columns.get(((Integer)args[0]) - 1).name;
                case "getColumnName": return qr.columns.get(((Integer)args[0]) - 1).name;
                case "getColumnType": return java.sql.Types.VARCHAR;
                case "getColumnTypeName": return "VARCHAR";
                case "getTableName": return qr.columns.get(((Integer)args[0]) - 1).table;
                default: {
                    if (method.getReturnType().isPrimitive()) {
                        if (method.getReturnType() == boolean.class) return false;
                        return 0;
                    }
                    return "";
                }
            }
        };
        return (ResultSetMetaData) Proxy.newProxyInstance(cl, new Class[]{ResultSetMetaData.class}, h);
    }
}