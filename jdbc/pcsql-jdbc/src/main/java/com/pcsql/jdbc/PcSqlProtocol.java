package com.pcsql.jdbc;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
//用 MySQL v10 握手与 COM_QUERY 的包格式，但这是我们自研驱动自己的实现，不依赖外部库
final class PcSqlProtocol {
    // MySQL capability flags (subset)
    static final int CLIENT_LONG_PASSWORD   = 0x00000001;
    static final int CLIENT_LONG_FLAG       = 0x00000004;
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    static final int CLIENT_PROTOCOL_41     = 0x00000200;
    static final int CLIENT_TRANSACTIONS    = 0x00002000;
    static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    static final int CLIENT_PLUGIN_AUTH     = 0x00080000;

    static class Packet {
        byte seq;
        byte[] payload;
    }

    static Packet readPacket(InputStream in) throws IOException {
        byte[] hdr = readFully(in, 4);
        int len = (hdr[0] & 0xFF) | ((hdr[1] & 0xFF) << 8) | ((hdr[2] & 0xFF) << 16);
        byte seq = hdr[3];
        byte[] payload = len > 0 ? readFully(in, len) : new byte[0];
        Packet p = new Packet();
        p.seq = seq;
        p.payload = payload;
        return p;
    }

    static void writePacket(OutputStream out, byte[] payload, byte seq) throws IOException {
        int len = payload.length;
        byte[] hdr = new byte[] { (byte)(len & 0xFF), (byte)((len >>> 8) & 0xFF), (byte)((len >>> 16) & 0xFF), seq };
        out.write(hdr);
        if (len > 0) out.write(payload);
        out.flush();
    }

    static long readLenEncInt(DataInputStream in) throws IOException {
        int first = in.readUnsignedByte();
        if (first < 0xFB) return first;
        if (first == 0xFC) {
            int b0 = in.readUnsignedByte();
            int b1 = in.readUnsignedByte();
            return (b0 | (b1 << 8)) & 0xFFFFL;
        }
        if (first == 0xFD) {
            int b0 = in.readUnsignedByte();
            int b1 = in.readUnsignedByte();
            int b2 = in.readUnsignedByte();
            return (b0 | (b1 << 8) | (b2 << 16)) & 0xFFFFFFL;
        }
        if (first == 0xFE) {
            long v = 0;
            for (int i = 0; i < 8; i++) v |= ((long)in.readUnsignedByte()) << (8 * i);
            return v;
        }
        // 0xFB = NULL, but we should not get here for column count
        return -1;
    }

    static String readLenEncString(DataInputStream in) throws IOException {
        long len = readLenEncInt(in);
        if (len < 0) return null; // NULL
        byte[] buf = new byte[(int)len];
        in.readFully(buf);
        return new String(buf, StandardCharsets.UTF_8);
    }

    static void writeLenEncInt(ByteArrayBuilder b, long v) {
        if (v < 0xFB) {
            b.write((byte)v);
        } else if (v <= 0xFFFF) {
            b.write((byte)0xFC);
            b.write((byte)(v & 0xFF));
            b.write((byte)((v >> 8) & 0xFF));
        } else if (v <= 0xFFFFFF) {
            b.write((byte)0xFD);
            b.write((byte)(v & 0xFF));
            b.write((byte)((v >> 8) & 0xFF));
            b.write((byte)((v >> 16) & 0xFF));
        } else {
            b.write((byte)0xFE);
            for (int i = 0; i < 8; i++) b.write((byte)((v >> (8*i)) & 0xFF));
        }
    }

    static void writeNullTerminated(ByteArrayBuilder b, String s) {
        if (s != null) b.write(s.getBytes(StandardCharsets.UTF_8));
        b.write((byte)0x00);
    }

    static byte[] readFully(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int off = 0;
        while (off < n) {
            int r = in.read(buf, off, n - off);
            if (r < 0) throw new IOException("Unexpected EOF");
            off += r;
        }
        return buf;
    }

    static final class ColumnDef {
        String schema;
        String table;
        String name;
        int type;
    }

    static List<ColumnDef> readColumnDefs(DataInputStream in, int count) throws IOException {
        List<ColumnDef> cols = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // Using lenenc strings per ColumnDefinition41, we only read the name
            String catalog = readLenEncString(in); // "def"
            String schema = readLenEncString(in);
            String table = readLenEncString(in);
            String orgTable = readLenEncString(in);
            String name = readLenEncString(in);
            String orgName = readLenEncString(in);
            int lenOfFixed = in.readUnsignedByte();
            int charset = (in.readUnsignedByte() | (in.readUnsignedByte() << 8));
            long colLen = ((long)in.readUnsignedByte())
                    | ((long)in.readUnsignedByte() << 8)
                    | ((long)in.readUnsignedByte() << 16)
                    | ((long)in.readUnsignedByte() << 24);
            int type = in.readUnsignedByte();
            int flags = (in.readUnsignedByte() | (in.readUnsignedByte() << 8));
            int decimals = in.readUnsignedByte();
            in.readUnsignedByte(); // filler 1
            in.readUnsignedByte(); // filler 2
            ColumnDef c = new ColumnDef();
            c.schema = schema;
            c.table = table;
            c.name = name;
            c.type = type;
            cols.add(c);
        }
        return cols;
    }

    static boolean isEOF(byte[] payload) {
        return payload.length > 0 && (payload[0] & 0xFF) == 0xFE && payload.length <= 5;
    }

    static boolean isERR(byte[] payload) {
        return payload.length > 0 && (payload[0] & 0xFF) == 0xFF;
    }

    static boolean isOK(byte[] payload) {
        return payload.length > 0 && (payload[0] & 0xFF) == 0x00;
    }

    static final class ByteArrayBuilder {
        private byte[] buf = new byte[256];
        private int len = 0;
        void write(byte b) {
            ensure(1); buf[len++] = b;
        }
        void write(byte[] b) {
            ensure(b.length); System.arraycopy(b, 0, buf, len, b.length); len += b.length;
        }
        byte[] toByteArray() {
            byte[] out = new byte[len];
            System.arraycopy(buf, 0, out, 0, len);
            return out;
        }
        private void ensure(int add) {
            int need = len + add;
            if (need > buf.length) {
                int n = Math.max(buf.length << 1, need);
                byte[] nb = new byte[n];
                System.arraycopy(buf, 0, nb, 0, len);
                buf = nb;
            }
        }
    }

    static void performHandshake(Socket socket, String user, String password) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        // read server handshake
        Packet hs = readPacket(in);
        byte seq = (byte)((hs.seq + 1) & 0xFF);
        byte[] p = hs.payload;
        int idx = 0;
        int protoVer = p[idx++] & 0xFF;
        while (idx < p.length && p[idx] != 0) idx++; // server version
        idx++; // null term
        idx += 4; // thread id
        idx += 8; // auth-plugin-data-part-1 (8)
        idx += 1; // filler
        int capsLow = (p[idx++] & 0xFF) | ((p[idx++] & 0xFF) << 8);
        int charset = p[idx++] & 0xFF;
        int status = (p[idx++] & 0xFF) | ((p[idx++] & 0xFF) << 8);
        int capsHigh = (p[idx++] & 0xFF) | ((p[idx++] & 0xFF) << 8);
        int caps = (capsHigh << 16) | capsLow;
        int authDataLen = p[idx++] & 0xFF;
        idx += 10; // reserved
        // read 13 bytes of salt2
        idx += 13;
        idx += 1; // 0x00
        // plugin name till 0
        // Build client response (HandshakeResponse41)
        int clientCaps = 0;
        clientCaps |= CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        PcSqlProtocol.ByteArrayBuilder b = new PcSqlProtocol.ByteArrayBuilder();
        // capabilities
        b.write((byte)(clientCaps & 0xFF));
        b.write((byte)((clientCaps >>> 8) & 0xFF));
        b.write((byte)((clientCaps >>> 16) & 0xFF));
        b.write((byte)((clientCaps >>> 24) & 0xFF));
        // max packet size
        b.write(new byte[] {0,0,0,0});
        // charset: utf8mb4 (45)
        b.write((byte)45);
        // reserved 23
        for (int i = 0; i < 23; i++) b.write((byte)0);
        // username NUL
        writeNullTerminated(b, user == null ? "" : user);
        // auth-response: send empty (server ignores)
        b.write((byte)0);
        // plugin name
        writeNullTerminated(b, "mysql_native_password");
        writePacket(out, b.toByteArray(), seq);
        // read OK/ERR
        Packet resp = readPacket(in);
        if (isERR(resp.payload)) {
            throw new IOException("Server returned ERR during handshake");
        }
    }

    static QueryResult sendQuery(Socket socket, String sql) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();
        // seq resets per command; start from 0
        byte seq = 0;
        ByteArrayBuilder b = new ByteArrayBuilder();
        b.write((byte)0x03); // COM_QUERY
        b.write(sql.getBytes(StandardCharsets.UTF_8));
        writePacket(out, b.toByteArray(), seq);
        DataInputStream din = new DataInputStream(in);
        Packet p0 = readPacket(in);
        if (isERR(p0.payload)) throw new IOException("ERR from server for query");
        if (isOK(p0.payload)) {
            return new QueryResult(new java.util.ArrayList<>(), new java.util.ArrayList<>()); // OK without resultset
        }
        // column count
        DataInputStream c0 = new DataInputStream(new java.io.ByteArrayInputStream(p0.payload));
        int colCount = (int) readLenEncInt(c0);
        // column defs (colCount packets), then EOF
        List<ColumnDef> cols = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            Packet pc = readPacket(in);
            DataInputStream cIn = new DataInputStream(new java.io.ByteArrayInputStream(pc.payload));
            List<ColumnDef> one = readColumnDefs(cIn, 1);
            cols.add(one.get(0));
        }
        Packet eof = readPacket(in); // EOF
        // rows until EOF
        List<List<String>> rows = new ArrayList<>();
        while (true) {
            Packet pr = readPacket(in);
            if (isEOF(pr.payload)) break;
            DataInputStream rin = new DataInputStream(new java.io.ByteArrayInputStream(pr.payload));
            List<String> row = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) {
                String v = readLenEncString(rin);
                row.add(v);
            }
            rows.add(row);
        }
        return new QueryResult(cols, rows);
    }

    static final class QueryResult {
        final List<ColumnDef> columns;
        final List<List<String>> rows;
        QueryResult(List<ColumnDef> c, List<List<String>> r) { this.columns = (c==null? new java.util.ArrayList<>() : c); this.rows = (r==null? new java.util.ArrayList<>() : r); }
    }
}