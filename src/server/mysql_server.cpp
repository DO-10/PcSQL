#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <random>
#include <cstdlib>
#include <signal.h>
#include <atomic>
#include <errno.h>
#include <sys/select.h>
#include <cctype>

#include "storage/storage_engine.hpp"
#include "execution/execution_engine.h"
#include "compiler/compiler.h"

using namespace pcsql;

namespace {
// -------- Utils --------
static inline std::string to_lower(std::string s){ for(auto &c:s) c=(char)std::tolower((unsigned char)c); return s; }
static inline std::vector<std::string> split(const std::string& s, char delim){ std::vector<std::string> out; std::string cur; std::istringstream iss(s); while(std::getline(iss,cur,delim)) out.push_back(cur); return out; }

// simple trim
static inline std::string trim(const std::string& s){ size_t b=0,e=s.size(); while(b<e && std::isspace((unsigned char)s[b])) ++b; while(e>b && std::isspace((unsigned char)s[e-1])) --e; return s.substr(b,e-b); }

// length-encoded integer write
static void lenc_int(std::vector<uint8_t>& b, uint64_t v){
    if (v < 251) { b.push_back((uint8_t)v); return; }
    else if (v < (1ULL<<16)) { b.push_back(0xFC); b.push_back((uint8_t)(v & 0xFF)); b.push_back((uint8_t)((v>>8)&0xFF)); }
    else if (v < (1ULL<<24)) { b.push_back(0xFD); b.push_back((uint8_t)(v & 0xFF)); b.push_back((uint8_t)((v>>8)&0xFF)); b.push_back((uint8_t)((v>>16)&0xFF)); }
    else { b.push_back(0xFE); for(int i=0;i<8;++i) b.push_back((uint8_t)((v>>(8*i))&0xFF)); }
}

static void lenc_str(std::vector<uint8_t>& b, const std::string& s){ lenc_int(b, s.size()); b.insert(b.end(), s.begin(), s.end()); }

static void put_int2(std::vector<uint8_t>& b, uint16_t v){ b.push_back((uint8_t)(v & 0xFF)); b.push_back((uint8_t)((v>>8)&0xFF)); }
static void put_int3(std::vector<uint8_t>& b, uint32_t v){ b.push_back((uint8_t)(v & 0xFF)); b.push_back((uint8_t)((v>>8)&0xFF)); b.push_back((uint8_t)((v>>16)&0xFF)); }
static void put_int4(std::vector<uint8_t>& b, uint32_t v){ for(int i=0;i<4;++i) b.push_back((uint8_t)((v>>(8*i))&0xFF)); }

static int mysql_type_from(DataType t){
    // MySQL protocol type codes (subset)
    // TINY=1, LONG=3, FLOAT=4, DOUBLE=5, VAR_STRING=253
    switch(t){
        case DataType::INT: return 3;      // MYSQL_TYPE_LONG
        case DataType::DOUBLE: return 5;   // MYSQL_TYPE_DOUBLE
        case DataType::BOOLEAN: return 1;  // MYSQL_TYPE_TINY
        case DataType::VARCHAR: return 253; // MYSQL_TYPE_VAR_STRING
        default: return 253;
    }
}

// duplicate trim removed (use the inline trim defined above)
// removed duplicate trim (use the inline trim defined above)

// strip leading block comments like /* ... */ (including MySQL /*! ... */ hints)
static std::string strip_leading_block_comments(const std::string& in){
    std::string s = trim(in);
    while (s.size() >= 2 && s[0]=='/' && s[1]=='*'){
        size_t pos = s.find("*/", 2);
        if (pos == std::string::npos) break;
        s = trim(s.substr(pos+2));
    }
    return s;
}

// Normalize SQL for simple keep-alive patterns
static std::string normalize_sql(const std::string& in){
    std::string s = strip_leading_block_comments(in);
    // remove trailing semicolon
    if (!s.empty() && s.back() == ';') s.pop_back();
    s = trim(s);
    std::string sl = to_lower(s);
    if (sl == "select 1 from dual") return "select 1";
    if (sl == "values 1" || sl == "values(1)") return "select 1";
    // normalize IDE keep-alive like SELECT 'keep alive' -> SELECT 1
    if (sl == "select 'keep alive'" || sl == "select \"keep alive\"") return "select 1";
    return s;
}

// Removed server-side WHERE helpers (now handled by ExecutionEngine)
// parse_condition, to_bool_ci, compare_typed

// write a packet with header (3-byte length + 1-byte seq)
static bool write_packet(int fd, uint8_t& seq, const std::vector<uint8_t>& payload){
    uint32_t len = (uint32_t)payload.size();
    uint8_t hdr[4] = { (uint8_t)(len & 0xFF), (uint8_t)((len>>8)&0xFF), (uint8_t)((len>>16)&0xFF), seq++ };
    if (send(fd, hdr, 4, 0) != 4) return false;
    if (len>0 && send(fd, payload.data(), len, 0) != (ssize_t)len) return false;
    return true;
}

static bool read_fully(int fd, void* buf, size_t n){ char* p=(char*)buf; size_t r=0; while(r<n){ ssize_t k=recv(fd,p+r,n-r,0); if(k<=0) return false; r+=k; } return true; }

static bool read_packet(int fd, uint8_t& seq, std::vector<uint8_t>& out){
    uint8_t hdr[4];
    if(!read_fully(fd,hdr,4)){
        std::cerr << "[MySQLCompat] read_packet: failed to read header (EOF or socket error)" << std::endl;
        return false;
    }
    uint32_t len = (uint32_t)hdr[0] | ((uint32_t)hdr[1]<<8) | ((uint32_t)hdr[2]<<16);
    seq = hdr[3]+1;
    out.resize(len);
    if(len>0 && !read_fully(fd,out.data(),len)){
        std::cerr << "[MySQLCompat] read_packet: failed to read payload, len=" << len << std::endl;
        return false;
    }
    return true;
}

// OK packet (pre-5.7 EOF style OK)
static std::vector<uint8_t> make_ok(uint64_t affected=0, uint16_t status=0x0002){
    std::vector<uint8_t> p; p.push_back(0x00); lenc_int(p, affected); lenc_int(p, 0); // last_insert_id=0
    put_int2(p, status); put_int2(p, 0); // warnings
    return p;
}

static std::vector<uint8_t> make_err(uint16_t code, const std::string& msg){
    std::vector<uint8_t> p; p.push_back(0xFF); put_int2(p, code); p.push_back('#'); std::string sqlstate = "HY000"; p.insert(p.end(), sqlstate.begin(), sqlstate.end()); p.insert(p.end(), msg.begin(), msg.end()); return p;
}

static std::vector<uint8_t> make_eof(uint16_t status=0x0002){ std::vector<uint8_t> p; p.push_back(0xFE); put_int2(p, 0); put_int2(p, status); return p; }

// ColumnDefinition41
static std::vector<uint8_t> make_coldef(const std::string& table, const std::string& name, int type){
    std::vector<uint8_t> p; lenc_str(p, "def"); lenc_str(p, ""); lenc_str(p, table); lenc_str(p, ""); lenc_str(p, name); lenc_str(p, name);
    p.push_back(0x0C); // length of fixed fields
    put_int2(p, 33);   // character set (utf8_general_ci)
    put_int4(p, 1024); // column length
    p.push_back((uint8_t)type);
    put_int2(p, 0);    // flags
    p.push_back(0);    // decimals
    put_int2(p, 0);    // filler
    return p;
}

static std::vector<uint8_t> make_text_row(const std::vector<std::string>& fields){
    std::vector<uint8_t> p; for(const auto& f: fields){ lenc_str(p, f); } return p;
}

} // namespace

// graceful shutdown flag/handler at file-scope
static std::atomic<bool> g_stop{false};
static void on_signal(int /*sig*/) { g_stop.store(true); }

class MySQLServer {
public:
    //构造函数，初始化存储引擎和执行引擎。
    MySQLServer()
        : storage_("./storage_data", 64, Policy::LRU, true), exec_(storage_) {
        // 支持通过环境变量默认开启索引跟踪：PCSQL_INDEX_TRACE=1|on|true|yes
        if (const char* env = std::getenv("PCSQL_INDEX_TRACE")) {
            std::string v = to_lower(std::string(env));
            if (v == "1" || v == "on" || v == "true" || v == "yes") {
                storage_.set_index_trace(true);
                std::cout << "[MySQLCompat] PCSQL_INDEX_TRACE enabled by env" << std::endl;
            }
        }
    }

    int run(uint16_t port = 3307){
        int srv = socket(AF_INET, SOCK_STREAM, 0); if(srv<0){ perror("socket"); return 1; }
        int opt=1; setsockopt(srv,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
        sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_addr.s_addr=INADDR_ANY; addr.sin_port=htons(port);
        if(bind(srv,(sockaddr*)&addr,sizeof(addr))<0){ perror("bind"); close(srv); return 1; }
        if(listen(srv, 16)<0){ perror("listen"); close(srv); return 1; }
        std::cout << "PcSQL MySQL-compatible server listening on 0.0.0.0:" << port << std::endl;
        // accept loop with graceful shutdown on g_stop
        while(!g_stop.load()){
            fd_set rfds; FD_ZERO(&rfds); FD_SET(srv, &rfds);
            timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0; // 1s tick
            int sel = select(srv+1, &rfds, nullptr, nullptr, &tv);
            if (sel < 0) {
                if (errno == EINTR) continue; // interrupted by signal
                perror("select"); break;
            }
            if (sel == 0) continue; // timeout, check g_stop again
            sockaddr_in cli{}; socklen_t cl=sizeof(cli);
            int fd = accept(srv,(sockaddr*)&cli,&cl);
            if(fd<0){
                if (errno == EINTR || errno == EAGAIN) continue;
                perror("accept"); continue;
            }
            handle_client(fd);
            close(fd);
        }
        close(srv);
        std::cout << "PcSQL server shutting down (SIGINT/SIGTERM)" << std::endl;
        return 0;
    }

private:
    void handle_client(int fd){
        uint8_t seq = 0;
        std::cout << "[MySQLCompat] Client connected" << std::endl;
        // handshake v10
        std::vector<uint8_t> p;
        p.push_back(0x0A); // protocol version
        std::string svr = "PcSQL-MySQL-Compat 0.1"; p.insert(p.end(), svr.begin(), svr.end()); p.push_back(0x00);
        uint32_t thread_id = 1234; put_int4(p, thread_id);
        std::string salt1 = "abcdefgh"; p.insert(p.end(), salt1.begin(), salt1.end()); p.push_back(0x00);
        // Correct capability flags: include CLIENT_SECURE_CONNECTION (0x8000) and CLIENT_PROTOCOL_41 (0x0200)
        uint32_t caps = 0;
        caps |= 0x00000001; // CLIENT_LONG_PASSWORD
        caps |= 0x00000004; // CLIENT_LONG_FLAG
        caps |= 0x00000008; // CLIENT_CONNECT_WITH_DB
        caps |= 0x00000200; // CLIENT_PROTOCOL_41
        caps |= 0x00002000; // CLIENT_TRANSACTIONS
        caps |= 0x00008000; // CLIENT_SECURE_CONNECTION
        caps |= 0x00080000; // CLIENT_PLUGIN_AUTH
        put_int2(p, (uint16_t)(caps & 0xFFFF));
        p.push_back(45); // character set id (utf8mb4_general_ci)
        put_int2(p, 0x0002); // status AUTOCOMMIT
        put_int2(p, (uint16_t)((caps >> 16) & 0xFFFF)); // capability flags upper 2 bytes
        p.push_back(21); // auth plugin data len (20 bytes scramble + 1 terminator)
        for(int i=0;i<10;++i) p.push_back(0x00); // reserved 10 bytes
        // auth-plugin-data-part-2 must be at least 13 bytes
        std::string salt2 = "ijklmnopqrstuv"; // base seed
        if (salt2.size() < 13) salt2.append(13 - salt2.size(), 'x');
        if (salt2.size() > 13) salt2.resize(13);
        p.insert(p.end(), salt2.begin(), salt2.end()); // 13 bytes
        p.push_back(0x00);
        std::string plugin = "mysql_native_password"; p.insert(p.end(), plugin.begin(), plugin.end()); p.push_back(0x00);
        if(!write_packet(fd, seq, p)) return;

        // read client response (handshake), responsive to shutdown
        for(;;){
            if (g_stop.load()) return;
            fd_set rfds; FD_ZERO(&rfds); FD_SET(fd, &rfds);
            timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0;
            int sel = select(fd+1, &rfds, nullptr, nullptr, &tv);
            if (sel < 0) { if (errno == EINTR) continue; perror("select handshake"); return; }
            if (sel == 0) continue; // timeout
            break;
        }
        std::vector<uint8_t> resp; if(!read_packet(fd, seq, resp)) return;
        if(resp.size() >= 4){
            uint32_t cc = (uint32_t)resp[0] | ((uint32_t)resp[1]<<8) | ((uint32_t)resp[2]<<16) | ((uint32_t)resp[3]<<24);
            std::cout << "[MySQLCompat] Handshake Response: client caps=0x" << std::hex << cc << std::dec << ", size=" << resp.size() << std::endl;
        } else {
            std::cout << "[MySQLCompat] Handshake Response: size=" << resp.size() << std::endl;
        }
        // We accept any user/pass/db for now.
        // send OK
        auto ok = make_ok(); if(!write_packet(fd, seq, ok)) return;

        // command loop (responsive to shutdown)
        for(;;){
            if (g_stop.load()) return;
            fd_set rfds; FD_ZERO(&rfds); FD_SET(fd, &rfds);
            timeval tv{}; tv.tv_sec = 1; tv.tv_usec = 0; // 1s tick
            int sel = select(fd+1, &rfds, nullptr, nullptr, &tv);
            if (sel < 0) { if (errno == EINTR) continue; perror("select cmd"); return; }
            if (sel == 0) continue; // timeout; check g_stop again
            std::vector<uint8_t> cmdpkt; if(!read_packet(fd, seq, cmdpkt)) return; if(cmdpkt.empty()) return;
            uint8_t cmd = cmdpkt[0];
            std::cout << "[MySQLCompat] Command: 0x" << std::hex << (int)cmd << std::dec << ", payload_len=" << (cmdpkt.size() ? (cmdpkt.size()-1) : 0) << std::endl;
            // Reset sequence id for new server response (MySQL convention)
            seq = 1;
            if(cmd == 0x01 /*COM_QUIT*/){ return; }
            if(cmd == 0x0E /*COM_PING*/){ auto okp = make_ok(); if(!write_packet(fd, seq, okp)){ std::cerr << "[MySQLCompat] Failed to send PING OK" << std::endl; } continue; }
            if(cmd == 0x03 /*COM_QUERY*/){
                std::string sql((const char*)&cmdpkt[1], cmdpkt.size()-1);
                handle_query(fd, seq, sql);
                continue;
            }
            // unknown command -> ERR
            auto err = make_err(1064, "Unknown command"); if(!write_packet(fd, seq, err)){ std::cerr << "[MySQLCompat] Failed to send ERR for unknown cmd" << std::endl; }
        }
    }

    void handle_query(int fd, uint8_t& seq, std::string sql){
        std::string ns = normalize_sql(sql);
        std::string usql = to_lower(ns);
        std::cout << "[MySQLCompat] COM_QUERY: raw='" << sql << "' | normalized='" << ns << "'" << std::endl;
        // Server-side shutdown commands (case-insensitive): SHUTDOWN | QUIT | EXIT
        // Note: mysql 客户端中的 quit/exit 往往是客户端本地命令，不会以 COM_QUERY 发送；
        // 建议使用 SHUTDOWN; 来关闭服务器。
        if (usql == "shutdown" || usql == "quit" || usql == "exit" || usql.rfind("shutdown ", 0) == 0) {
            g_stop.store(true);
            auto ok = make_ok();
            if(!write_packet(fd, seq, ok)) { std::cerr << "[MySQLCompat] Failed to send OK for SHUTDOWN" << std::endl; }
            std::cout << "[MySQLCompat] Shutdown requested by client via SQL ('" << ns << "')" << std::endl;
            return;
        }
        if(usql.rfind("set ",0)==0){
            // 支持运行时开启/关闭索引跟踪：
            //   SET pcsql_index_trace=ON; / OFF;  (大小写不敏感，支持 1/0/true/false/yes/no)
            //   也兼容 SET @@pcsql_index_trace=1;
            std::string after = ns.substr(4); // strip leading 'SET '
            after = trim(after);
            while(!after.empty() && after[0]=='@'){ after.erase(after.begin()); }
            after = trim(after);
            auto pos = after.find('=');
            if (pos != std::string::npos) {
                std::string key = to_lower(trim(after.substr(0, pos)));
                std::string val = to_lower(trim(after.substr(pos+1)));
                if (key == "pcsql_index_trace" || key == "index_trace") {
                    bool on = (val == "1" || val == "on" || val == "true" || val == "yes");
                    storage_.set_index_trace(on);
                    std::cout << "[MySQLCompat] set index_trace=" << (on?"on":"off") << std::endl;
                    auto ok = make_ok(); if(!write_packet(fd, seq, ok)){ std::cerr << "[MySQLCompat] Failed to send OK for SET index_trace" << std::endl; }
                    return;
                }
            }
            auto ok = make_ok(); if(!write_packet(fd, seq, ok)){ std::cerr << "[MySQLCompat] Failed to send OK for SET" << std::endl; } return; }
        if(usql=="select 1" || usql=="select 1;"){
            // one-column, one-row result
            std::vector<uint8_t> pcols; lenc_int(pcols, 1); if(!write_packet(fd, seq, pcols)) { std::cerr << "[MySQLCompat] Failed to send column-count for select 1" << std::endl; return; }
            auto col = make_coldef("", "1", 3); if(!write_packet(fd, seq, col)) { std::cerr << "[MySQLCompat] Failed to send coldef for select 1" << std::endl; return; }
            auto eof = make_eof(); if(!write_packet(fd, seq, eof)) { std::cerr << "[MySQLCompat] Failed to send EOF for select 1" << std::endl; return; }
            std::vector<uint8_t> row = make_text_row({"1"}); if(!write_packet(fd, seq, row)) { std::cerr << "[MySQLCompat] Failed to send row for select 1" << std::endl; return; }
            auto eof2 = make_eof(); if(!write_packet(fd, seq, eof2)) { std::cerr << "[MySQLCompat] Failed to send EOF2 for select 1" << std::endl; } return;
        }
        if(usql.rfind("select @@version",0)==0 || usql.find("select version()")!=std::string::npos){
            std::vector<uint8_t> pcols; lenc_int(pcols, 1); if(!write_packet(fd, seq, pcols)) { std::cerr << "[MySQLCompat] Failed to send column-count for version" << std::endl; return; }
            auto col = make_coldef("", "version()", 253); if(!write_packet(fd, seq, col)) { std::cerr << "[MySQLCompat] Failed to send coldef for version" << std::endl; return; }
            auto eof = make_eof(); if(!write_packet(fd, seq, eof)) { std::cerr << "[MySQLCompat] Failed to send EOF for version" << std::endl; return; }
            std::vector<uint8_t> row = make_text_row({"PcSQL 1.0.0"}); if(!write_packet(fd, seq, row)) { std::cerr << "[MySQLCompat] Failed to send row for version" << std::endl; return; }
            auto eof2 = make_eof(); if(!write_packet(fd, seq, eof2)) { std::cerr << "[MySQLCompat] Failed to send EOF2 for version" << std::endl; } return;
        }
        if(usql.rfind("show ",0)==0){ // naive OK to bypass client checks
            auto ok = make_ok(); if(!write_packet(fd, seq, ok)){ std::cerr << "[MySQLCompat] Failed to send OK for SHOW" << std::endl; } return; }

        // --- Minimal CREATE INDEX support (server-side quick parser) ---
        if (usql.rfind("create index", 0) == 0 || usql.rfind("create unique index", 0) == 0) {
            bool unique = (usql.rfind("create unique index", 0) == 0);
            // remove trailing semicolon in the original-normalized SQL
            std::string s = ns;
            if (!s.empty() && s.back() == ';') s.pop_back();
            // expected forms:
            //   CREATE INDEX idx_name ON table_name(col_name)
            //   CREATE UNIQUE INDEX idx_name ON table_name(col_name)
            auto lower = to_lower(s);
            auto err_out = [&](const std::string& m){ auto err = make_err(1064, m); if(!write_packet(fd, seq, err)){ std::cerr << "[MySQLCompat] Failed to send ERR for CREATE INDEX" << std::endl; } };

            size_t kw_pos = lower.find("create");
            size_t idx_kw = lower.find(" index ", kw_pos);
            if (idx_kw == std::string::npos) idx_kw = lower.find(" index", kw_pos);
            size_t on_pos = lower.find(" on ", (idx_kw==std::string::npos? kw_pos : idx_kw));
            if (kw_pos == std::string::npos || on_pos == std::string::npos) { err_out("Malformed CREATE INDEX"); return; }

            size_t name_start = lower.find("index", kw_pos);
            if (name_start == std::string::npos) { err_out("Missing INDEX keyword"); return; }
            name_start += 5; // skip 'index'
            while (name_start < s.size() && std::isspace((unsigned char)s[name_start])) ++name_start;
            std::string index_name = trim(s.substr(name_start, on_pos - name_start));

            size_t paren_l = s.find('(', on_pos + 4);
            size_t paren_r = s.find(')', (paren_l == std::string::npos ? on_pos + 4 : paren_l));
            if (paren_l == std::string::npos || paren_r == std::string::npos || paren_r <= paren_l) { err_out("Missing or invalid column list"); return; }
            std::string table_name = trim(s.substr(on_pos + 4, paren_l - (on_pos + 4)));
            std::string column_name = trim(s.substr(paren_l + 1, paren_r - (paren_l + 1)));

            auto dequote = [&](const std::string& x){ std::string y; y.reserve(x.size()); for(char c: x){ if(c!='`' && c!='"' && c!='\''){ y.push_back(c); } } return trim(y); };
            index_name = dequote(index_name);
            table_name = dequote(table_name);
            column_name = dequote(column_name);

            try{
                std::cout << "[MySQLCompat] CREATE " << (unique?"UNIQUE ":"") << "INDEX request: index='" << index_name
                          << "' on " << table_name << "(" << column_name << ")" << std::endl;
                bool okb = storage_.create_index(index_name, table_name, column_name, unique);
                if (okb) {
                    auto ok = make_ok(); if(!write_packet(fd, seq, ok)){ std::cerr << "[MySQLCompat] Failed to send OK for CREATE INDEX" << std::endl; }
                } else {
                    err_out("CREATE INDEX failed");
                }
            } catch(const std::exception& e){
                err_out(std::string("CREATE INDEX error: ")+e.what());
            }
            return;
        }

        try{
            // 在进入编译阶段前，确保语句以分号结尾
            std::string compileSql = ns;
            if (!compileSql.empty() && compileSql.back() != ';') {
                compileSql.push_back(';');
            }
            Compiler compiler; auto unit = compiler.compile(compileSql, storage_);
            if (auto* s = dynamic_cast<SelectStatement*>(unit.ast.get())){
                // Use execution engine to build result rows (with WHERE and index pushdown)
                int tid = storage_.get_table_id(to_lower(s->fromTable));
                if(tid < 0){ auto err = make_err(1146, "Table not found: "+s->fromTable); if(!write_packet(fd, seq, err)){ std::cerr << "[MySQLCompat] Failed to send ERR for table not found" << std::endl; } return; }
                const auto& schema = storage_.get_table_schema(to_lower(s->fromTable));

                auto rows = exec_.selectRows(s);

                // columns list: if not matching '*', use provided identifiers intersecting schema; our parser doesn't support '*'
                std::vector<int> col_idx; std::vector<std::string> col_names;
                if(s->columns.empty()){
                    for(size_t i=0;i<schema.columns.size();++i){ col_idx.push_back((int)i); col_names.push_back(schema.columns[i].name); }
                } else {
                    for(const auto& name : s->columns){
                        std::string nlc = to_lower(name);
                        for(size_t i=0;i<schema.columns.size();++i){ if(to_lower(schema.columns[i].name)==nlc){ col_idx.push_back((int)i); col_names.push_back(schema.columns[i].name); break; } }
                    }
                    if(col_idx.empty()){ for(size_t i=0;i<schema.columns.size();++i){ col_idx.push_back((int)i); col_names.push_back(schema.columns[i].name); }
                    }
                }
                // send column count
                std::vector<uint8_t> pcols; lenc_int(pcols, col_idx.size()); if(!write_packet(fd, seq, pcols)) { std::cerr << "[MySQLCompat] Failed to send column-count for SELECT" << std::endl; return; }
                // column definitions
                for(size_t k=0;k<col_idx.size();++k){ int i = col_idx[k]; int t = mysql_type_from(schema.columns[i].type); auto col = make_coldef(s->fromTable, col_names[k], t); if(!write_packet(fd, seq, col)) { std::cerr << "[MySQLCompat] Failed to send coldef for SELECT" << std::endl; return; } }
                auto eof = make_eof(); if(!write_packet(fd, seq, eof)) { std::cerr << "[MySQLCompat] Failed to send EOF for SELECT" << std::endl; return; }
                // rows
                for(const auto& kv : rows){ auto fields = split(kv.second, '|'); std::vector<std::string> out; out.reserve(col_idx.size()); for(int i : col_idx){ if(i<(int)fields.size()) out.push_back(fields[i]); else out.push_back(""); } auto row = make_text_row(out); if(!write_packet(fd, seq, row)) { std::cerr << "[MySQLCompat] Failed to send row for SELECT" << std::endl; return; } }
                auto eof2 = make_eof(); if(!write_packet(fd, seq, eof2)) { std::cerr << "[MySQLCompat] Failed to send EOF2 for SELECT" << std::endl; } return;
            }
            // Non-SELECT -> execute and return OK
            std::string out = exec_.execute(unit);
            std::cout << "[MySQLCompat] Non-SELECT exec output: " << out << std::endl;
            // very rough affected rows extraction for DELETE
            uint64_t affected=0; if(out.find("count=")!=std::string::npos){ std::string tail = out.substr(out.find("count=")+6); affected = strtoull(tail.c_str(), nullptr, 10); }
            auto ok = make_ok(affected);
            bool okSent = write_packet(fd, seq, ok);//发送回应
            std::cout << (okSent ? "[MySQLCompat] OK sent" : "[MySQLCompat] Failed to send OK") << std::endl;
        } catch(const std::exception& e){
            auto err = make_err(1064, std::string(e.what())); if(!write_packet(fd, seq, err)){ std::cerr << "[MySQLCompat] Failed to send ERR for exception" << std::endl; }
        }
    }

private:
    StorageEngine storage_;
    ExecutionEngine exec_;
};

int main(int argc, char** argv){
    try{
        uint16_t port = 3307;
        // install signal handlers for graceful shutdown
        signal(SIGINT, on_signal);
        signal(SIGTERM, on_signal);
        // Prefer argv[1] if provided
        if (argc > 1) {
            try{
                int p = std::stoi(argv[1]);
                if (p > 0 && p < 65536) port = (uint16_t)p;
            }catch(...){ /* ignore invalid */ }
        } else {
            // Fallback to env vars
            const char* envp = std::getenv("PCSQL_PORT");
            if(!envp) envp = std::getenv("PORT");
            if(envp){
                try{
                    int p = std::stoi(envp);
                    if (p > 0 && p < 65536) port = (uint16_t)p;
                }catch(...){ /* ignore invalid */ }
            }
        }
        std::cout << "Starting PcSQL MySQL-compatible server on 0.0.0.0:" << port << std::endl;
        MySQLServer server; return server.run(port);
    }catch(const std::exception& e){ std::cerr << "Fatal: " << e.what() << std::endl; return 1; }
}