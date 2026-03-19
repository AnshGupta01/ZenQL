#include "parser.h"
#include <sstream>
#include <algorithm>

ParsedQuery Parser::parse(const std::string& sql) {
    ParsedQuery result;
    result.type = QueryType::UNKNOWN;

    std::string q = sql;
    q.erase(0, q.find_first_not_of(" \n\r\t"));
    q.erase(q.find_last_not_of(" \n\r\t") + 1);

    if (q.empty() || q.find("--") == 0) return result;

    std::string up_q = q;
    std::transform(up_q.begin(), up_q.end(), up_q.begin(), ::toupper);

    if (up_q.find("CREATE TABLE") == 0) {
        result.type = QueryType::CREATE_TABLE;
        CreateTableQuery ct;
        size_t start_name = q.find_first_not_of(" \n\r\t", 12); 
        size_t end_name = q.find("(", start_name);
        if (end_name != std::string::npos) {
            std::string tname = q.substr(start_name, end_name - start_name);
            tname.erase(tname.find_last_not_of(" \n\r\t;") + 1);
            tname.erase(0, tname.find_first_not_of(" \n\r\t;"));
            ct.table_name = tname;

            size_t col_start = end_name + 1;
            size_t col_end = q.find_last_of(")");
            if (col_end != std::string::npos && col_end > col_start) {
                std::string cols_str = q.substr(col_start, col_end - col_start);
                std::stringstream ss(cols_str);
                std::string col_def;
                while (std::getline(ss, col_def, ',')) {
                    std::stringstream ss_inner(col_def);
                    std::string cname, ctype;
                    ss_inner >> cname >> ctype;
                    cname.erase(cname.find_last_not_of(";") + 1);
                    ctype.erase(ctype.find_last_not_of(";") + 1);
                    if (!cname.empty() && !ctype.empty()) {
                        ColumnDef cd;
                        cd.name = cname;
                        std::transform(ctype.begin(), ctype.end(), ctype.begin(), ::toupper);
                        if (ctype == "INT") cd.type = DataType::INT;
                        else if (ctype == "DECIMAL") cd.type = DataType::DECIMAL;
                        else if (ctype == "DATETIME") cd.type = DataType::DATETIME;
                        else cd.type = DataType::VARCHAR;
                        
                        std::string extra;
                        while(ss_inner >> extra) {
                            std::transform(extra.begin(), extra.end(), extra.begin(), ::toupper);
                            if (extra == "PRIMARY") cd.is_primary_key = true;
                        }
                        ct.columns.push_back(cd);
                    }
                }
            }
        }
        result.query = ct;
    } 
    else if (up_q.find("INSERT INTO") == 0) {
        result.type = QueryType::INSERT;
        InsertQuery iq;
        iq.expires_at = 0; 
        
        size_t start = up_q.find("INTO") + 4;
        size_t end = up_q.find("VALUES");
        
        if (end != std::string::npos) {
            std::string tname = q.substr(start, end - start);
            tname.erase(tname.find_last_not_of(" \n\r\t;") + 1);
            tname.erase(0, tname.find_first_not_of(" \n\r\t;"));
            iq.table_name = tname;

            size_t val_start = q.find("(", end);
            size_t val_end = q.find(")", val_start);
            if (val_end != std::string::npos && val_start != std::string::npos) {
                std::string val_str = q.substr(val_start + 1, val_end - val_start - 1);
                std::stringstream ss(val_str);
                std::string token;
                while(std::getline(ss, token, ',')) {
                    token.erase(0, token.find_first_not_of(" \n\r\t'"));
                    token.erase(token.find_last_not_of(" \n\r\t'") + 1);
                    iq.values.push_back(token);
                }

                // Check for optional EXPIRY clause
                size_t expiry_pos = up_q.find("EXPIRY", val_end);
                if (expiry_pos != std::string::npos) {
                    std::string exp_str = q.substr(expiry_pos + 6);
                    exp_str.erase(exp_str.find_last_not_of(" \n\r\t;") + 1);
                    exp_str.erase(0, exp_str.find_first_not_of(" \n\r\t;"));
                    if (!exp_str.empty()) {
                        uint64_t ttl = std::stoull(exp_str);
                        auto now = std::chrono::system_clock::now();
                        auto duration = now.time_since_epoch();
                        iq.expires_at = std::chrono::duration_cast<std::chrono::seconds>(duration).count() + ttl;
                    }
                }
            }
        }
        result.query = iq;
    } 
    else if (up_q.find("SELECT") == 0) {
        result.type = QueryType::SELECT;
        SelectQuery sq;
        sq.has_where = false;
        sq.is_join = false;
        
        size_t from_pos = up_q.find("FROM");
        
        if (from_pos != std::string::npos) {
            size_t sel_end = up_q.find("SELECT") + 6;
            std::string cols_part = q.substr(sel_end, from_pos - sel_end);
            std::stringstream ss_cols(cols_part);
            std::string col;
            while (std::getline(ss_cols, col, ',')) {
                // Trim whitespace
                col.erase(0, col.find_first_not_of(" \n\r\t"));
                col.erase(col.find_last_not_of(" \n\r\t") + 1);
                if (!col.empty()) sq.columns.push_back(col);
            }

            std::string after_from = q.substr(from_pos + 4);
            std::string up_after_from = up_q.substr(from_pos + 4);
            size_t semi = after_from.find(";");
            if (semi != std::string::npos) {
                after_from = after_from.substr(0, semi);
                up_after_from = up_after_from.substr(0, semi);
            }

            size_t join_pos = up_after_from.find("INNER JOIN");
            size_t where_pos = up_after_from.find("WHERE");

            if (join_pos != std::string::npos) {
                sq.is_join = true;
                std::string tname = after_from.substr(0, join_pos);
                tname.erase(tname.find_last_not_of(" \n\r\t") + 1);
                tname.erase(0, tname.find_first_not_of(" \n\r\t"));
                sq.table_name = tname;

                size_t on_pos = up_after_from.find("ON", join_pos);
                if (on_pos != std::string::npos) {
                    std::string jname = after_from.substr(join_pos + 10, on_pos - (join_pos + 10));
                    jname.erase(jname.find_last_not_of(" \n\r\t") + 1);
                    jname.erase(0, jname.find_first_not_of(" \n\r\t"));
                    sq.join_table = jname;

                    std::string condition = after_from.substr(on_pos + 2);
                    size_t where_in_on = up_after_from.find("WHERE", on_pos);
                    if (where_in_on != std::string::npos) {
                        condition = after_from.substr(on_pos + 2, where_in_on - (on_pos + 2));
                        
                        // Also parse the WHERE clause if it exists
                        sq.has_where = true;
                        std::string where_cond = after_from.substr(where_in_on + 5);
                        size_t w_eq = where_cond.find("=");
                        if (w_eq != std::string::npos) {
                            std::string w_col = where_cond.substr(0, w_eq);
                            std::string w_val = where_cond.substr(w_eq + 1);
                            if (w_col.find(".") != std::string::npos) w_col = w_col.substr(w_col.find(".") + 1);
                            w_col.erase(w_col.find_last_not_of(" \n\r\t;") + 1);
                            w_col.erase(0, w_col.find_first_not_of(" \n\r\t;"));
                            w_val.erase(w_val.find_last_not_of(" \n\r\t';") + 1);
                            w_val.erase(0, w_val.find_first_not_of(" \n\r\t';"));
                            sq.where_column = w_col;
                            sq.where_value = w_val;
                        }
                    }

                    size_t eq_pos = condition.find("=");
                    if (eq_pos != std::string::npos) {
                        std::string col1 = condition.substr(0, eq_pos);
                        std::string col2 = condition.substr(eq_pos + 1);
                        
                        if (col1.find(".") != std::string::npos) col1 = col1.substr(col1.find(".") + 1);
                        if (col2.find(".") != std::string::npos) col2 = col2.substr(col2.find(".") + 1);
                        
                        col1.erase(col1.find_last_not_of(" \n\r\t") + 1);
                        col1.erase(0, col1.find_first_not_of(" \n\r\t"));
                        col2.erase(col2.find_last_not_of(" \n\r\t") + 1);
                        col2.erase(0, col2.find_first_not_of(" \n\r\t"));

                        sq.join_condition_col1 = col1;
                        sq.join_condition_col2 = col2;
                    }
                }
            } else {
                std::string tname = after_from.substr(0, (where_pos != std::string::npos) ? where_pos : std::string::npos);
                tname.erase(tname.find_last_not_of(" \n\r\t") + 1);
                tname.erase(0, tname.find_first_not_of(" \n\r\t"));
                sq.table_name = tname;

                if (where_pos != std::string::npos) {
                    sq.has_where = true;
                    std::string condition = after_from.substr(where_pos + 5);
                    size_t eq_pos = condition.find("=");
                    if (eq_pos != std::string::npos) {
                        std::string col = condition.substr(0, eq_pos);
                        std::string val = condition.substr(eq_pos + 1);

                        col.erase(col.find_last_not_of(" \n\r\t;") + 1);
                        col.erase(0, col.find_first_not_of(" \n\r\t;"));
                        val.erase(val.find_last_not_of(" \n\r\t';") + 1);
                        val.erase(0, val.find_first_not_of(" \n\r\t';"));
                        
                        sq.where_column = col;
                        sq.where_value = val;
                    }
                }
            }
            result.query = sq;
        }
    }
    return result;
}
