#include "parser/parser.h"
#include <cctype>
#include <algorithm>
#include <stdexcept>
#include <sstream>

// ─── Tokenizer ───────────────────────────────────────────────────────────────
// Splits SQL into tokens. Handles:
//  - Quoted strings: 'hello world'  → single token (without quotes)
//  - Parentheses, comma, semicolon: each is its own token
//  - Numbers, identifiers: whitespace-delimited

void Parser::tokenize(const std::string& sql) {
    tokens_.clear();
    pos_ = 0;
    size_t i = 0, n = sql.size();
    while (i < n) {
        char c = sql[i];
        if (std::isspace((unsigned char)c)) { i++; continue; }
        if (c == '\'') {
            // Quoted string
            size_t j = i + 1;
            while (j < n && sql[j] != '\'') j++;
            tokens_.push_back(std::string_view(sql.data() + i + 1, j - i - 1));
            i = j + 1;
        } else if (c == '(' || c == ')' || c == ',' || c == ';') {
            tokens_.push_back(std::string_view(sql.data() + i, 1));
            i++;
        } else if (c == '<' || c == '>' || c == '=') {
            // Handle <=, >=
            if (i + 1 < n && sql[i+1] == '=') {
                tokens_.push_back(std::string_view(sql.data() + i, 2));
                i += 2;
            } else {
                tokens_.push_back(std::string_view(sql.data() + i, 1));
                i++;
            }
        } else if (c == '.') {
            // Dot attached to previous token (table.column)
            if (!tokens_.empty() && tokens_.back().data() + tokens_.back().size() == sql.data() + i) {
                // peek forward
                size_t j = i + 1;
                while (j < n && (std::isalnum((unsigned char)sql[j]) || sql[j] == '_')) j++;
                // Expand the string_view window perfectly!
                tokens_.back() = std::string_view(tokens_.back().data(), tokens_.back().size() + (j - i));
                i = j;
            } else {
                i++;
            }
        } else {
            // Identifier / number
            size_t j = i;
            while (j < n && !std::isspace((unsigned char)sql[j]) &&
                   sql[j] != '(' && sql[j] != ')' && sql[j] != ',' &&
                   sql[j] != ';' && sql[j] != '\'' && sql[j] != '=' &&
                   sql[j] != '<' && sql[j] != '>') {
                j++;
            }
            tokens_.push_back(std::string_view(sql.data() + i, j - i));
            i = j;
        }
    }
}

bool Parser::icase_eq(std::string_view a, std::string_view b) const {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (std::tolower((unsigned char)a[i]) != std::tolower((unsigned char)b[i])) return false;
    return true;
}

bool Parser::try_consume(std::string_view tok) {
    if (has() && icase_eq(peek(), tok)) { pos_++; return true; }
    return false;
}

bool Parser::expect(std::string_view tok) {
    if (!has() || !icase_eq(peek(), tok)) return false;
    pos_++;
    return true;
}

ColType Parser::parse_type(const std::string& tok, uint16_t& max_len) {
    max_len = 0;
    std::string up = tok;
    std::transform(up.begin(), up.end(), up.begin(), ::toupper);
    if (up == "INT" || up == "INTEGER" || up == "BIGINT") return ColType::INT;
    if (up == "DECIMAL" || up == "FLOAT" || up == "DOUBLE" || up == "REAL") return ColType::DECIMAL;
    if (up == "DATETIME" || up == "TIMESTAMP") return ColType::DATETIME;
    // VARCHAR / TEXT
    max_len = 255;
    return ColType::VARCHAR;
}

bool Parser::parse_create(Statement& s, std::string& err) {
    if (!expect("TABLE")) { err = "Expected TABLE"; return false; }
    if (icase_eq(peek(), "IF")) {
        consume(); // IF
        if (!expect("NOT")) { err = "Expected NOT after IF"; return false; }
        if (!expect("EXISTS")) { err = "Expected EXISTS after NOT"; return false; }
    }
    if (!has()) { err = "Expected table name"; return false; }
    s.table_name = std::string(consume());
    std::transform(s.table_name.begin(), s.table_name.end(), s.table_name.begin(), ::toupper);
    if (!expect("(")) { err = "Expected ("; return false; }

    while (has() && !icase_eq(peek(), ")")) {
        ColSpec cs{};
        cs.name = std::string(consume());
        std::transform(cs.name.begin(), cs.name.end(), cs.name.begin(), ::toupper);
        if (!has()) { err = "Expected type"; return false; }
        std::string type_tok = std::string(consume());
        cs.type = parse_type(type_tok, cs.max_len);
        // Optional: VARCHAR(N)
        if (cs.type == ColType::VARCHAR && has() && peek() == "(") {
            consume(); // (
            if (has() && peek() != ")") cs.max_len = (uint16_t)std::stoi(std::string(consume()));
            expect(")");
        }
        // Modifiers: PRIMARY KEY, NOT NULL
        while (has() && !icase_eq(peek(), ",") && !icase_eq(peek(), ")")) {
            if (icase_eq(peek(), "PRIMARY")) {
                consume(); try_consume("KEY"); cs.primary_key = true;
            } else if (icase_eq(peek(), "NOT")) {
                consume(); try_consume("NULL"); cs.not_null = true;
            } else {
                consume(); // skip AUTOINCREMENT etc
            }
        }
        s.col_specs.push_back(cs);
        try_consume(",");
    }
    expect(")");
    try_consume(";");
    return true;
}

bool Parser::parse_insert(Statement& s, std::string& err) {
    if (!expect("INTO")) { err = "Expected INTO"; return false; }
    if (!has()) { err = "Expected table name"; return false; }
    std::string tbl = std::string(consume());
    std::transform(tbl.begin(), tbl.end(), tbl.begin(), ::toupper);
    s.table_name = tbl;

    // Skip optional column list
    if (has() && icase_eq(peek(), "(")) {
        // Could be column list or VALUES (...)
        // Peek: if first token inside looks like an identifier -> column list; skip
        consume(); // (
        int depth = 1;
        while (has() && depth > 0) {
            if (peek() == "(") depth++;
            else if (peek() == ")") depth--;
            if (depth > 0) consume();
        }
        consume(); // closing )
    }

    if (!expect("VALUES")) { err = "Expected VALUES"; return false; }
    
    do {
        if (!expect("(")) { err = "Expected ("; return false; }
        std::vector<std::string_view> row;
        while (has() && !icase_eq(peek(), ")")) {
            row.push_back(consume());
            try_consume(",");
        }
        if (!expect(")")) { err = "Expected )"; return false; }
        s.insert_rows.push_back(std::move(row));
    } while (try_consume(","));

    try_consume(";");
    return true;
}

bool Parser::parse_select(Statement& s, std::string& err) {
    // Parse column list or *
    if (has() && peek() == "*") {
        consume();
        s.select_cols.push_back({true, "", ""});
    } else {
        // col, col, ...
        while (has() && !icase_eq(peek(), "FROM")) {
            SelectCol sc{false, "", ""};
            std::string tok = std::string(consume());
            // Check for "table.col"
            auto dot = tok.find('.');
            if (dot != std::string::npos) {
                sc.table = tok.substr(0, dot);
                sc.col   = tok.substr(dot + 1);
                std::transform(sc.table.begin(), sc.table.end(), sc.table.begin(), ::toupper);
                std::transform(sc.col.begin(), sc.col.end(), sc.col.begin(), ::toupper);
                if (sc.col == "*") sc.star = true;
            } else {
                std::transform(tok.begin(), tok.end(), tok.begin(), ::toupper);
                sc.col = tok;
            }
            s.select_cols.push_back(sc);
            try_consume(",");
        }
    }

    if (!expect("FROM")) { err = "Expected FROM"; return false; }
    if (!has()) { err = "Expected table name"; return false; }
    s.from_table = std::string(consume());
    std::transform(s.from_table.begin(), s.from_table.end(), s.from_table.begin(), ::toupper);

    // INNER JOIN?
    if (has() && icase_eq(peek(), "INNER")) {
        consume(); // INNER
        if (!expect("JOIN")) { err = "Expected JOIN"; return false; }
        JoinClause jc;
        jc.right_table = std::string(consume());
        std::transform(jc.right_table.begin(), jc.right_table.end(), jc.right_table.begin(), ::toupper);
        if (!expect("ON")) { err = "Expected ON"; return false; }
        jc.left_col  = std::string(consume());
        std::transform(jc.left_col.begin(), jc.left_col.end(), jc.left_col.begin(), ::toupper);
        expect("=");
        jc.right_col = std::string(consume());
        std::transform(jc.right_col.begin(), jc.right_col.end(), jc.right_col.begin(), ::toupper);
        s.join = jc;
    }

    // WHERE?
    if (has() && icase_eq(peek(), "WHERE")) {
        consume();
        s.where = WhereClause{};
        if (!parse_where(s.where.value(), err)) return false;
    }

    if (has() && icase_eq(peek(), "ORDER")) {
        consume(); // ORDER
        if (!expect("BY")) { err = "Expected BY after ORDER"; return false; }
        if (!has()) { err = "Expected column for ORDER BY"; return false; }
        s.order_by_col = std::string(consume());
        std::transform(s.order_by_col.begin(), s.order_by_col.end(), s.order_by_col.begin(), ::toupper);
        if (has() && icase_eq(peek(), "DESC")) {
            consume();
            s.order_desc = true;
        } else if (has() && icase_eq(peek(), "ASC")) {
            consume();
        }
    }

    try_consume(";");
    return true;
}

bool Parser::parse_delete(Statement& s, std::string& err) {
    s.type = StmtType::DELETE;
    // The "DELETE" token has already been consumed by parse()
    if (!expect("FROM")) { err = "Expected FROM"; return false; }
    if (!has()) { err = "Expected table name"; return false; }
    s.table_name = std::string(consume());
    std::transform(s.table_name.begin(), s.table_name.end(), s.table_name.begin(), ::toupper);
    if (has() && icase_eq(peek(), "WHERE")) {
        consume();
        s.where = WhereClause{};
        if (!parse_where(s.where.value(), err)) return false;
    }
    try_consume(";");
    return true;
}

bool Parser::parse_where(WhereClause& w, std::string& err) {
    if (!has()) { err = "Expected WHERE column"; return false; }
    w.col = std::string(consume());
    std::transform(w.col.begin(), w.col.end(), w.col.begin(), ::toupper);
    if (!has()) { err = "Expected operator"; return false; }
    w.op = std::string(consume());
    if (!has()) { err = "Expected value"; return false; }
    w.rhs = std::string(consume());
    return true;
}

bool Parser::parse(const std::string& sql, Statement& stmt, std::string& err) {
    tokenize(sql);
    if (!has()) { err = "Empty statement"; return false; }
    std::string first = std::string(consume());

    bool ok = false;
    if (icase_eq(first, "CREATE")) {
        stmt.type = StmtType::CREATE_TABLE;
        ok = parse_create(stmt, err);
    } else if (icase_eq(first, "INSERT")) {
        stmt.type = StmtType::INSERT;
        ok = parse_insert(stmt, err);
    } else if (icase_eq(first, "SELECT")) {
        stmt.type = StmtType::SELECT;
        ok = parse_select(stmt, err);
    } else if (icase_eq(first, "DELETE")) {
        stmt.type = StmtType::DELETE;
        ok = parse_delete(stmt, err);
    } else {
        err = "Unknown statement: " + first;
        return false;
    }

    if (ok && has()) {
        err = "Syntax error: trailing characters: " + std::string(peek());
        return false;
    }
    if (pos_ < tokens_.size()) {
        err = "Syntax error: trailing characters: " + std::string(peek());
        return false;
    }
    return ok;
}
