#include "parser.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstring>
#include <limits>
#include <sstream>
#include <string_view>

namespace
{
    inline std::string_view trim(std::string_view sv)
    {
        size_t start = 0;
        while (start < sv.size() && std::isspace(static_cast<unsigned char>(sv[start])))
            ++start;

        size_t end = sv.size();
        while (end > start && std::isspace(static_cast<unsigned char>(sv[end - 1])))
            --end;

        return sv.substr(start, end - start);
    }

    inline bool iequals(std::string_view a, std::string_view b)
    {
        if (a.size() != b.size())
            return false;

        for (size_t i = 0; i < a.size(); ++i)
        {
            if (std::toupper(static_cast<unsigned char>(a[i])) !=
                std::toupper(static_cast<unsigned char>(b[i])))
            {
                return false;
            }
        }
        return true;
    }

    inline bool starts_with_keyword(std::string_view sv, const char *keyword)
    {
        const size_t len = std::strlen(keyword);
        if (sv.size() < len)
            return false;

        std::string_view head = sv.substr(0, len);
        if (!iequals(head, std::string_view(keyword, len)))
            return false;

        if (sv.size() == len)
            return true;

        return std::isspace(static_cast<unsigned char>(sv[len])) != 0;
    }

    size_t ifind_keyword(std::string_view sv, const char *keyword)
    {
        const size_t klen = std::strlen(keyword);
        if (klen == 0 || sv.size() < klen)
            return std::string::npos;

        for (size_t i = 0; i + klen <= sv.size(); ++i)
        {
            bool match = true;
            for (size_t j = 0; j < klen; ++j)
            {
                if (std::toupper(static_cast<unsigned char>(sv[i + j])) !=
                    std::toupper(static_cast<unsigned char>(keyword[j])))
                {
                    match = false;
                    break;
                }
            }

            if (!match)
                continue;

            const bool left_ok = (i == 0) || std::isspace(static_cast<unsigned char>(sv[i - 1]));
            const bool right_ok = (i + klen == sv.size()) || std::isspace(static_cast<unsigned char>(sv[i + klen]));

            if (left_ok && right_ok)
                return i;
        }

        return std::string::npos;
    }

    inline bool is_identifier_char(char c)
    {
        const unsigned char uc = static_cast<unsigned char>(c);
        return std::isalnum(uc) || c == '_';
    }

    bool is_valid_identifier(std::string_view id)
    {
        if (id.empty())
            return false;

        const unsigned char first = static_cast<unsigned char>(id.front());
        if (!(std::isalpha(first) || id.front() == '_'))
            return false;

        for (char c : id)
        {
            if (!is_identifier_char(c))
                return false;
        }
        return true;
    }

    bool parse_uint64(std::string_view text, uint64_t &out)
    {
        text = trim(text);
        if (text.empty())
            return false;

        uint64_t value = 0;
        for (char c : text)
        {
            if (!std::isdigit(static_cast<unsigned char>(c)))
                return false;
            const uint64_t digit = static_cast<uint64_t>(c - '0');
            if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10)
                return false;
            value = value * 10 + digit;
        }

        out = value;
        return true;
    }

    bool split_comma_list(std::string_view input, std::vector<std::string> &out, bool allow_quotes)
    {
        out.clear();

        std::string token;
        token.reserve(input.size());

        bool in_single_quote = false;
        bool in_double_quote = false;

        for (size_t i = 0; i < input.size(); ++i)
        {
            const char c = input[i];

            if (allow_quotes)
            {
                if (c == '\'' && !in_double_quote)
                {
                    in_single_quote = !in_single_quote;
                    token.push_back(c);
                    continue;
                }
                if (c == '"' && !in_single_quote)
                {
                    in_double_quote = !in_double_quote;
                    token.push_back(c);
                    continue;
                }
            }

            if (c == ',' && !in_single_quote && !in_double_quote)
            {
                std::string_view part = trim(token);
                if (part.empty())
                    return false;
                out.emplace_back(part);
                token.clear();
                continue;
            }

            token.push_back(c);
        }

        if (in_single_quote || in_double_quote)
            return false;

        std::string_view last = trim(token);
        if (last.empty())
            return false;
        out.emplace_back(last);
        return true;
    }

    bool parse_create_table(std::string_view q, ParsedQuery &result)
    {
        if (!starts_with_keyword(q, "CREATE"))
            return false;

        q = trim(q.substr(6));
        if (!starts_with_keyword(q, "TABLE"))
            return false;

        q = trim(q.substr(5));
        const size_t open_paren = q.find('(');
        const size_t close_paren = q.rfind(')');

        if (open_paren == std::string::npos || close_paren == std::string::npos || close_paren <= open_paren)
            return false;

        const std::string_view table_name = trim(q.substr(0, open_paren));
        if (!is_valid_identifier(table_name))
            return false;

        // Ensure no tokens after trailing ')'.
        if (!trim(q.substr(close_paren + 1)).empty())
            return false;

        const std::string_view cols_raw = q.substr(open_paren + 1, close_paren - open_paren - 1);
        std::vector<std::string> col_defs;
        if (!split_comma_list(cols_raw, col_defs, false))
            return false;

        CreateTableQuery ct;
        ct.table_name = std::string(table_name);

        bool seen_primary = false;
        for (const std::string &col_def : col_defs)
        {
            std::stringstream ss(col_def);
            std::string name;
            std::string type;
            if (!(ss >> name >> type))
                return false;

            if (!is_valid_identifier(name))
                return false;

            ColumnDef col;
            col.name = name;
            col.type = ColumnDef::parse_type_string(type);

            std::vector<std::string> extras;
            std::string extra;
            while (ss >> extra)
                extras.push_back(extra);

            if (!extras.empty())
            {
                if (extras.size() == 2)
                {
                    std::string a = extras[0];
                    std::string b = extras[1];
                    std::transform(a.begin(), a.end(), a.begin(), ::toupper);
                    std::transform(b.begin(), b.end(), b.begin(), ::toupper);
                    if (a == "PRIMARY" && b == "KEY")
                    {
                        col.is_primary_key = true;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }

            if (col.is_primary_key)
            {
                if (seen_primary)
                    return false;
                seen_primary = true;
            }

            ct.columns.push_back(col);
        }

        if (ct.columns.empty())
            return false;

        result.type = QueryType::CREATE_TABLE;
        result.query = std::move(ct);
        return true;
    }

    bool parse_insert(std::string_view q, ParsedQuery &result)
    {
        if (!starts_with_keyword(q, "INSERT"))
            return false;

        q = trim(q.substr(6));
        if (!starts_with_keyword(q, "INTO"))
            return false;

        q = trim(q.substr(4));

        const size_t values_pos = ifind_keyword(q, "VALUES");
        if (values_pos == std::string::npos)
            return false;

        std::string_view left = trim(q.substr(0, values_pos));
        if (!is_valid_identifier(left))
            return false;

        std::string_view rest = trim(q.substr(values_pos + 6));
        if (rest.empty() || rest.front() != '(')
            return false;

        size_t close_paren = std::string::npos;
        bool in_single_quote = false;
        bool in_double_quote = false;
        for (size_t i = 0; i < rest.size(); ++i)
        {
            const char c = rest[i];
            if (c == '\'' && !in_double_quote)
            {
                in_single_quote = !in_single_quote;
                continue;
            }
            if (c == '"' && !in_single_quote)
            {
                in_double_quote = !in_double_quote;
                continue;
            }

            if (c == ')' && !in_single_quote && !in_double_quote)
            {
                close_paren = i;
                break;
            }
        }

        if (close_paren == std::string::npos)
            return false;

        const std::string_view values_part = rest.substr(1, close_paren - 1);
        rest = trim(rest.substr(close_paren + 1));

        InsertQuery iq;
        iq.table_name = std::string(left);
        iq.expires_at = 0;

        std::vector<std::string> raw_values;
        if (!split_comma_list(values_part, raw_values, true))
            return false;

        for (std::string token : raw_values)
        {
            std::string_view val = trim(token);
            if (val.size() >= 2)
            {
                if ((val.front() == '\'' && val.back() == '\'') ||
                    (val.front() == '"' && val.back() == '"'))
                {
                    val = val.substr(1, val.size() - 2);
                }
            }

            if (val.empty())
                return false;

            iq.values.emplace_back(val);
        }

        if (iq.values.empty())
            return false;

        if (!rest.empty())
        {
            if (!starts_with_keyword(rest, "EXPIRY"))
                return false;

            rest = trim(rest.substr(6));
            uint64_t ttl = 0;
            if (!parse_uint64(rest, ttl))
                return false;

            const uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
            iq.expires_at = now + ttl;
        }

        result.type = QueryType::INSERT;
        result.query = std::move(iq);
        return true;
    }

    bool parse_where_clause(std::string_view where_text, SelectQuery &sq)
    {
        where_text = trim(where_text);
        if (where_text.empty())
            return false;

        const size_t eq_pos = where_text.find('=');
        if (eq_pos == std::string::npos)
            return false;

        if (where_text.find('=', eq_pos + 1) != std::string::npos)
            return false;

        std::string_view lhs = trim(where_text.substr(0, eq_pos));
        std::string_view rhs = trim(where_text.substr(eq_pos + 1));

        if (lhs.empty() || rhs.empty())
            return false;

        const size_t dot = lhs.find('.');
        if (dot != std::string::npos)
        {
            lhs = lhs.substr(dot + 1);
        }

        if (!is_valid_identifier(lhs))
            return false;

        if (rhs.size() >= 2)
        {
            if ((rhs.front() == '\'' && rhs.back() == '\'') ||
                (rhs.front() == '"' && rhs.back() == '"'))
            {
                rhs = rhs.substr(1, rhs.size() - 2);
            }
        }

        if (rhs.empty())
            return false;

        sq.has_where = true;
        sq.where_column = std::string(lhs);
        sq.where_value = std::string(rhs);
        return true;
    }

    bool parse_select(std::string_view q, ParsedQuery &result)
    {
        if (!starts_with_keyword(q, "SELECT"))
            return false;

        q = trim(q.substr(6));

        const size_t from_pos = ifind_keyword(q, "FROM");
        if (from_pos == std::string::npos)
            return false;

        std::string_view cols_part = trim(q.substr(0, from_pos));
        std::string_view after_from = trim(q.substr(from_pos + 4));

        if (cols_part.empty() || after_from.empty())
            return false;

        SelectQuery sq;
        sq.has_where = false;
        sq.is_join = false;

        if (cols_part == "*")
        {
            sq.columns.push_back("*");
        }
        else
        {
            std::vector<std::string> cols;
            if (!split_comma_list(cols_part, cols, false))
                return false;

            for (const std::string &col_raw : cols)
            {
                std::string_view col = trim(col_raw);
                if (col.empty())
                    return false;

                const size_t dot = col.find('.');
                if (dot != std::string::npos)
                {
                    std::string_view table = col.substr(0, dot);
                    std::string_view cname = col.substr(dot + 1);
                    if (!is_valid_identifier(table) || !is_valid_identifier(cname))
                        return false;
                    sq.columns.emplace_back(col);
                }
                else
                {
                    if (!is_valid_identifier(col))
                        return false;
                    sq.columns.emplace_back(col);
                }
            }
        }

        const size_t join_pos = ifind_keyword(after_from, "INNER JOIN");

        if (join_pos != std::string::npos)
        {
            sq.is_join = true;

            std::string_view base_table = trim(after_from.substr(0, join_pos));
            if (!is_valid_identifier(base_table))
                return false;
            sq.table_name = std::string(base_table);

            std::string_view join_rest = trim(after_from.substr(join_pos + 10));
            if (join_rest.empty())
                return false;

            const size_t on_pos = ifind_keyword(join_rest, "ON");
            if (on_pos == std::string::npos)
                return false;

            std::string_view join_table = trim(join_rest.substr(0, on_pos));
            if (!is_valid_identifier(join_table))
                return false;
            sq.join_table = std::string(join_table);

            std::string_view on_rest = trim(join_rest.substr(on_pos + 2));
            const size_t where_after_on = ifind_keyword(on_rest, "WHERE");

            std::string_view join_cond = where_after_on == std::string::npos
                                             ? on_rest
                                             : trim(on_rest.substr(0, where_after_on));

            const size_t eq_pos = join_cond.find('=');
            if (eq_pos == std::string::npos)
                return false;
            if (join_cond.find('=', eq_pos + 1) != std::string::npos)
                return false;

            std::string_view lhs = trim(join_cond.substr(0, eq_pos));
            std::string_view rhs = trim(join_cond.substr(eq_pos + 1));

            const size_t lhs_dot = lhs.find('.');
            const size_t rhs_dot = rhs.find('.');
            if (lhs_dot == std::string::npos || rhs_dot == std::string::npos)
                return false;

            std::string_view lhs_col = trim(lhs.substr(lhs_dot + 1));
            std::string_view rhs_col = trim(rhs.substr(rhs_dot + 1));

            if (!is_valid_identifier(lhs_col) || !is_valid_identifier(rhs_col))
                return false;

            sq.join_condition_col1 = std::string(lhs_col);
            sq.join_condition_col2 = std::string(rhs_col);

            if (where_after_on != std::string::npos)
            {
                std::string_view where_text = trim(on_rest.substr(where_after_on + 5));
                if (!parse_where_clause(where_text, sq))
                    return false;
            }
        }
        else
        {
            const size_t local_where_pos = ifind_keyword(after_from, "WHERE");
            std::string_view table_name = local_where_pos == std::string::npos
                                              ? trim(after_from)
                                              : trim(after_from.substr(0, local_where_pos));

            if (!is_valid_identifier(table_name))
                return false;
            sq.table_name = std::string(table_name);

            if (local_where_pos != std::string::npos)
            {
                std::string_view where_text = trim(after_from.substr(local_where_pos + 5));
                if (!parse_where_clause(where_text, sq))
                    return false;
            }
        }

        if (sq.table_name.empty() || sq.columns.empty())
            return false;

        result.type = QueryType::SELECT;
        result.query = std::move(sq);
        return true;
    }
}

ParsedQuery Parser::parse(const std::string &sql)
{
    ParsedQuery result;
    result.type = QueryType::UNKNOWN;

    std::string_view q = trim(sql);

    // Strip one optional trailing semicolon then ensure no extra trailing tokens.
    if (!q.empty() && q.back() == ';')
    {
        q.remove_suffix(1);
        q = trim(q);
    }

    if (q.empty())
        return result;

    if (q.size() >= 2 && q[0] == '-' && q[1] == '-')
        return result;

    if (starts_with_keyword(q, "CHECKPOINT"))
    {
        if (!trim(q.substr(10)).empty())
            return result;

        result.type = QueryType::CHECKPOINT;
        result.query = CheckpointQuery{};
        return result;
    }

    if (starts_with_keyword(q, "CREATE"))
    {
        if (parse_create_table(q, result))
            return result;
        return ParsedQuery{QueryType::UNKNOWN, CreateTableQuery{}};
    }

    if (starts_with_keyword(q, "INSERT"))
    {
        if (parse_insert(q, result))
            return result;
        return ParsedQuery{QueryType::UNKNOWN, CreateTableQuery{}};
    }

    if (starts_with_keyword(q, "SELECT"))
    {
        if (parse_select(q, result))
            return result;
        return ParsedQuery{QueryType::UNKNOWN, CreateTableQuery{}};
    }

    return result;
}
