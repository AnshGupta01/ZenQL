#include <iostream>
#include <string>
#include <vector>

#include "../src/parser/parser.h"

namespace
{
    bool expect_valid(const std::string &sql, QueryType expected)
    {
        ParsedQuery q = Parser::parse(sql);
        if (q.type != expected)
        {
            std::cerr << "Expected valid query type " << static_cast<int>(expected)
                      << " but got " << static_cast<int>(q.type)
                      << " for SQL: " << sql << std::endl;
            return false;
        }
        return true;
    }

    bool expect_invalid(const std::string &sql)
    {
        ParsedQuery q = Parser::parse(sql);
        if (q.type != QueryType::UNKNOWN)
        {
            std::cerr << "Expected invalid query but parser accepted SQL: " << sql << std::endl;
            return false;
        }
        return true;
    }
}

int main()
{
    bool ok = true;

    // Valid baseline commands.
    ok = ok && expect_valid("CREATE TABLE USERS (ID INT PRIMARY KEY, NAME VARCHAR);", QueryType::CREATE_TABLE);
    ok = ok && expect_valid("INSERT INTO USERS VALUES (1, 'Alice') EXPIRY 30;", QueryType::INSERT);
    ok = ok && expect_valid("SELECT * FROM USERS;", QueryType::SELECT);
    ok = ok && expect_valid("SELECT NAME, ID FROM USERS WHERE ID = 1;", QueryType::SELECT);
    ok = ok && expect_valid("SELECT * FROM USERS INNER JOIN POSTS ON USERS.ID = POSTS.USER_ID;", QueryType::SELECT);
    ok = ok && expect_valid("CHECKPOINT;", QueryType::CHECKPOINT);

    // User-reported crashers / malformed queries.
    ok = ok && expect_invalid("select;");
    ok = ok && expect_invalid("select **;");

    // Additional malformed corner cases.
    ok = ok && expect_invalid("SELECT * FROM;");
    ok = ok && expect_invalid("SELECT FROM USERS;");
    ok = ok && expect_invalid("SELECT * FROM USERS WHERE;");
    ok = ok && expect_invalid("SELECT * FROM USERS WHERE ID == 1;");
    ok = ok && expect_invalid("SELECT * FROM USERS INNER JOIN POSTS ON ID = USER_ID;");
    ok = ok && expect_invalid("CREATE TABLE USERS ID INT);");
    ok = ok && expect_invalid("CREATE TABLE USERS (ID INT PRIMARY KEY PRIMARY KEY);");
    ok = ok && expect_invalid("INSERT INTO USERS VALUES ();");
    ok = ok && expect_invalid("INSERT INTO USERS VALUES (1, 'Alice') EXPIRY foo;");
    ok = ok && expect_invalid("INSERT INTO USERS VALUES (1, 'Alice') EXPIRY 184467440737095516160;");
    ok = ok && expect_invalid("CHECKPOINT now;");
    ok = ok && expect_invalid("   ;   ");

    if (!ok)
    {
        std::cerr << "Parser robustness test failed." << std::endl;
        return 1;
    }

    std::cout << "Parser robustness test passed." << std::endl;
    return 0;
}
