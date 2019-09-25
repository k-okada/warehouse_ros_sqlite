// Modified file dbclientcursor.h

/*    Copyright 2009 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#ifndef WAREHOUSE_ROS_SQLITE_CLIENT_H
#define WAREHOUSE_ROS_SQLITE_CLIENT_H

#include <stack>
#include <numeric>

#include <ros/ros.h>
#include <bson.h>
namespace bson{
void appendElementHandlingGtLt(BSONObjBuilder& b, const BSONElement& e);
}
#include <json.h>

#include <string>
#include <vector>
#include <iterator>
#include <boost/foreach.hpp>
#include <boost/lockfree/stack.hpp>

#include <sqlite3.h>

namespace sqlite
{

using namespace bson;
class Query {
public:
  bson::BSONObj obj;
  Query() : obj(bson::BSONObj()) {}
  Query(const bson::BSONObj& b) : obj(b) { }

  std::string toString() const {
    return obj.toString();
  }


  Query& sort(const bson::BSONObj& sortPattern) {
    appendComplex("$orderby", sortPattern);
    return *this;
  }
  Query& sort(const std::string& field, int asc = 1) {
    sort(BSON(field << asc));
    return *this;
  }

private:
  template <class T>
  void appendComplex(const char* fieldName, const T& val) {
    //makeComplex();
    //bson::BSONObjBuilder b(std::move(obj));
    bson::BSONObjBuilder b;
    b.appendElements(obj);
    b.append(fieldName, val);
    obj = b.obj();
  }
  
  
};

static int sqliteCountCallback(void* arg, int argc, char** argv, char** errMsg) {
  int* c = (int*)arg;
  *c = atoi((const char*)(*argv));
  return 0;
}
  
class DBClientConnection;
class DBClientCursor : boost::noncopyable
{
public:

  /** If true, safe to call next().  Requests more from server if necessary. */
  bool more() {
    if (!_putBack.empty())
        return true;
    return false;    
  }

  void putBack(const bson::BSONObj& o) {
    //_putBack.push(o.getOwned());
    _putBack.push(bson::BSONObj(o)); // FIXME
  }

  bson::BSONObj next() {
    if (!_putBack.empty()) {
      bson::BSONObj ret = _putBack.top();
      _putBack.pop();
      return ret;
    }
    ROS_ERROR("DBClientCursor next() called but more() is false");
#if 0 // FIXME
    uassert(
        13422, "DBClientCursor next() called but more() is false", batch.pos < batch.objs.size());

    /* todo would be good to make data null at end of batch for safety */
    return std::move(batch.objs[batch.pos++]);
#endif
  }

  bson::BSONObj nextSafe() {
    return next();
  }

  DBClientCursor() {}

  DBClientCursor(DBClientConnection* client,
                 const std::string ns,
                 const bson::BSONObj& query)
    : _client(client),
      ns(ns),
      query(query) {
  }
    
private:
  DBClientConnection* _client;
  std::string ns;
  bson::BSONObj query;
  std::stack<bson::BSONObj> _putBack;
};

class DBClientConnection
{
public:
  ~DBClientConnection()
  {
    int err = sqlite3_close(conn_);
    if ( err != SQLITE_OK) {
      ROS_ERROR("Failed to closed DB");
    }
    ROS_WARN("Successfully closed the the DB");
  }

  int executeSQLQuery(std::string cmd)
  {
    ROS_DEBUG("executeSQLQuery %s", cmd.c_str());
    int count;
    char *errMsg = NULL;
    int err = sqlite3_exec(conn_, cmd.c_str(), sqliteCountCallback, &count, &errMsg);
    if ( err != SQLITE_OK) {
      ROS_ERROR("Failed to execute %s (%s)", cmd.c_str(), errMsg);
      sqlite3_free(errMsg);
      errMsg = NULL;
      return -1;
    }
    return count;
  }
  std::string createSQLQuery(const sqlite::Query query)
  {
    std::vector<std::string> cmd;
    bson::BSONForEach(q, query.obj) {
      bson::BSONElement elem = q;
      std::string comp = " = ";
      bson::BSONObjBuilder b;
      bson::BSONObj e;
      if(q.isABSONObj()) { //
        bson::appendElementHandlingGtLt(b, q);
        e = b.obj();
        elem = e.getField(q.fieldName());
        if ( q.Obj().getField( "$lt" ).ok() ) {
          comp = " < ";
        }else if ( q.Obj().getField( "$lte" ).ok() ) {
          comp = " <= ";
        }else if ( q.Obj().getField( "$gt" ).ok() ) {
          comp = " > ";
        }else if ( q.Obj().getField( "$gte" ).ok() ) {
          comp = " >= ";
        }else if ( std::string(q.fieldName()) == "$orderby" ) {
          continue; // skip
        }else {
          ROS_ERROR_STREAM(__PRETTY_FUNCTION__ << " : Unsupported query [" << q.toString() << "]");
        }
      }
      cmd.push_back("json_extract(document, '$." + std::string(q.fieldName()) + "') " + comp + elem.toString(false));
    }
    // join() https://stackoverflow.com/questions/9277906/stdvector-to-string-with-custom-delimiter
    if ( cmd.empty() )
    {
      return std::string("");
    }
    
    return std::string(" WHERE ") +
      std::accumulate(std::begin(cmd), std::end(cmd), std::string(),
                      [](std::string &ss, std::string &s)
                      {
                        return ss.empty()? s : ss + " AND " + s;
                      });
  }

  void connect(const std::string db_address)
  {
    int err = sqlite3_open(db_address.c_str(), &conn_);
    if ( err != SQLITE_OK) {
      ROS_ERROR("Failed to open %s", db_address.c_str());
    }
    ROS_WARN("Successfully connected to the %s DB", db_address.c_str());
  }
  
  void createTable(const std::string ns)
  {
    std::string cmd = "CREATE TABLE IF NOT EXISTS \"" + ns + "\"" +
      "(id INTEGER PRIMARY KEY AUTOINCREMENT, document JSON)";
    if ( executeSQLQuery(cmd) ) {
      ROS_WARN("Successfully created table %s", ns.c_str());
    }
  }
  
  void dropTable(const std::string& ns)
  {
    std::string cmd = "DROP TABLE IF EXISTS \"" + ns + "\";";
    if ( executeSQLQuery(cmd) ) {
      ROS_WARN("Successfully dropped table %s", ns.c_str());
    }
  }
  
  void createDatabase(const std::string& db_name)
  {
    createTable(db_name);
  }
  
  void dropDatabase(const std::string& db_name)
  {
    std::string cmd = "SELECT name FROM sqlite_master WHERE type='table';";
    sqlite3_stmt *stmt = NULL;
    int err;
    std::set<std::string> table_names;
    err = sqlite3_prepare_v2(conn_, cmd.c_str(), cmd.size(), &stmt, NULL);
    ROS_DEBUG("executeSQLQuery %s", cmd.c_str());    
    if ( err != SQLITE_OK) {
      ROS_ERROR("Failed to get table names %s", db_name.c_str());
      ROS_ERROR(" COMMAND = %s", cmd.c_str());
    } else {
      sqlite3_reset(stmt);
      while ( SQLITE_ROW == (err = sqlite3_step(stmt))){
        const unsigned char* txt = sqlite3_column_text(stmt, 0);
        table_names.insert(std::string(reinterpret_cast<const char *>(txt)));
      }
      sqlite3_finalize(stmt); 
    }
    for (auto &n : table_names) {
      if ( n == "sqlite_sequence") continue; // sqlite_sequence is a system table
      dropTable(n);
    }
    return;
  }
  
  bool isFailed()
  {
    return false;
  }

  void insert(const std::string ns, const bson::BSONObj& obj)
  {
    std::set<std::string> fields;
    obj.getFieldNames(fields);
    std::string cmd = "INSERT INTO \"" + ns + "\" (document) " +
      "VALUES ('" + obj.jsonString() + "')";

    sqlite3_stmt *stmt = NULL;
    int err;
    err = sqlite3_prepare_v2(conn_, cmd.c_str(), cmd.size(), &stmt, NULL);
    ROS_DEBUG("executeSQLQuery %s", cmd.c_str());    
    if (err != SQLITE_OK) {
      ROS_ERROR("Failed to insert %s into table %s", obj.toString().c_str(), ns.c_str());
      ROS_ERROR(" COMMAND = %s", cmd.c_str());
    } else {
      sqlite3_reset(stmt);
      while ( SQLITE_ROW == (err = sqlite3_step(stmt))){
        //std::string name = sqlite3_column_name(stmt,0);
      }
      sqlite3_finalize(stmt);
    }
  }

  unsigned int count(const std::string ns)
  {
    int count;
    char *errMsg = NULL;
    std::string cmd = "SELECT COUNT(*) FROM \"" + ns + "\";";
    return executeSQLQuery(cmd);
  }

  unsigned int count(const std::string ns, bson::BSONObj bson)
  {
    // FIXME
    return 0;
  }

  void remove(const std::string ns, const sqlite::Query& query )
  {
    std::string cmd = "DELETE FROM \"" + ns + "\" " + createSQLQuery(query);
    executeSQLQuery(cmd);
    return;
  }
    
  void update(const std::string ns, const sqlite::Query& query, const bson::BSONObj metadata)
  {
    std::string cmd;
    cmd += "UPDATE \"" + ns + "\" ";
    cmd += "SET ";
    std::vector<std::string> metastring;
    bson::BSONForEach(m, metadata) {
      metastring.push_back("document = (select json_set(json(document), '$." + std::string(m.fieldName()) + "', " + m.toString(false) + ") from \"" + ns + "\")");
    }
    // join() https://stackoverflow.com/questions/9277906/stdvector-to-string-with-custom-delimiter
    cmd += std::accumulate(std::begin(metastring), std::end(metastring), std::string(),
                           [](std::string &ss, std::string &s)
                           {
                             return ss.empty()? s : ss + ", " + s;
                           });
    
    cmd += createSQLQuery(query);
    executeSQLQuery(cmd);
    return;
  }

  std::auto_ptr<DBClientCursor> query(const std::string ns, const sqlite::Query query)
  {
    std::auto_ptr<DBClientCursor> c(new DBClientCursor(this, ns , query.obj));

    std::string cmd = "SELECT document FROM \"" + ns + "\" " + createSQLQuery(query);
    if ( query.obj.hasField("$orderby") ) {
      std::vector<std::string> orderby;
      bson::BSONForEach(o, query.obj.getField("$orderby").Obj()) {
        std::string str = "json_extract(document, '$." + std::string(o.fieldName()) + "') ";
        if ( o.numberInt() > 0 ) str += " DESC";
        orderby.push_back(str);
      }
      cmd += " ORDER BY " + std::accumulate(std::begin(orderby), std::end(orderby), std::string(),
                                           [](std::string &ss, std::string &s)
                                           {
                                             return ss.empty()? s : ss + ", " + s;
                                           });
    }
    
    sqlite3_stmt *stmt = NULL;
    int err;
    err = sqlite3_prepare_v2(conn_, cmd.c_str(), cmd.size(), &stmt, NULL);
    ROS_DEBUG("executeSQLQuery %s", cmd.c_str());
    if (err != SQLITE_OK) {
      ROS_ERROR("Failed to select table %s", ns.c_str());
      ROS_ERROR(" COMMAND = %s", cmd.c_str());
    } else {
      sqlite3_reset(stmt);
      while ( SQLITE_ROW == (err = sqlite3_step(stmt))){
        bson::BSONObjBuilder b;
        for(int i = 0; i < sqlite3_column_count(stmt); i++) {
          std::string name = sqlite3_column_name(stmt, i);
          int type = sqlite3_column_type(stmt, i);
          if ( type == SQLITE_TEXT ) {
            std::string doc = std::string(reinterpret_cast<const char *>(sqlite3_column_text(stmt, i)));
            ROS_DEBUG(".. returns %s", doc.c_str());
            c->putBack(bson::fromjson(doc));
          }else{
            ROS_ERROR_STREAM(__PRETTY_FUNCTION__ << " Invalid column type " << type << ", expected " << SQLITE_TEXT << std::endl);
          }
        }
      }
      sqlite3_finalize(stmt);

      if ( err != SQLITE_DONE ) {
        ROS_ERROR("Failed to select table %s", ns.c_str());
        ROS_ERROR(" COMMAND = %s", cmd.c_str());
      }
    }

    return c;
  }

private:
  sqlite3 *conn_;
  
  std::map<std::string, std::vector<bson::BSONObj> > data_;
};

}

#endif
