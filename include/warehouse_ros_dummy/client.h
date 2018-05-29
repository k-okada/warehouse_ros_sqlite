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

#ifndef WAREHOUSE_ROS_DUMMY_CLIENT_H
#define WAREHOUSE_ROS_DUMMY_CLIENT_H

#include <stack>

#include <ros/ros.h>
#include <bson.h>
namespace bson{
void appendElementHandlingGtLt(BSONObjBuilder& b, const BSONElement& e);
}

#include <string>
#include <vector>
#include <iterator>
#include <boost/foreach.hpp>
#include <boost/lockfree/stack.hpp>

namespace dummy
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
    ROS_ERROR(__PRETTY_FUNCTION__);
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
  void connect(const std::string db_address)
  {
    ROS_WARN("%s(%s)", __PRETTY_FUNCTION__, db_address.c_str());
  }
  
  void dropDatabase(const std::string& db_name)
  {
    data_[db_name].clear();
  }
  
  bool isFailed()
  {
    return false;
  }

  void insert(const std::string ns, const bson::BSONObj& obj)
  {
    data_[ns].push_back(obj);
  }

  unsigned int count(const std::string ns)
  {
    return data_[ns].size();
  }

  unsigned int count(const std::string ns, bson::BSONObj bson)
  {
    // FIXME
    return 0;
  }

  void remove(const std::string ns, const dummy::Query& query )
  {
    std::auto_ptr<DBClientCursor> ret = this->query(ns, query);
    while(ret->more()) {
      bson::BSONObj bson = ret->nextSafe();
      auto result = remove_if(data_[ns].begin(), data_[ns].end(), [&](bson::BSONObj &d) {
          return ( d["_id"] == bson["_id"] );
        });
      data_[ns].erase(result, data_[ns].end()); // erase removed data from list
    }
  }
    
  void update(const std::string ns, const dummy::Query& query, const bson::BSONObj metadata)
  {
    std::set<std::string> metadata_keys;
    metadata.getFieldNames(metadata_keys);
    
    std::auto_ptr<DBClientCursor> ret = this->query(ns, query);
    while(ret->more()) {
      bson::BSONObj bson = ret->nextSafe();
      for(auto &d : data_[ns]) {
        if ( d["_id"] == bson["_id"] ) {
          // replace
          bson::BSONObjBuilder b;
          bson::BSONForEach(e, d) {
            if ( metadata.getField(e.fieldName()).ok() ) {
              b.append(metadata.getField(e.fieldName()));
            } else {
              b.append(e);
            }
          }
          d = b.obj();
        }
      }
    }
    return;
  }

  std::auto_ptr<DBClientCursor> query(const std::string ns, const dummy::Query query)
  {
    std::vector<bson::BSONObj> data_ns = data_[ns];
    std::auto_ptr<DBClientCursor> c(new DBClientCursor(this, ns , query.obj));
    for(auto& d : data_ns) {
        bool ret = true;
        bson::BSONForEach(q, query.obj) {
          if(q.isABSONObj()) { //
            bson::BSONObjBuilder b;
            bson::appendElementHandlingGtLt(b, q);
            bson::BSONObj e = b.obj();
            int comp = d.extractFields(e).woCompare(e);
            if ( q.Obj().getField( "$lt" ).ok() ) {
              ret = ret and (( comp < 0 ) ? true : false);
            }else if ( q.Obj().getField( "$lte" ).ok() ) {
              ret = ret and (( comp <= 0 ) ? true : false);
            }else if ( q.Obj().getField( "$gt" ).ok() ) {
              ret = ret and (( comp > 0 ) ? true : false);
            }else if ( q.Obj().getField( "$gte" ).ok() ) {
              ret = ret and (( comp >= 0 ) ? true : false);
            }else if ( std::string(q.fieldName()) == "$orderby" ) {
            }else {
              ROS_ERROR_STREAM(__PRETTY_FUNCTION__ << " : Unsupported query [" << q.toString() << "]");
            }
          }else{
            ret = ret and ((d.extractFields(q.wrap()).woCompare(q.wrap()) == 0) ? true : false);
          }
        }
        if (ret) {
          c->putBack(d);
        }
    };
    if ( query.obj.hasField("$orderby") ) {
      // FIXME
      std::vector<bson::BSONObj > tmp;
      while ( c->more() ) tmp.push_back(c->next());
      bson::BSONForEach(o, query.obj.getField("$orderby").Obj()) {
        std::sort(tmp.begin(), tmp.end(), [&](const bson::BSONObj &a, const bson::BSONObj &b){
            if ( o.numberInt() > 0 )
              return a.getField(o.fieldName()).woCompare(b.getField(o.fieldName())) > 0;
            else
              return a.getField(o.fieldName()).woCompare(b.getField(o.fieldName())) < 0;
        });
      }
      for(auto &a : tmp) c->putBack(a);
    }
    return c;
  }

private:
  std::map<std::string, std::vector<bson::BSONObj> > data_;
};

}

#endif
