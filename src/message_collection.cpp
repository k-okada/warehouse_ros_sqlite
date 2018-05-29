/*
 * Copyright (c) 2008, Willow Garage, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Willow Garage, Inc. nor the names of its
 *       contributors may be used to endorse or promote products derived from
 *       this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 */

/**
 * \file 
 * 
 * Implementation of DummyMessageCollection
 *
 * \author Bhaskara Marthi
 */

#include <warehouse_ros_dummy/message_collection.h>
#include <std_msgs/String.h>
#include <boost/shared_ptr.hpp>
#include <boost/foreach.hpp>

namespace warehouse_ros_dummy
{

using std::string;

DummyMessageCollection::DummyMessageCollection(boost::shared_ptr<dummy::DBClientConnection> conn,
                                               const string& db,
                                               const string& coll) :
  conn_(conn),
  ns_(db+"."+coll),
  db_(db),
  coll_(coll)
{
}


bool DummyMessageCollection::initialize(const std::string& datatype, const std::string& md5)
{
  ensureIndex("creation_time");

  // Add to the metatable
  const string meta_ns = db_+".ros_message_collections";
  if (!conn_->count(meta_ns, BSON("name" << coll_)))
  {
    ROS_DEBUG_NAMED("create_collection", "Inserting info for %s into metatable", coll_.c_str());
    conn_->insert(meta_ns, BSON("name" << coll_ << "type" << datatype << "md5sum" << md5));
  }
  else if (!conn_->count(meta_ns, BSON("name" << coll_ << "md5sum" << md5)))
  {
    ROS_ERROR("The md5 sum for message %s changed to %s. Only reading metadata.", datatype.c_str(), md5.c_str());
    return false;
  }
  return true;
}

void DummyMessageCollection::ensureIndex(const string& field)
{
  ROS_ERROR("%s(%s)", __PRETTY_FUNCTION__, field.c_str()); // FIXME
  /*
#if (DUMMYCLIENT_VERSION_MAJOR >= 1)
  conn_->createIndex(ns_, BSON(field << 1));
#else
  conn_->ensureIndex(ns_, BSON(field << 1));
#endif
  */
}

void DummyMessageCollection::insert(char* msg, size_t msg_size, Metadata::ConstPtr metadata)
{
  /// Get the BSON and id from the metadata
  bson::BSONObj bson = downcastMetadata(metadata);
  bson::OID id;
  bson["_id"].Val(id);

  // Add blob id to metadata and store it in the message collection
  bson::BSONObjBuilder builder;
  builder.appendElements(bson);

  // Store in message in
  builder.appendBinData(id.toString(), msg_size, bson::BinDataType::BinDataGeneral, (const uint8_t *)msg);

  bson:BSONObj entry = builder.obj();
  ROS_DEBUG_NAMED("insert", "Inserting %s into %s", entry.toString().c_str(), ns_.c_str());
  conn_->insert(ns_, entry);
}

ResultIteratorHelper::Ptr DummyMessageCollection::query(Query::ConstPtr query,
                                                        const string& sort_by,
                                                        bool ascending) const
{
  dummy::Query mquery(downcastQuery(query));
  if (sort_by.size() > 0)
    mquery.sort(sort_by, ascending ? 1 : -1);
  ROS_DEBUG_NAMED("query", "Sending query %s to %s", mquery.toString().c_str(), ns_.c_str());
  return typename ResultIteratorHelper::Ptr(new DummyResultIterator(conn_, ns_, mquery));
}

void DummyMessageCollection::listMetadata(dummy::Query& mquery, std::vector<bson::BSONObj>& metas)
{
  DummyResultIterator iter(conn_, ns_, mquery);
  while (iter.hasData())
  {
    metas.push_back(iter.metadataRaw());
    iter.next();
  }
}

unsigned DummyMessageCollection::removeMessages(Query::ConstPtr query)
{
  dummy::Query mquery(downcastQuery(query));
  
  std::vector<bson::BSONObj> metas;
  listMetadata(mquery, metas);

  // Remove messages from db
  conn_->remove(ns_, mquery);

  unsigned num_removed = 0;
  // Also remove the raw messages from gridfs
  for (std::vector<bson::BSONObj>::iterator it = metas.begin(); it != metas.end(); ++it)
  {
    // dummy::OID id;
    // (*it)["blob_id"].Val(id);
    // gfs_->removeFile(id.toString());
    ++num_removed;
  }
  return num_removed;
}

void DummyMessageCollection::modifyMetadata(Query::ConstPtr q, Metadata::ConstPtr m)
{
  bson::BSONObj bson = downcastMetadata(m);
  dummy::Query query(downcastQuery(q));

  std::vector<bson::BSONObj> metas;
  listMetadata(query, metas);
  if (metas.size() == 0)
    throw warehouse_ros::NoMatchingMessageException(coll_);
  bson::BSONObj orig = metas.front();
  bson::BSONObjBuilder new_meta_builder;

  std::set<std::string> fields;
  bson.getFieldNames(fields);

  BOOST_FOREACH (const string& f, fields)
  {
    if ((f!="_id") && (f!="creation_time")) {
      // new_meta_builder.append(BSON("$set" << BSON(f << bson.getField(f))).\
      //                         getField("$set"));
      new_meta_builder.append(BSON(f<<bson.getField(f)).getField(f));
      //if ( bson.hasField(f) ) { FIXME
      //  new_meta[f] = bson.getField(f);
      //}
    }
  }
  bson::BSONObj new_meta = new_meta_builder.obj();
  conn_->update(ns_, query, new_meta);
}

unsigned DummyMessageCollection::count()
{
  return conn_->count(ns_);
}

string DummyMessageCollection::collectionName() const
{
  return coll_;
}

} // namespace
