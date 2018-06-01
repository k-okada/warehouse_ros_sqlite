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
 * Implementation of database_connection.h
 *
 * \author Bhaskara Marthi
 */

#include <warehouse_ros_sqlite/database_connection.h>
#include <pluginlib/class_list_macros.h>

namespace warehouse_ros_sqlite
{

using std::string;

SQLiteDatabaseConnection::SQLiteDatabaseConnection() :
  host_("localhost"),
  port_(27017),
  timeout_(60.0)
{
}

bool SQLiteDatabaseConnection::setParams(const string& host, unsigned port, float timeout)
{
  host_ = host;
  port_ = port;
  timeout_ = timeout;
  return true;
}

bool SQLiteDatabaseConnection::setTimeout(float timeout)
{
  timeout_ = timeout;
  return true;
}

bool SQLiteDatabaseConnection::connect()
{
  const string db_address = (boost::format("%1%:%2%") % host_ % port_).str();
  const ros::WallTime end = ros::WallTime::now() + ros::WallDuration(timeout_);

  while (ros::ok() && ros::WallTime::now()<end)
  {
    conn_.reset(new sqlite::DBClientConnection());
    try
    {
      ROS_DEBUG_STREAM_NAMED("db_connect", "Attempting to connect to SQLiteDB at " << db_address);
      conn_->connect(db_address);
      if (!conn_->isFailed())
        break;
    }
    catch (warehouse_ros::DbConnectException& e)
    {
      ros::Duration(1.0).sleep();
    }
  }
  if (conn_->isFailed())
  {
    ROS_ERROR_STREAM("Unable to connect to the database at '" << db_address << "'. If you just created the database, it could take a while for initial setup.");
    return false;
  }

  ROS_DEBUG_STREAM_NAMED("db_connect", "Successfully connected to the DB");
  return true;
}

bool SQLiteDatabaseConnection::isConnected()
{
  return ((bool)conn_ && !conn_->isFailed());
}

void SQLiteDatabaseConnection::dropDatabase(const string& db_name)
{
  if (!isConnected())
    throw warehouse_ros::DbConnectException("Cannot drop database");
  conn_->dropDatabase(db_name);
}

string SQLiteDatabaseConnection::messageType(const string& db, const string& coll)
{
  if (!isConnected())
    throw warehouse_ros::DbConnectException("Cannot look up metatable.");
  const string meta_ns = db+".ros_message_collections";
  std::auto_ptr<sqlite::DBClientCursor> cursor = conn_->query(meta_ns, BSON("name" << coll));
  bson::BSONObj obj = cursor->next();
  return obj.getStringField("type");
}

MessageCollectionHelper::Ptr SQLiteDatabaseConnection::openCollectionHelper(const std::string& db_name,
                                                                            const std::string& collection_name)
{
  return typename MessageCollectionHelper::Ptr(new SQLiteMessageCollection(conn_, db_name, collection_name));
}

} // namespace

PLUGINLIB_EXPORT_CLASS( warehouse_ros_sqlite::SQLiteDatabaseConnection, warehouse_ros::DatabaseConnection )
