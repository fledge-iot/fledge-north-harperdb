#ifndef _HARPERDB_H
#define _HARPERDB_H
/*
 * Fledge HarperDB north plugin.
 *
 * Copyright (c) 2020 
 *      
 * Released under the Apache 2.0 Licence
 *
 * Author: 
 */
#include <string>
#include <vector>
#include <set>
#include <reading.h>
#include <simple_https.h>
#include <simple_http.h>

/**
 * The class that is used to implement a north plugin that can send data to
 * the HarperDB REST API and insert Fledge data into tables within HarperDB.
 */
class HarperDB
{
	public:
		HarperDB(const std::string& url, const std::string& schema);
		~HarperDB();
		void		authenticate(const std::string& user, const std::string& password);
		void		connect();
		uint32_t	send(const std::vector<Reading *> readings);
	private:
		bool		createTable(const std::string& table);
		bool		createSchema(const std::string& schema);
		bool		post(const std::string& payload);
		bool		m_isHTTPS;
		bool		m_firstBlock;
		SimpleHttps	*m_https;
		SimpleHttp	*m_http;
		std::string	m_url;
		std::string	m_schema;
		std::set<std::string>
				m_tables;
		std::vector<std::pair<std::string, std::string> >
				m_headers;
};
#endif
