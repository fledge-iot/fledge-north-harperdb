/*
 * Fledge HarperDB north plugin.
 *
 * Copyright (c) 2020 HarperDB
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: 
 */
#include <harperdb.h>
#include <logger.h>
#include <crypto.hpp>

using namespace	std;

HarperDB::HarperDB(const string& url, const string& schema) :
	m_url(url), m_schema(schema), m_https(0), m_http(0), m_isHTTPS(false), m_firstBlock(true)
{
	m_headers.push_back(pair<string, string>("Content-Type", "application/json"));
}

/**
 * Destructor for the plugin
 */
HarperDB::~HarperDB()
{
	if (m_https)
	{
		delete m_https;
	}
	if (m_http)
	{
		delete m_http;
	}
}

/**
 * Generate the basic authentication credentials and append to the headers
 *
 * @param user	The username
 * @param password	The password for the user
 */
void
HarperDB::authenticate(const string& user, const string& password)
{
        string credentials;

        credentials = SimpleWeb::Crypto::Base64::encode(user + ":" + password);
	string method = "b";
	if (m_isHTTPS)
	{
		m_https->setAuthMethod(method);
		m_https->setAuthBasicCredentials(credentials);
	}
	else
	{
		m_http->setAuthMethod(method);
		m_http->setAuthBasicCredentials(credentials);
	}
}

/**
 * Create the HTTP connection to the HarperDB REST  API
 */
void
HarperDB::connect()
{
	/**
	 * Extract host and port from URL
	 */
	size_t findProtocol = m_url.find_first_of(":");
	string protocol = m_url.substr(0,findProtocol);
	if (protocol.compare("https") == 0)
	{
		m_isHTTPS = true;
	}

	string tmpUrl = m_url.substr(findProtocol + 3);
	size_t findPort = tmpUrl.find_first_of(":");
	size_t findPath = tmpUrl.find_first_of("/");
	string port, hostName;
	if (findPort == string::npos)
	{
		hostName = tmpUrl.substr(0, findPath);
		Logger::getLogger()->info("Connect to host %s with %s", hostName.c_str(), protocol.c_str());
		if (m_isHTTPS)
		{
			m_https  = new SimpleHttps(hostName);
		}
		else
		{
			m_http  = new SimpleHttp(hostName);
		}
	}
	else
	{
		hostName = tmpUrl.substr(0, findPort);
		port = tmpUrl.substr(findPort + 1 , findPath - findPort -1);
		string hostAndPort(hostName + ":" + port);
		Logger::getLogger()->info("Connect to host %s, port %s with %s", hostName.c_str(), port.c_str(), protocol.c_str());
		if (m_isHTTPS)
		{
			m_https  = new SimpleHttps(hostAndPort);
		}
		else
		{
			m_http  = new SimpleHttp(hostAndPort);
		}
	}
}

/**
 * Send the readings to the HarperDB channel
 *
 * @param readings	The Readings to send
 * @return		The number of readings sent
 */
uint32_t
HarperDB::send(const vector<Reading *> readings)
{
ostringstream	payload;
int sent = 0;

	if (m_firstBlock)
	{
		// The first tiem we send data we do a create schema in case this schema does not exist
		createSchema(m_schema);
		m_firstBlock = false;
	}
	for (auto it = readings.cbegin(); it != readings.cend(); ++it)
	{
		string assetName = (*it)->getAssetName();
		if (m_tables.find(assetName) == m_tables.end())
		{
			// New table found, we create the table but ignore the result as it may already exist
			createTable(assetName);
			m_tables.emplace(assetName);
		}
	}
	for (auto tableItr = m_tables.cbegin(); tableItr != m_tables.cend(); ++tableItr)
	{
		string table = *tableItr;
		bool	first = true;
		for (auto it = readings.cbegin(); it != readings.cend(); ++it)
		{
			string assetName = (*it)->getAssetName();
			if (assetName.compare(table) == 0)
			{
				if (first)
				{
					payload << "{ ";
					payload << "\"operation\":\"insert\",";
					payload << "\"schema\":\"" + m_schema + "\",";
					payload << "\"table\":\"" + table + "\",";
					payload << "\"records\":[";
					first = false;
				}
				else
				{
					payload << ",";
				}
				payload << "{";
				payload << "\"id\" : ";
				char idStr[40];
				snprintf(idStr, sizeof(idStr), "\"%lu\", ", (*it)->getId());
				payload << idStr;
				payload << "\"timestamp\" : \"" + (*it)->getAssetDateUserTime() + "\", ";
				vector<Datapoint *> datapoints = (*it)->getReadingData();
				bool comma = false;
				for (auto dit = datapoints.cbegin(); dit != datapoints.cend();
							++dit)
				{
					if (comma)
					{
						payload << ",";
					}
					else
					{
						comma = true;
					}
					payload << (*dit)->toJSONProperty();
				}
				payload << "}";
			}
		}
		if (!first)
		{
			payload << "]}";

			if (post(payload.str()))
			{
				sent += readings.size();
			}
		}
	}
	return sent;
}


/**
 * Create a new table
 *
 * @param table	The name of the table to create
 */
bool
HarperDB::createTable(const string& table)
{
ostringstream	payload;

	Logger::getLogger()->info("Creating table %s in schema %s", table.c_str(), m_schema.c_str());

	payload << "{ \"operation\" : \"create_table\", \"schema\" : \"" + m_schema + "\", ";
	payload << "\"table\" : \"" + table + "\", \"hash_attribute\" : \"id\" }";

	return post(payload.str());
}


/**
 * Create a new schema
 *
 * @param schema	The name of the schema to create
 */
bool
HarperDB::createSchema(const string& schema)
{
ostringstream	payload;

	Logger::getLogger()->info("Creating schema %s", schema.c_str());

	payload << "{ \"operation\" : \"create_schema\", \"schema\" : \"" + schema + "\" }";

	return post(payload.str());
}

/**
 * POST a request to the HarperDB REST API
 *
 * @param payload	The payload to send
 * @return 		Returns true if the POST was accepted
 */
bool
HarperDB::post(const string& payload)
{
	try {
		int errorCode;
		if (m_isHTTPS)
		{
			errorCode = m_https->sendRequest("POST", m_url, m_headers, payload);
		}
		else
		{
			errorCode = m_http->sendRequest("POST", m_url, m_headers, payload);
		}
		if (errorCode == 200 || errorCode == 202)
		{
			return true;
		}
		else
		{
			Logger::getLogger()->error("Failed to send to HarperDB %s, errorCode %d", m_url.c_str(), errorCode);
			if (m_isHTTPS)
				Logger::getLogger()->error("HTTP response: %s for payload %s", m_https->getHTTPResponse().c_str(), payload.c_str());
			else
				Logger::getLogger()->error("HTTP response: %s for payload %s", m_http->getHTTPResponse().c_str(), payload.c_str());
		}
	} catch (runtime_error& re) {
		Logger::getLogger()->error("Failed to send to HarperDB %s, runtime error: %s", m_url.c_str(), re.what());
	} catch (exception& e) {
		Logger::getLogger()->error("Failed to send to HarperDB %s, exception occured: %s", m_url.c_str(), e.what());
	} catch (...) {
		Logger::getLogger()->error("Failed to send to HarperDB %s, unexpected exception occured", m_url.c_str());
	}
	return false;
}
