/*
 * Fledge HarperDB  north plugin.
 *
 * Copyright (c) 2020 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: 
 */
#include <plugin_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string>
#include <logger.h>
#include <plugin_exception.h>
#include <iostream>
#include <harperdb.h>
#include <simple_https.h>
#include <config_category.h>
#include <storage_client.h>
#include <rapidjson/document.h>
#include <version.h>

using namespace std;
using namespace rapidjson;

#define PLUGIN_NAME "HarperDB"

/**
 * Plugin specific default configuration
 */
static const char *default_config = QUOTE({
		"plugin": {
			"description": "HarperDB North",
			"type": "string",
			"default": PLUGIN_NAME,
			"readonly": "true"
			},
		"URL": {
			"description": "The URL of the HarperDB service",
			"type": "string",
			"default": "http://localhost:9925/",
			"order": "1",
			"displayName": "URL"
			},
		"username": {
			"description": "The username within HarperDB",
			"type": "string",
			"default": "",
			"order": "2",
			"displayName": "Username"
			},
		"password": {
			"description": "The password for this user",
			"type": "password",
			"default": "",
			"order": "3",
			"displayName": "Password"
			},
		"schema": {
			"description": "The HarperDB schema to use for the tables",
			"type": "string",
			"default": "fledge",
			"order": "4",
			"displayName": "Schema"
			},
		"source": {
			"description": "Defines the source of the data to be sent on the stream",
			"type": "enumeration",
			"default": "readings",
			"options": ["readings", "statistics"],
		       	"order": "5",
			"displayName": "Source"
			}
	});


/**
 * The HarperDB plugin interface
 */
extern "C" {

/**
 * The C API plugin information structure
 */
static PLUGIN_INFORMATION info = {
	PLUGIN_NAME,			// Name
	VERSION,			// Version
	0,				// Flags
	PLUGIN_TYPE_NORTH,		// Type
	"1.0.0",			// Interface version
	default_config			// Configuration
};

/**
 * Return the information about this plugin
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &info;
}

/**
 * Initialise the plugin with configuration.
 *
 * This function is called to get the plugin handle.
 */
PLUGIN_HANDLE plugin_init(ConfigCategory* configData)
{
	if (!configData->itemExists("URL"))
	{
		Logger::getLogger()->fatal("HarperDB plugin must have a URL defined for the ThinkSpeak API");
		throw exception();
	}
	string url = configData->getValue("URL");
	if (!configData->itemExists("schema"))
	{
		Logger::getLogger()->fatal("HarperDB plugin must have a schema defined");
		throw exception();
	}
	string schema = configData->getValue("schema");

	HarperDB *harperDB = new HarperDB(url, schema);
	harperDB->connect();
	harperDB->authenticate(configData->getValue("username"), configData->getValue("password"));

	Logger::getLogger()->info("HarperDB plugin configured: URL=%s, "
				  "schema=%s",
				  url.c_str(),
				  schema.c_str());

	return (PLUGIN_HANDLE)harperDB;
}

/**
 * Send Readings data to historian server
 */
uint32_t plugin_send(const PLUGIN_HANDLE handle,
		     const vector<Reading *>& readings)
{
HarperDB	*harperDB = (HarperDB *)handle;

	return harperDB->send(readings);
}

/**
 * Shutdown the plugin
 *
 * Delete allocated data
 *
 * @param handle    The plugin handle
 */
void plugin_shutdown(PLUGIN_HANDLE handle)
{
HarperDB	*harperDB = (HarperDB *)handle;

        delete harperDB;
}

// End of extern "C"
};
