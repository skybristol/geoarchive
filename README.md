# GeoArchive

The GeoArchive is a loosely coupled architecture of interoperating diverse repositories containing important geoscientific data, information, and knowledge content. Through a software layer, the GeoArchive coordinates actions across these repositories to ensure their near and long-term viability as scientific assets. This Python package is one element of the software layer.

## Changelog

### Version 0.2.0
* Major change from previous generation based on refactoring how the GeoArchive is structured. The new version accounts for the shift to USGS ScienceBase as the primary store for file contents with Zotero serving as a metadata outlet and the Geoscience Knowledgebase holding the most robust representation of the documents and extracted (meta)data.
* Introduced the sciencebase and geokb modules with sciencebase driving the basic workflow for processing files, distributing content to Zotero and the GeoKB.
* The Zotero module now focuses on the simple process of setting up public bibliographic citation records for reports.
* The GeoKB module handles the creation or update of items in the knowledge graph representing a GeoArchive document and its statements, including the creation/linking of new entities representing the companies operating a mineral development project in the case of NI 43-101 reports.

Note: Much of the functionality at this stage is closely associated with the NI 43-101 technical reports. However, as we shifted to using schema.org standards for encoding metadata at the root of the process, the system can accommodate new types of GeoArchive collections with some modifications. The major business logic occurs at the point of how the schema.org/CreativeWork documents are digested to produce entities and claims in the GeoKB. The Zotero module will need to be updated through time based on type classification of other GeoArchive collections. The current iteration deals with itemType=report, and this may need to become more sophisticated to deal with other types of materials.

## Installing

```bash
pip install geoarchive
```

## Dependencies

### Software packages

A number of specific packages are required to run geoarchive. These are included in the pyproject.toml file and will be installed as needed when you install the package. The main dependencies are:

* sciencebasepy for authenticated interactions with ScienceBase
* wbmaker for authenticated interactions with the Geoscience Knowledgebase
* pyzotero for authenticated interactions with Zotero

### Environment variables/secrets

Each module includes an initial check for required environment variables that must be in place in order for the module to operate. Variables include the following:

* SB_ACCESS_TOKEN: ScienceBase access token
* SB_REFRESH_TOKEN: ScienceBase refresh token
* ZOTERO_API_KEY: Zotero API key issued to a user account with appropriate access rights to the target Zotero group library
* WB_SPARQL_ENDPOINT: URL for the Geoscience Knowledgebase SPARQL endpoint (https://geokb.wikibase.cloud/query/sparql)
* WB_URL: URL for the Geoscience Knowledgebase (https://geokb.wikibase.cloud)
* MEDIAWIKI_API: URL for the Geoscience Knowledgebase MediaWiki API (https://geokb.wikibase.cloud/w/api.php)
* WB_BOT_USER_AGENT: User agent for the Geoscience Knowledgebase bot user (e.g., 'geoarchive/0.2.0 (<email address>)')
* WB_BOT_USER: Geoscience Knowledgebase bot user name created by a user with appropriate access rights
* WB_BOT_PASS: Geoscience Knowledgebase bot user password created by a user with appropriate access rights

## Disclaimer

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.