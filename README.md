# GeoArchive

The GeoArchive is a loosely coupled architecture for adopting various kinds of data, information, and knowledge repositories as archives of important scientific material that an institution (the USGS in this case) wants to adopt for long-term curation and management. The archives of the GeoArchive construct are considered "active archives" in that they may be used in practice. This Python package provides utilities for operating curation and management tasks in standardized ways across different types of third party technologies. 

Enterprise archival tasks include things like:

* Registering a repository for adoption
* Reading and checking the metadata that describe items in a repository for compliance with standards and conventions that make the materials viable for archival
* Examining, evaluating, and reporting on the digital contents of a repository, classifying materials as to their long-term viability and recommended actions for curation
* Pulling all or select materials from a participating repository into a long-term backup/storage solution
* Registering links for a repository's items through a handle system for the purpose of creating long-lasting references
* Creating additional linkages for the items in a repository and/or descriptive metadata to other information sources inside or outside the GeoArchive framework (e.g., between repositories known to the GeoArchive, between items in a GeoArchive repository and third party information systems)

Note: This is a work in progress project that has certain aspects which are highly tuned specifically to the USGS and what we are doing with this concept. We will be working to generalize functionality over time such that it may prove useful to others as well.

## Installing

```
pip install geoarchive
```

## Modules

We are designing the package to contain individual modules for interacting with different types of third party repository systems that serve as sources for GeoArchive collections. Each module has its own specific requirements and dependencies for what it needs to do to make its specific connections. All modules will likely require some type of specific connection information.

### Zotero

Our initial use case is for using a Zotero group library as a repository of document-type materials. This module leverages the pyzotero package to create an instance of an API connection to a specified Group Library in Zotero and a set of functions for working with that connection in a variety of ways. It requires a library_id and an api_key, which can be supplied through environment variables or through passed variables in instantiating the connection. Certain functionality also requires the specification of a inventory_item, which is the identifier for a specific item stored in the library that contains a cache of metadata.

