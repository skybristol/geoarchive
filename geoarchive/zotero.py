import os
from pyzotero import zotero
import pandas as pd
import json
import pycountry

class Zot:
    if "ZOTERO_LIBRARY_ID" in os.environ:
        library_id = os.environ["ZOTERO_LIBRARY_ID"]
    if "ZOTERO_API_KEY" in os.environ:
        api_key = os.environ["ZOTERO_API_KEY"]

    def __init__(self, library_id, api_key):
        """All of our interactions here require a Zot instance that connects to a specific library ID
        with an API key that supports read/write access. Initiating a Zot instance establishes the Zotero
        API connection (self.z), passes on the library_id, and pulls up connection information used to
        determine if the connection has write access in several other functions (self.cnxn).

        Args:
            library_id (str): Alphanumeric identifier for a particular library
            api_key (str): API key providing access to one or more libraries
        """
        try:
            self.z = zotero.Zotero(
                library_id=library_id,
                library_type='group',
                api_key=api_key,
            )
            self.library_id = library_id
            self.cnxn = self.z.key_info()
        except Exception as e:
            raise ValueError(f"Problem creating connection to Zotero API: {e}")

    def inventory_item_key(self):
        """If necessary, this function can be used to identify the item containing the inventory cache in the library
        using a specific convention on the item's title - "Inventory:<library_id>".

        Returns:
            str: Key (identifier) of the library item providing the cached inventory of items, collections, and tags.
        """
        inventory_item_title = f"Inventory:{self.library_id}"
        inventory_items = self.z.items(q=inventory_item_title, itemType="document")
        inventory_item = next((i for i in inventory_items if i["data"]["title"] == inventory_item_title), None)
        if inventory_item is None:
            return

        return str(inventory_item["key"])

    def get_inventory_files(self, inventory_item=None):
        """To facilitate efficient access to larger libraries, we established a method of building and maintaining
        a specific item in the library where we cache inventory data as JSON files which can be retrieved in lieu
        of what can be a time-consuming pull of the entire recordset via the API. If the inventory item is not
        explicitly provided, the inventory_item_key() function is used to try and find it.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library. Defaults to None.

        Returns:
            list: List of dictionaries from the Zotero API containing the inventory item and its files
        """
        if inventory_item is None:
            inventory_item = self.inventory_item_key()
            if inventory_item is None:
                return
        
        inventory_files = self.z.children(inventory_item, itemType="attachment")
        inventory_file_items = {
            "inventory_item": self.z.item(inventory_item),
            "inventory_item_keys": [inventory_item] + [i["key"] for i in inventory_files]
        }
        for file_type in ["items","collections","tags"]:
            inventory_file_items[file_type] = next(
                (
                    i for i in inventory_files 
                    if i["data"]["filename"] == f"{file_type}.json"
                ), None)
            
        return inventory_file_items

    def load_raw_inventory(self, inventory_item):
        """This function loads the raw inventory information for items, collections, and tags stored in the inventory
        item as JSON files.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library

        Returns:
            dict: Dictionary containing lists for each set of cached records - items, collections, and tags
        """
        inventory_files = self.get_inventory_files(inventory_item)
        if inventory_files is None:
            raise ValueError("The inventory item could not be found in the library.")

        inventory = {
            "inventory_item_keys": inventory_files["inventory_item_keys"],
            "inventory_files": inventory_files
        }
        for file_type in ["items","collections","tags"]:
            raw_data = self.z.file(inventory_files[file_type]["key"])
            inventory[file_type] = raw_data
        
        return inventory

    def load_df_inventory(self, raw_inventory, tag_delimiter=":"):
        """This function takes the raw inventory dict containing three sets of records for items, collections,
        and tags and builds Pandas dataframes for further processing. We do some minimal processing on tags
        to split these out using a ":" delimiter by default.

        Args:
            raw_inventory (dict): Dictionary contanining three keys with lists for items, collections, and tags
            from load_raw_inventory()
            tag_delimiter (str, optional): Delimiter to use in splitting tags into classes. Defaults to ":".

        Returns:
            dict: Dictionary containing three dataframes for items, collections, and tags
        """
        inventory = {}
        for k,v in raw_inventory.items():
            if k == "tags":
                df_tags = pd.DataFrame([
                    {
                        "tag": i,
                        "type": i.split(tag_delimiter)[0],
                        "value": i.split(tag_delimiter)[-1]
                    } for i in v if tag_delimiter in i
                ])
                inventory["tags"] = df_tags.convert_dtypes()
            elif k in ["items","collections"]:
                inventory[k] = pd.DataFrame([i["data"] for i in v]).convert_dtypes()

        return inventory

    def baseline_tags(self, output_path="data"):
        """This function gets all tags for a library and dumps them as a JSON file to a specified output path.

        Args:
            output_path (str, optional): Relative or absolute path to output json file. Defaults to "data".
        """
        tags = self.z.everything(self.z.tags())
        json.dump(tags, open(f"{output_path}/tags.json", "w"))

    def baseline_collections(self, output_path="data"):
        """This function gets all collections for a library and dumps them as a JSON file to a specified output path.

        Args:
            output_path (str, optional): Relative or absolute path to output json file. Defaults to "data".
        """
        records = self.z.everything(self.z.collections())
        json.dump(records, open(f"{output_path}/collections.json", "w"))

    def baseline_items(self, inventory_item=None, output_path="data"):
        """This function gets all items (metadata and files) for a library and dumps them as a JSON file to a 
        specified output path. It will strip out any items associated with the inventory item.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library
            output_path (str, optional): Relative or absolute path to output json file. Defaults to "data".
        """
        records = self.z.everything(self.z.items())
        
        inventory_files = self.get_inventory_files(inventory_item)
        if inventory_files is None:
            raise ValueError("The inventory item could not be found in the library.")

        records = [
            i for i in records 
            if i["key"] not in inventory_files["inventory_item_keys"]
        ]
        json.dump(records, open(f"{output_path}/items.json", "w"))

    def baseline_cache(
        self, 
        inventory_item=None, 
        output_path="data"
    ):
        """This function takes the files found in a specified output_path (from baseline or update) for
        items, collections, and tags and uploads these to the specified inventory item in the library.
        This function can only be successfully operated using an API key with write permissions.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library
            output_path (str, optional): Relative or absolute path to output json file. Defaults to "data".

        Returns:
            bool: False if the API key does not have permission to write to the library
        """
        if not self.cnxn["access"]["user"]["write"]:
            return False

        inventory_files = self.get_inventory_files(inventory_item)
        if inventory_files is None:
            raise ValueError("The inventory item could not be found in the library.")

        new_uploads = []
        for file_type in ["items","collections","tags"]:
            if os.path.exists(f"{output_path}/{file_type}.json"):
                if inventory_files[file_type] is not None:
                    self.z.delete_item(self.z.item(inventory_files[file_type]["key"]))
                    print("Deleted existing cache file for", file_type)
                new_uploads.append(os.path.abspath(f"{output_path}/{file_type}.json"))
            
        if new_uploads:
            inventory_item_update = {
                "key": inventory_item,
                "version": inventory_files["inventory_item"]["data"]["version"],
                "extra": self.z.last_modified_version()
            }
            self.z.update_item(inventory_item_update)
            print("Updated inventory item with last modified version for the library")
            
            self.z.attachment_simple(new_uploads, parentid=inventory_item)
            print("Created new files in inventory", new_uploads)

    def update_inventory(self, inventory_item=None, output_path="data"):
        """This function handles a periodic refresh of the cached inventory. It reads the current inventory
        files, determines the last version of the library cached, retrieves new records (items, collections, tags),
        and updates the cached files. This function can only be successfully operated using an API key with write
        permissions.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library
            output_path (str, optional): Relative or absolute path to output json file. Defaults to "data".

        Returns:
            bool: False if the API key does not have permission to write to the library
        """
        if not self.cnxn["access"]["user"]["write"]:
            return False

        inventory_files = self.get_inventory_files(inventory_item)
        if inventory_files is None:
            raise ValueError("The inventory item could not be found in the library.")

        raw_inventory = self.load_raw_inventory(inventory_item)

        cache_item_version = max([i["version"] for i in raw_inventory["items"]])
        cache_collection_version = max([i["version"] for i in raw_inventory["collections"]])
        min_cache_version = min([cache_item_version, cache_collection_version])

        deletions = self.z.deleted(since=min_cache_version)
        
        new_inventory = {}
        for x in ["items","collections"]:
            new_inventory[x] = [
                i for i in raw_inventory[x] if i["key"] not in deletions[x]
            ]
        new_inventory["tags"] = [
            i for i in raw_inventory["tags"] if i not in deletions["tags"]
        ]
        
        new_items = self.z.everything(self.z.items(since=cache_item_version))
        new_collections = self.z.everything(self.z.collections(since=cache_collection_version))
        new_tags = self.z.everything(self.z.tags(since=cache_item_version))
        
        if new_tags:
            new_inventory["tags"].extend(new_tags)
            new_inventory["tags"] = list(set(new_inventory["tags"]))
            
        if new_collections:
            new_inventory["collections"] = [
                i for i in new_inventory["collections"]
                if i["key"] not in [
                    x["key"] for x in new_collections
                ]
            ]
            new_inventory["collections"].extend(new_collections)
            
        if new_items:
            items_wo_inventory = [
                i for i in new_items 
                if i["key"] not in raw_inventory["inventory_item_keys"]
            ]
            if items_wo_inventory:
                new_inventory["items"] = [
                    i for i in new_inventory["items"]
                    if i["key"] not in [
                        x["key"] for x in new_items
                    ]
                ]
                new_inventory["items"].extend(items_wo_inventory)
        
        new_uploads = []
        for x in ["items","collections","tags"]:
            if raw_inventory[x] != new_inventory[x]:
                fp = f"{output_path}/{x}.json"
                json.dump(new_inventory[x], open(fp, "w"))
                print(f"WROTE {len(new_inventory[x])} RECORDS TO {fp}")
                
                cache_file_key = raw_inventory["inventory_files"][x]["key"]
                self.z.delete_item(self.z.item(cache_file_key))
                print(f"DELETED PREVIOUS CACHE FILE FOR {x}: {cache_file_key}")
                new_uploads.append(os.path.abspath(fp))

        if new_uploads:
            inventory_item_update = {
                "key": inventory_item,
                "version": inventory_files["inventory_item"]["data"]["version"],
                "extra": self.z.last_modified_version()
            }
            self.z.update_item(inventory_item_update)
            print("Updated inventory item with last modified version for the library")
            
            self.z.attachment_simple(new_uploads, parentid=inventory_item)
            print("Created new files in inventory", new_uploads)
            
    def build_identifier(self, record):
        """Builds identifier objects for a particular type of export format.

        Args:
            record (series): Dataframe series (record)

        Returns:
            list: List of identifier objects/dicts
        """
        identifiers = []
        identifiers.append({
            "type": "Zotero Item Key",
            "id": record.key
        })
        identifiers.append({
            "type": "Zotero Attachment Key",
            "id": record.file_key
        })
        return identifiers

    def build_link(self, record):
        """Builds link objects for a particular type of export format.

        Args:
            record (series): Dataframe series (record)

        Returns:
            list: List of link objects/dicts
        """
        items_api = f"https://api.zotero.org/groups/{self.library_id}/items"
        items_ui = f"https://www.zotero.org/groups/{self.library_id}/items"

        links = []
        links.append({
            "type": "source_item",
            "link": f"{items_api}/{record.key}"
        })
        links.append({
            "type": "source_file",
            "link": f"{items_api}/{record.file_key}"
        })
        links.append({
            "type": "landing_page",
            "link": f"{items_ui}/{record.key}"
        })
        return links

    def collection_tags(self, geo_tags=True, custom_tags=None):
        """This is a fairly specialized function that interprets collections from the Zotero library to turn these
        into additional type-classified tags. 

        Args:
            geo_tags (bool, optional): Specifies whether or not to check collections for geo names. Defaults to True.
            custom_tags (list, optional): List of lists of specific types of additional tags that should be found within
            the collection structure of a library. Defaults to None.

        Returns:
            list: List of additional tags as compound, type-classified strings containing a type and term.
        """
        raw_collections = self.z.everything(self.z.collections())

        collection_tags = []

        if isinstance(custom_tags, list):
            for tag_list in custom_tags:
                for t in tag_list:
                    matching_collection = next((i for i in raw_collections if i["data"]["name"] == t.split(":")[-1]), None)
                    if matching_collection:
                        collection_tags.append({
                            "collection": matching_collection["key"],
                            "tag": t
                        })

        if geo_tags:
            country_names = [i.name for i in pycountry.countries]
            us_states = [i.name for i in pycountry.subdivisions.get(country_code='US')]

            collection_tags.extend([{
                "collection": i["key"],
                "tag": f"country:{i['data']['name']}"
            } for i in raw_collections if i["data"]["name"] in country_names])
            
            collection_tags.extend([{
                "collection": i["key"],
                "tag": f"us_state:{i['data']['name']}"
            } for i in raw_collections if i["data"]["name"] in us_states])

        return collection_tags

    def item_export(self, inventory_item=None, target_schema="xdd", output_format="dataframe"):
        """This function packages the items for a given library using the cached inventory files and returns them for
        some particular use. We need to better work up the configuration details for this so that we can accept some
        form of configuration principals that are used to determine the appropriate mappings or transformations.

        Args:
            inventory_item (str): Alphanumeric identifier for the inventory item in a given library
            target_schema (str, optional): The particular target output being transformed to. Defaults to "xdd".
            output_format (str, optional): Determines the output format for the transformation. Defaults to "dataframe".

        Returns:
            dataframe/list: Either a dataframe (default) containing the transformation or a list of dicts.
        """
        try:
            raw_inventory = self.load_raw_inventory(inventory_item)
        except ValueError as e:
            raise ValueError(e)

        df_inventory = self.load_df_inventory(raw_inventory)

        items = df_inventory["items"]

        if target_schema == "xdd":
            region_tags = [
                "geo_region:Africa",
                "geo_region:Asia",
                "geo_region:Australasia",
                "geo_region:Caribbean",
                "geo_region:Central America",
                "geo_region:Europe",
                "geo_region:Middle East",
                "geo_region:North America",
                "geo_region:Oceania",
                "geo_region:South America",
            ]

            c_tags = self.collection_tags(custom_tags=[region_tags])

            reports = items[items.itemType == "report"][["key","title","date","tags","collections"]].rename(columns={"date":"year"}).copy()

            report_tags = reports[["key","tags"]].explode("tags")
            report_tags["tag"] = report_tags.tags.apply(lambda x: x["tag"] if isinstance(x, dict) else None)
            report_collections = reports[["key","collections"]].explode("collections").rename(columns={"collections":"collection"})
            report_collections = report_collections[report_collections.collection.notnull()]

            all_tags_mapping = pd.concat([
                pd.merge(
                    left=report_collections,
                    right=pd.DataFrame(c_tags),
                    how="left",
                    on="collection"
                )[["key","tag"]],
                report_tags[["key","tag"]]
            ])
            key_tags = all_tags_mapping.groupby(["key"], as_index=False).agg(list).rename(columns={"tag":"tags"})

            reports_with_tags = pd.merge(
                left=reports[["key","title","year"]],
                right=key_tags,
                how="left",
                on="key"
            )

            files = items[items.itemType == "attachment"][["key","parentItem","filename"]].rename(columns={"key": "file_key"}).copy()

            reports_with_files = pd.merge(
                left=reports_with_tags,
                right=files,
                how="left",
                left_on="key",
                right_on="parentItem"
            )

            reports_with_files["publisher"] = "Canadian Securities Administrators"
            reports_with_files["identifier"] = reports_with_files.apply(lambda x: self.build_identifier(x), axis=1)
            reports_with_files["link"] = reports_with_files.apply(lambda x: self.build_link(x), axis=1)

        if output_format == "dataframe":
            return reports_with_files
        else:
            return reports_with_files.to_dict(orient="records")