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
        self.z = zotero.Zotero(
            library_id=library_id,
            library_type='group',
            api_key=api_key,
        )
        self.library_id = library_id

    def connection_info(self):
        return self.z.key_info()

    def get_inventory_files(self, inventory_item):
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
        existing_files = self.get_inventory_files(inventory_item)

        inventory = {
            "inventory_item_keys": existing_files["inventory_item_keys"],
            "inventory_files": existing_files
        }
        for file_type in ["items","collections","tags"]:
            raw_data = self.z.file(existing_files[file_type]["key"])
            inventory[file_type] = raw_data
        
        return inventory

    def load_df_inventory(self, raw_inventory):
        inventory = {}
        for k,v in raw_inventory.items():
            if k == "tags":
                df_tags = pd.DataFrame([
                    {
                        "tag": i,
                        "type": i.split(":")[0],
                        "value": i.split(":")[-1]
                    } for i in v if ":" in i
                ])
                inventory["tags"] = df_tags.convert_dtypes()
            elif k in ["items","collections"]:
                inventory[k] = pd.DataFrame([i["data"] for i in v]).convert_dtypes()

        return inventory

    def baseline_tags(self, output_path="data"):
        tags = self.z.everything(self.z.tags())
        json.dump(tags, open(f"{output_path}/tags.json", "w"))

    def baseline_collections(self, output_path="data"):
        records = self.z.everything(self.z.collections())
        json.dump(records, open(f"{output_path}/collections.json", "w"))

    def baseline_items(self, inventory_item, output_path="data"):
        records = self.z.everything(self.z.items())
        
        inventory_files = self.get_inventory_files(inventory_item)
        records = [
            i for i in records 
            if i["key"] not in inventory_files["inventory_item_keys"]
        ]
        json.dump(records, open(f"{output_path}/items.json", "w"))

    def baseline_cache(
        self, 
        inventory_item, 
        output_path="data"
    ):
        inventory_files = self.get_inventory_files(inventory_item)

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

    def update_inventory(self, inventory_item, output_path="data"):
        inventory_files = self.get_inventory_files(inventory_item)
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

    def item_export(self, inventory_item, target_schema="xdd", collection_tag_mapping=None, output_format="dataframe"):
        raw_inventory = self.load_raw_inventory(inventory_item)
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