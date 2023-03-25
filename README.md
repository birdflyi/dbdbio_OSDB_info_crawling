# dbdbio_OSDB_info_crawling
Crawling Open Source DataBases from [dbdb.io](https://www.dbdb.io).

# 1. Crawling OSDB list
Crawling the Open Source DBMS list from [dbdb.io/browse?type=open-source](https://www.dbdb.io/browse?type=open-source) with the beautifulsoup package.

Save as [OSDB_crawling_202301_raw.csv](./data/dbdbio_OSDB_list/OSDB_crawling_202301_raw.csv)

About dbdb.io http link:
- Use "https://dbdb.io/browse?type=open-source" to get all the open source databases in dbdb.io
- Use "https://dbdb.io/browse?q=*" to get all the databases in dbdb.io

This repository focus on the repositories which have image source codes and communities on github. 
The commercial databases is not within the scope of crawling. 
However, you can use "[dbdb.io/browse?q=*](https://www.dbdb.io/browse?q=*)" to crawl the entire data set if it is necessary.
And the column "open_source_license" in "data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv" may need to be re-labeled manually.

# 2. recalc OSDB list
The table of OSDB list csv have columns ["card_title", "card_title_href", "card_img_href", "card_text"]. 
"card_title" is almost the DBMS name we want, however, some DBMS has the same card_title values: 
e.g.
- DBMS "Consus" from "https://dbdb.io/db/consus" and DBMS "Consus" from "https://dbdb.io/db/consus-java" have the same "card_title" value. 

So the Key column should be re-calculated to distinct data format.
We add a new column "Name" to store the recalculated DBMS name by the "card_title_href" column, which has unique values.

Finally, we override the old OSDB list csv in place.

# 3. Crawling OSDB information
Crawling the Open Source DBMS information from the OSDB card_title_href of OSDB list csv, which has crawled by function "crawling_OSDB_infos_soup".

# 4. recalc OSDB information
Some fields should be re-calculated as other data formats. 
e.g. 
- Check the "Name" column, if not found, recalculated DBMS name by the "card_title_href" column; 
- Mapping the "Data Model" column to [DB-Engines](https://db-engines.com/en/ranking) with [DB-Engines DBMS categories labels mapping table](https://github.com/birdflyi/db_engines_ranking_table_crawling/blob/main/data/existing_tagging_info/category_labels.csv), 
and get a new [dbdbio DBMS category labels mapping table](./data/existing_tagging_info/category_labels_mapping_table.csv); 
- Check whether Source_Code_record_from_github by "Source Code" column; 
- Convert type from float to str(int) for "Start Year" and "End Year" columns.

# 5. join OSDB list and OSDB information
Join OSDB list and OSDB information on the column "Name", and set the key name alias to 'DBMS_uriform' after joined.

Set use_cols_OSDB_list = None to use all fields of OSDB list, and set
use_cols_OSDB_info = ["Name", "card_title", "Description", "Data Model", "Query Interface", "System Architecture", "Website",
                      "Source Code", "Tech Docs", "Developer", "Country of Origin", "Start Year", "End Year",
                      "Project Type", "Written in", "Supported languages", "Embeds / Uses", "Licenses",
                      "Operating Systems"] as default.

---
### 1. The reason for crawling OSDB information from dbdb.io
See [X-lab2017/open-digger#1136](https://github.com/X-lab2017/open-digger/issues/1136)

### 2. Brother project
- [birdflyi/db_engines_ranking_table_crawling](https://github.com/birdflyi/db_engines_ranking_table_crawling)
