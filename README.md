# dbdbio_OSDB_info_crawling
Crawling Open Source DataBases from [dbdb.io](https://www.dbdb.io).

# 1. Crawling OSDB list
Crawling the Open Source DBMS list from [dbdb.io/browse?project-type=open-source](https://dbdb.io/browse?project-type=open-source) with the beautifulsoup package.

Save as [OSDB_crawling_202301_raw.csv](./data/dbdbio_OSDB_list/OSDB_crawling_202301_raw.csv)

About dbdb.io http link:
- Use "https://dbdb.io/browse?project-type=open-source" to get all the open source databases in dbdb.io
- Use "https://dbdb.io/browse?q=*" to get all the databases in dbdb.io

This repository focus on the repositories which have image source codes and communities on github. 
The commercial databases is not within the scope of crawling. 
However, you can use "[dbdb.io/browse?q=*](https://www.dbdb.io/browse?q=*)" to crawl the entire data set if it is necessary.
And the column "open_source_license" in "data/dbdbio_OSDB_list/OSDB_info_crawling_{month_yyyyMM}_raw.csv" may need to be re-labeled manually.

## Slow responses and interrupted downloads

The main crawler uses `urllib` and BeautifulSoup. `IncompleteRead` means the HTTP
response ended before the transfer was complete; increasing a Selenium element
wait does not fix this download error.

`HTTP_OPTIONS` in `main.py` applies to the list, missing-record checks, and details:

- `timeout=30`: each blocking socket operation times out after 30 seconds.
- `max_attempts=3`: retry incomplete transfers, network errors, and temporary HTTP
  errors (408, 429, 500, 502, 503, 504) up to three attempts in total.
- `max_read_seconds=120`: check the response budget between reads. A pending read
  can exceed this budget by up to `timeout` seconds. This is not a strict total
  wall-clock deadline, and the whole crawl can take longer across many pages.
- `retry_backoff=2`: wait 2 then 4 seconds between attempts.

The console prints each URL and attempt before connecting, download progress
during slow transfers, and completion or retry reasons. Partial responses are
discarded and retried from the beginning. A list download or parsing failure
raises an error before replacing the existing list CSV. Detail failures are
recorded in `Crawl Error`, and the crawler continues with the next database.
Ctrl+C interrupts the crawl normally. Existing batch checkpoint behavior is
preserved; only completed batches have been saved if the crawl is interrupted.

Run the local network regression tests without changing monthly data:

```powershell
.\venv\Scripts\python.exe -m unittest discover -s tests -v
```

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
Join OSDB list and OSDB information on the column "Name", and set the key name alias to 'DBMS_urnform' after joined.

Set use_cols_OSDB_list = None to use all fields of OSDB list, and set
use_cols_OSDB_info = ["Name", "card_title", "Description", "Data Model", "Query Interface", "System Architecture", "Website",
                      "Source Code", "Tech Docs", "Developer", "Country of Origin", "Start Year", "End Year",
                      "Project Type", "Written in", "Supported languages", "Embeds / Uses", "Licenses",
                      "Operating Systems"] as default.

# 6. Merging the current month manulabeled data with last month manulabeled data
Find difference between current month manulabeled data(e.g. OSDB_info_202303_joined_manulabeled.csv) and last month manulabeled data(e.g. OSDB_info_202302_joined_manulabeled.csv). 
Solve conflicts in the merged table(default overwrite the current month manulabeled data) manually.

`AUTO_KEEP_CURRENT_MONTH_URLS = True` in `main.py` automatically keeps the current
month's value for conflicts in `CURRENT_MONTH_URL_COLUMNS`: Website URL/Website,
Documentation/Tech Docs, Wikipedia URL/Wikipedia, Blog URL, card_title_href, and
card_img_href. The exact current value is preserved, including its trailing slash.
When the current value is missing, the previous month's value is used as before.
Other fields still show both months separated by
`#start_checkpoint>|<last_month_manulabeled#` for manual review.

Set the switch to `False` to restore manual conflict review for all columns, or
edit the column list to choose which fields automatically keep the current month.
Source Code and repository mapping fields retain manual conflict review by
default. This policy applies when generating a merged table; changing the switch
does not edit existing CSV files.

`UPDATE_START_CHECKPOINT_USE_MANULABELED` only selects the current-month input:
`0` reads the joined crawl output, while `1` reads the current annotated file.
It does not control conflict resolution.

---
### 1. The reason for crawling OSDB information from dbdb.io
See [X-lab2017/open-digger#1136](https://github.com/X-lab2017/open-digger/issues/1136)

### 2. Brother project
- [birdflyi/db_engines_ranking_table_crawling](https://github.com/birdflyi/db_engines_ranking_table_crawling)
