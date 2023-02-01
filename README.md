# mongodb_update_data
Update and insert USPTO PAIR data to mongoDB
1. main.sh: Whole data ETL pipeline
2. download_pair_file.py: Download latest data from USPTO Data Center API to data directory and unzip it.
3. pair_upsert_mongodb.py: Update and insert PAIR data to mongoDB collection=pair_v2
4. pair_error_fixed.py: Fix datatype error(p.s. in latest version of pair_upsert_mongodb.py this problem doesn't occur anymore)
5. send_msg.py: send log file and successful or failure message by email
