ydb --endpoint grpcs://ydb.serverless.yandexcloud.net:2135 \
    --database /ru-central1/b1gfhcse172450fifcje/etn3p0qk8go18m3m7lfs \
    --sa-key-file ~/authorized_key.json \
    import file csv \
    --path weather_data \
    --delimiter "," \
    --skip-rows 1 \
    --null-value "" \
    "$(pwd)/weather_data.csv"