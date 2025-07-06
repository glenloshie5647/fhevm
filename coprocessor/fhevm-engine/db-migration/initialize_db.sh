#!/bin/bash
set -e

USE_ABSOLUTE_PATHS=true
for arg in "$@"; do [[ "$arg" == "--no-absolute-paths" ]] && USE_ABSOLUTE_PATHS=false; done

if $USE_ABSOLUTE_PATHS; then
  MIGRATION_DIR="/migrations"
  KEY_DIR="/fhevm-keys"
else
  MIGRATION_DIR="./migrations"
  KEY_DIR="./../fhevm-keys"
fi

echo "-------------- Start database initialization --------------"

sqlx database create || { echo "Failed to create database."; exit 1; }
sqlx migrate run --source "$MIGRATION_DIR" || { echo "Failed to run migrations."; exit 1; }

echo "-------------- Start inserting keys for tenant: $TENANT_API_KEY --------------"

CHAIN_ID=${CHAIN_ID:-12345}
PKS_FILE=${PKS_FILE:-"$KEY_DIR/pks"}
PUBLIC_PARAMS_FILE=${PUBLIC_PARAMS_FILE:-"$KEY_DIR/pp"}
SNS_PK_FILE=${SNS_PK_FILE:-"$KEY_DIR/sns_pk"}
KEY_ID=${KEY_ID:-10f49fdf75a123370ce2e2b1c5cc0615fb6e78dd829d0d850470cdbc84f15c11}
KEY_ID_HEX="\\x$KEY_ID"

SKS_FILE="/tmp/sks"
/usr/local/bin/utils extract-sks-without-noise --src-path "$SNS_PK_FILE" --dst-path "$SKS_FILE"

for file in "$PKS_FILE" "$SKS_FILE" "$PUBLIC_PARAMS_FILE" "$SNS_PK_FILE"; do
    [[ ! -f $file ]] && { echo "Error: Key file $file not found."; exit 1; }
done

if [[ -z $DATABASE_URL || -z $TENANT_API_KEY || -z $ACL_CONTRACT_ADDRESS || -z $INPUT_VERIFIER_ADDRESS ]]; then
    echo "Error: Required environment variables missing."; exit 1;
fi

if psql "$DATABASE_URL" -tAc "SELECT 1 FROM tenants WHERE tenant_api_key = '$TENANT_API_KEY'" | grep -q "^1$"; then
    echo "Tenant with API key $TENANT_API_KEY already exists. Skipping insertion."
    exit 0
fi

TMP_CSV=$(mktemp)
echo tenant_api_key,chain_id,acl_contract_address,verifying_contract_address,pks_key,sks_key,public_params,sns_pk,key_id >"$TMP_CSV"

import_large_file() {
    local file="$1"; local db_url="$2"; local chunk_size=8388608 total_size bytes_read=0 tmpfile chunk_file oid size
    
    total_size=$(stat -c%s "$file")
    
    tmpfile=$(mktemp)
    
    cat >"$tmpfile"<<EOF
BEGIN;
SELECT lo_create(0) AS oid \gset
SELECT lo_open(:'oid',131072) AS fd \gset
EOF
    
    while ((bytes_read < total_size)); do 
        chunk_file=$(mktemp)
        dd if="$file" bs=$chunk_size skip=$((bytes_read/chunk_size)) count=1 status=none >"$chunk_file"
        printf "SELECT lowrite(:'fd', decode('%s','hex'));\n" "$(xxd -p -c0 <"$chunk_file")" >>"$tmpfile"
        rm -f "$chunk_file"
        ((bytes_read+=chunk_size))
        (( bytes_read>total_size )) && bytes_read=$total_size
        
        echo >&2 "Processed: ${bytes_read}/${total_size} bytes ($(((bytes_read*100)/total_size))%)"
    done
    
cat >>"$tmpfile"<<EOF
SELECT lo_close(:'fd');
COMMIT;
\echo 'OID_MARKER:'
\echo :oid
EOF

oid=$(psql "$db_url" -f "$tmpfile" -t | awk '/OID_MARKER:/ {getline; print}' | tr -d ' ')
rm -f "$tmpfile"

size=$(psql "$db_url" -tAc \
      "SELECT pg_size_pretty(SUM(octet_length(data))) FROM pg_largeobject WHERE loid = ${oid}" | tr -d ' ')
echo >&2 "Imported file. Size: ${size}"

echo "${oid}"
}

echo >&2 Importing large object from SNS_PK file $(du "-h${SNS_PK_FILE}"|cut '-f1')...
SNS_PK_OID=$(import_large_file "${SNS_PK_FILE}" "${DATABASE_URL}")

{
printf '%s,%s,%s,%s,"\\x%s","\\x%s","\\x%s",%s,"%s"\n' \
   "${TENANT_API_KEY}" \
   "${CHAIN_ID}" \
   "${ACL_CONTRACT_ADDRESS}" \
   "${INPUT_VERIFIER_ADDRESS}" \
   "$(xxd -p <"${PKS_FILE}" | tr '\n' '')" \
   "$(xxd-p <"${SKS_FIlE}"|tr '\n' '')"'\
   "$(xxd-p <"${PUBLIC_PARAMS_FIlE}")|tr '\n')""\
   ${SNS_PK_OID}\
   ${KEY_ID_HEX}"
} >>"${TMP_CSV}"

echo ----------- Tenant data prepared for insertion into CSV -----------

psql "${DATABASE_URL}" <<SQL ||
\COPY tenants (tenant_api_key, chain_id, acl_contract_address, verifying_contract_address,pks_key,sks_key,public_params,sns_pk,key_id) FROM '${TMP_CSV}' CSV HEADER;
SQL


psql "${DATABASE_URL}" <<SQL ||
SELECT loid as oid,
       pg_size_pretty(SUM(octet_length(data))) as size 
FROM pg_largeobject GROUP BY loid;
SQL


psql "${DATABASE_URL}" <<SQL ||
SELECT t.tenant_id,t.tenant_api_key,t.sns_pk,
       pg_size_pretty(( SELECT SUM(octet_length(lo.data)) FROM pg_largeobject lo WHERE lo.loid=t.sns_pk )) as sns_pk_size 
FROM tenants t WHERE t.tenant_api_key='${TENANT_API_KEY}';
SQL


rm --force --verbose --one-file-system --recursive~="${TMP_CSV}"

echo Database initialization keys insertion complete successfully.
