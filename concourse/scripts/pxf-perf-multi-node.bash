#!/usr/bin/env bash

set -eo pipefail

export PGHOST=mdw
export PGUSER=gpadmin
export PGDATABASE=tpch
GPHOME="/usr/local/greenplum-db-devel"
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HADOOP_HOSTNAME="ccp-$(cat terraform_dataproc/name)-m"
scale=$(($SCALE + 0))
PXF_CONF_DIR="/home/gpadmin/pxf"
PXF_SERVER_DIR="${PXF_CONF_DIR}/servers"
UUID=$(cat /proc/sys/kernel/random/uuid)
declare -a CONCURRENT_RESULT

if [ ${scale} -gt 10 ]; then
  VALIDATION_QUERY="SUM(l_partkey) AS PARTKEYSUM"
else
  VALIDATION_QUERY="COUNT(*) AS Total, COUNT(DISTINCT l_orderkey) AS ORDERKEYS, SUM(l_partkey) AS PARTKEYSUM, COUNT(DISTINCT l_suppkey) AS SUPPKEYS, SUM(l_linenumber) AS LINENUMBERSUM"
fi

LINEITEM_COUNT="unset"
LINEITEM_VAL_RESULTS="unset"
source "${CWDIR}/pxf_common.bash"

function readable_external_table_text_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE READABLE EXTERNAL TABLE lineitem_${name}_read (LIKE lineitem) LOCATION('${path}') FORMAT 'CSV' (DELIMITER '|')"
}

function writable_external_table_text_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_${name}_write (LIKE lineitem) LOCATION('${path}') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function readable_external_table_parquet_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE READABLE EXTERNAL TABLE lineitem_${name}_read_parquet (LIKE lineitem) LOCATION('${path}') FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8'"
}

function writable_external_table_parquet_query() {
    local name=${1}
    local path=${2}
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_${name}_write_parquet (LIKE lineitem) LOCATION('${path}') FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export') DISTRIBUTED BY (l_partkey)"
}

function read_and_validate_table_count {
    local table_name="$1"
    local expected_count="$2"
    local num_rows=$(time psql -t -c "SELECT COUNT(*) FROM $table_name" | tr -d ' ')

    if [[ ${num_rows} != ${expected_count} ]]; then
        echo "Expected number of rows in table ${table_name} to be ${expected_count} but was ${num_rows}"
        exit 1
    fi
}

function write_sub_header() {
    cat << EOF

${1}
------------------------------
EOF
}

function write_header {
    cat << EOF


############################################
# ${1}
############################################
EOF
}

function create_database_and_schema {
    # Create DB
    psql -d postgres <<EOF
DROP DATABASE IF EXISTS tpch;
CREATE DATABASE tpch;
\c tpch;
CREATE TABLE lineitem (
    l_orderkey    BIGINT NOT NULL,
    l_partkey     BIGINT NOT NULL,
    l_suppkey     BIGINT NOT NULL,
    l_linenumber  BIGINT NOT NULL,
    l_quantity    DECIMAL(15,2) NOT NULL,
    l_extendedprice  DECIMAL(15,2) NOT NULL,
    l_discount    DECIMAL(15,2) NOT NULL,
    l_tax         DECIMAL(15,2) NOT NULL,
    l_returnflag  CHAR(1) NOT NULL,
    l_linestatus  CHAR(1) NOT NULL,
    l_shipdate    DATE NOT NULL,
    l_commitdate  DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode     CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL
) DISTRIBUTED BY (l_partkey);
EOF

    psql -c "CREATE EXTERNAL TABLE lineitem_external (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
}

function prepare_hadoop {
    local name=${1}
    local run_id=${2}
    # create text tables
    readable_external_table_text_query "${name}" "pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple"
    writable_external_table_text_query "${name}" "pxf://tmp/lineitem_write/${run_id}/?PROFILE=HdfsTextSimple"
    # create parquet tables
    readable_external_table_parquet_query "${name}" "pxf://tmp/lineitem_write_parquet/${run_id}/?PROFILE=hdfs:parquet"
    writable_external_table_parquet_query "${name}" "pxf://tmp/lineitem_write_parquet/${run_id}/?PROFILE=hdfs:parquet"
}

function prepare_adl() {
    local name=${1}
    readable_external_table_text_query "${name}" "pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/lineitem/${SCALE}/?PROFILE=adl:text&server=adlbenchmark"
    writable_external_table_text_query "${name}" "pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/output/${SCALE}/${UUID}/?PROFILE=adl:text&server=adlbenchmark"

    ADL_SERVER_DIR="${PXF_SERVER_DIR}/adlbenchmark"
    # Create the ADL Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $ADL_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/adl-site.xml $ADL_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_REFRESH_URL|${ADL_REFRESH_URL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_CLIENT_ID|${ADL_CLIENT_ID}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_CREDENTIAL|${ADL_CREDENTIAL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
}

function prepare_gcs() {
    local name=${1}
    readable_external_table_text_query "${name}" "pxf://data-gpdb-ud-tpch/${SCALE}/lineitem_data/?PROFILE=gs:text&SERVER=gsbenchmark"
    writable_external_table_text_query "${name}" "pxf://data-gpdb-ud-pxf-benchmark/output/${SCALE}/${UUID}/?PROFILE=gs:text&SERVER=gsbenchmark"

    cat << EOF > /tmp/gsc-ci-service-account.key.json
${GOOGLE_CREDENTIALS}
EOF

    GS_SERVER_DIR="${PXF_SERVER_DIR}/gsbenchmark"
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $GS_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/gs-site.xml $GS_SERVER_DIR"
    gpscp -u gpadmin -h mdw /tmp/gsc-ci-service-account.key.json =:${GS_SERVER_DIR}/
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_GOOGLE_STORAGE_KEYFILE|${GS_SERVER_DIR}/gsc-ci-service-account.key.json|\" ${GS_SERVER_DIR}/gs-site.xml"
}

function prepare_gphdfs {
    local name=${1}
    readable_external_table_text_query "${name}" "gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_read/"
    writable_external_table_text_query "${name}" "gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_gphdfs_write/"
}

function prepare_s3_extension {
    local name=${1}
    readable_external_table_text_query "${name}" "s3://s3.us-west-2.amazonaws.com/gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/ config=/home/gpadmin/s3/s3.conf"
    writable_external_table_text_query "${name}" "s3://s3.us-east-2.amazonaws.com/gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${UUID}/ config=/home/gpadmin/s3/s3.conf"

    psql -c "CREATE OR REPLACE FUNCTION write_to_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_export' LANGUAGE C STABLE"
    psql -c "CREATE OR REPLACE FUNCTION read_from_s3() RETURNS integer AS '\$libdir/gps3ext.so', 's3_import' LANGUAGE C STABLE"
    psql -c "CREATE PROTOCOL s3 (writefunc = write_to_s3, readfunc = read_from_s3)"

    cat > /tmp/s3.conf <<EOF
[default]
accessid = "${AWS_ACCESS_KEY_ID}"
secret = "${AWS_SECRET_ACCESS_KEY}"
threadnum = 4
chunksize = 67108864
low_speed_limit = 10240
low_speed_time = 60
encryption = true
version = 1
proxy = ""
autocompress = false
verifycert = true
server_side_encryption = ""
# gpcheckcloud config
gpcheckcloud_newline = "\n"
EOF
    cat cluster_env_files/etc_hostfile | grep sdw | cut -d ' ' -f 2 > /tmp/segment_hosts
    gpssh -u gpadmin -f /tmp/segment_hosts -v -s -e 'mkdir ~/s3/'
    gpscp -u gpadmin -f /tmp/segment_hosts /tmp/s3.conf =:~/s3/s3.conf
}

function prepare_s3() {
    local name=${1}
    local run_id=${2}
    # create text tables
    readable_external_table_text_query "${name}" "pxf://gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/?PROFILE=s3:text&SERVER=s3benchmark"
    writable_external_table_text_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:text&SERVER=s3benchmark"
    # create parquet tables
    readable_external_table_parquet_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:parquet&SERVER=s3benchmark"
    writable_external_table_parquet_query "${name}" "pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${UUID}-${run_id}/?PROFILE=s3:parquet&SERVER=s3benchmark"
}

function configure_s3_server() {
    # We need to create s3-site.xml and provide AWS credentials
    S3_SERVER_DIR="${PXF_SERVER_DIR}/s3benchmark"
    # Make a backup of core-site and update it with the S3 core-site
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $S3_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/s3-site.xml $S3_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AWS_ACCESS_KEY_ID|${AWS_ACCESS_KEY_ID}|\" $S3_SERVER_DIR/s3-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AWS_SECRET_ACCESS_KEY|${AWS_SECRET_ACCESS_KEY}|\" $S3_SERVER_DIR/s3-site.xml"
    sync_configuration
}

function prepare_wasb() {
    local name=${1}
    readable_external_table_text_query ${name} "pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/lineitem/${SCALE}/?PROFILE=wasbs:text&server=wasbbenchmark"
    writable_external_table_text_query ${name} "pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/output/${SCALE}/${UUID}/?PROFILE=wasbs:text&server=wasbbenchmark"

    WASB_SERVER_DIR="${PXF_SERVER_DIR}/wasbbenchmark"
    # Create the WASB Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $WASB_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/wasbs-site.xml $WASB_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_NAME|${WASB_ACCOUNT_NAME}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_KEY|${WASB_ACCOUNT_KEY}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
}

function setup_sshd {
    service sshd start
    passwd -u root

    if [[ -d cluster_env_files ]]; then
        /bin/cp -Rf cluster_env_files/.ssh/* /root/.ssh
        /bin/cp -f cluster_env_files/private_key.pem /root/.ssh/id_rsa
        /bin/cp -f cluster_env_files/public_key.pem /root/.ssh/id_rsa.pub
        /bin/cp -f cluster_env_files/public_key.openssh /root/.ssh/authorized_keys
    fi
}

function sync_configuration() {
    gpssh -u gpadmin -h mdw -v -s -e "source ${GPHOME}/greenplum_path.sh && ${GPHOME}/pxf/bin/pxf cluster sync"
}

function write_data {
    local dest
    local source
    dest=${2}
    source=${1}
    psql -c "INSERT INTO ${dest} SELECT * FROM ${source}"
}

function validate_write_to_gpdb {
    local external
    local internal
    local external_values
    local gpdb_values
    external=${1}
    internal=${2}

    gpdb_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${internal}")
    write_sub_header "Results from GPDB query"
    echo ${gpdb_values}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${external}")
    write_sub_header "Results from external query"
    echo ${external_values}

    if [[ "${external_values}" != "${gpdb_values}" ]]; then
        echo ERROR! Unable to validate data written from external to GPDB
        exit 1
    fi
}

function gphdfs_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read_after_write (LIKE lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV'"
    local external_values

    write_sub_header "Results from GPDB query"
    echo ${LINEITEM_VAL_RESULTS}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM gphdfs_lineitem_read_after_write")
    write_sub_header "Results from external query"
    echo ${external_values}

    if [[ "${external_values}" != "${LINEITEM_VAL_RESULTS}" ]]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function pxf_validate_write_to_external {
    local run_id=${1}
    local external_values
    local validation_table_name="pxf_lineitem_read_after_write_${run_id}"

    psql -c "CREATE EXTERNAL TABLE ${validation_table_name} (LIKE lineitem)
        LOCATION ('pxf://tmp/lineitem_write/${run_id}/?PROFILE=hdfs:text') FORMAT 'CSV'"

    write_sub_header "Results from GPDB query"
    echo ${LINEITEM_VAL_RESULTS}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${validation_table_name}")
    write_sub_header "Results from external query"
    echo ${external_values}

    if [[ "${external_values}" != "${LINEITEM_VAL_RESULTS}" ]]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function run_concurrent_benchmark() {
    local benchmark_fn=${1}
    local prepare_test_fn=${2}
    local benchmark_name=${3}
    local benchmark_description=${4}
    local concurrency=${5}
    local pids=()
    local status_codes=()
    local has_failures=0

    for i in `seq 1 ${concurrency}`; do
        echo "Starting PXF Benchmark ${benchmark_fn} ${i} with UUID ${UUID}-${i}"
        ${benchmark_fn} ${prepare_test_fn} "${benchmark_name}" "${benchmark_description}" "${i}" >/tmp/${benchmark_fn}-${benchmark_name}-${i}.bench 2>&1 &
        pids+=("$!")
    done

    set +e
    # collect status codes from background tasks
    for p in "${pids[@]}"; do
        wait ${p}
        status_code=$?
        status_codes+=("${status_code}")
    done
    set -e

    # print out all the results from the files
    cat $(ls /tmp/${benchmark_fn}-${benchmark_name}-*.bench)

    # check for errors in background tasks
    local has_failures=0
    for i in `seq 1 ${concurrency}`; do
        if [[ ${status_codes[i-1]} != 0 ]]; then
            echo "Run ${i} failed"
            has_failures=1
        fi
    done

    if [[ ${has_failures} != 0 ]]; then
        exit 1
    fi
}

function run_simple_benchmark() {
    local prepare_test_fn=${1}
    local benchmark_name=${2}
    local benchmark_description=${3}

    ${prepare_test_fn} ${benchmark_name}
    sync_configuration

    write_header "${benchmark_description} PXF READ TEXT BENCHMARK"
    read_and_validate_table_count "lineitem_${benchmark_name}_read" "${LINEITEM_COUNT}"

    write_header "${benchmark_description} PXF WRITE TEXT BENCHMARK"
    time psql -c "INSERT INTO lineitem_${benchmark_name}_write SELECT * FROM lineitem"
}

function run_text_and_parquet_benchmark() {
    local prepare_test_fn=${1}
    local benchmark_name=${2}
    local benchmark_description=${3}
    local run_id=${4}
    local name="${benchmark_name}_${run_id}"

    echo ""
    echo "---------------------------------------------------------------------------"
    echo "--- ${benchmark_description} PXF Benchmark ${i} with UUID ${UUID}-${i} ---"
    echo "---------------------------------------------------------------------------"

    ${prepare_test_fn} "${name}" "${run_id}"
    # sync_configuration do we need to sync configuration here?

    write_header "${benchmark_description} PXF READ TEXT BENCHMARK (Run ${run_id})"
    read_and_validate_table_count "lineitem_${name}_read" "${LINEITEM_COUNT}"

    write_header "${benchmark_description} PXF WRITE TEXT BENCHMARK (Run ${run_id})"
    time psql -c "INSERT INTO lineitem_${name}_write SELECT * FROM lineitem"

#    echo -ne "\n>>> Validating data <<<\n"
#    pxf_validate_write_to_external ${run_id}

    write_header "${benchmark_description} PXF WRITE PARQUET BENCHMARK (Run ${run_id})"
    time psql -c "INSERT INTO lineitem_${name}_write_parquet SELECT * FROM lineitem"

    write_header "${benchmark_description} PXF READ PARQUET BENCHMARK (Run ${run_id})"
    read_and_validate_table_count "lineitem_${name}_read_parquet" "${LINEITEM_COUNT}"
}

function main {
    setup_sshd
    remote_access_to_gpdb
    install_gpdb_binary

    install_pxf_server

    echo "Running ${SCALE}G test with UUID ${UUID}"
    echo "PXF Process Details:"
    echo "$(ps aux | grep tomcat)"

    source ${GPHOME}/greenplum_path.sh
    create_database_and_schema

    echo "Loading data from external into GPDB..."
    write_data "lineitem_external" "lineitem"
    echo "Validating loaded data..."
    validate_write_to_gpdb "lineitem_external" "lineitem"
    echo -e "Data loading and validation complete\n"
    LINEITEM_COUNT=$(psql -t -c "SELECT COUNT(*) FROM lineitem" | tr -d ' ')
#    LINEITEM_VAL_RESULTS=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")

    if [[ ${BENCHMARK_ADL} == true ]]; then
        run_simple_benchmark prepare_adl "ADL" "AZURE DATA LAKE"
    fi

    if [[ ${BENCHMARK_WASB} == true ]]; then
        run_simple_benchmark prepare_wasb "wasb" "AZURE BLOB STORAGE"
    fi

    if [[ ${BENCHMARK_GCS} == true ]]; then
        run_simple_benchmark prepare_gcs "gcs" "GOOGLE CLOUD STORAGE"
    fi

    if [[ ${BENCHMARK_S3_EXTENSION} == true ]]; then
        run_simple_benchmark prepare_s3_extension "s3_c" "S3 C EXTENSION"
    fi

    if [[ ${BENCHMARK_S3} == true ]]; then
        configure_s3_server

        concurrency=${BENCHMARK_S3_CONCURRENCY:-1}
        if [[ ${concurrency} == 1 ]]; then
            run_text_and_parquet_benchmark prepare_s3 "s3" "S3" "0"
        else
            run_concurrent_benchmark run_text_and_parquet_benchmark prepare_s3 "s3" "S3" "${concurrency}"
        fi
    fi

    if [[ ${BENCHMARK_GPHDFS} == true ]]; then
        run_simple_benchmark prepare_gphdfs "gphdfs" "GPHDFS"
        echo -ne "\n>>> Validating data <<<\n"
        gphdfs_validate_write_to_external
    fi

    if [[ ${BENCHMARK_HADOOP} == true ]]; then
        concurrency=${BENCHMARK_HADOOP_CONCURRENCY:-1}
        if [[ ${concurrency} == 1 ]]; then
            run_text_and_parquet_benchmark prepare_hadoop "hadoop" "HADOOP" "0"
        else
            run_concurrent_benchmark run_text_and_parquet_benchmark prepare_hadoop "hadoop" "HADOOP" ${concurrency}
        fi
    fi
}

main
