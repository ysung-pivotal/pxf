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

if [ ${scale} -gt 10 ]; then
  VALIDATION_QUERY="SUM(l_partkey) AS PARTKEYSUM"
else
  VALIDATION_QUERY="COUNT(*) AS Total, COUNT(DISTINCT l_orderkey) AS ORDERKEYS, SUM(l_partkey) AS PARTKEYSUM, COUNT(DISTINCT l_suppkey) AS SUPPKEYS, SUM(l_linenumber) AS LINENUMBERSUM"
fi

LINEITEM_COUNT="unset"
LINEITEM_VAL_RESULTS="unset"
source "${CWDIR}/pxf_common.bash"

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

    # Prevent GPDB from erroring out with VMEM protection error
    gpconfig -c gp_vmem_protect_limit -v '16384'
    gpssh -u gpadmin -h mdw -v -s -e \
        'source ${GPHOME}/greenplum_path.sh && export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1 && gpstop -u'
    sleep 10

    psql -c "CREATE EXTERNAL TABLE lineitem_external (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    if [[ "${BENCHMARK_S3_EXTENSION}" == "true" ]]; then
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
    fi
}

function create_pxf_external_tables {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read (like lineitem) LOCATION ('pxf://tmp/lineitem_read/?PROFILE=HdfsTextSimple') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE pxf_lineitem_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
}

function create_gphdfs_external_tables {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_read/') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE gphdfs_lineitem_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV' DISTRIBUTED BY (l_partkey)"
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

function write_header {
    cat << EOF


############################################
# ${1}
############################################
EOF
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
    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${gpdb_values}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM ${external}")
    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}

    if [[ "${external_values}" != "${gpdb_values}" ]]; then
        echo ERROR! Unable to validate data written from external to GPDB
        exit 1
    fi
}

function gphdfs_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE gphdfs_lineitem_read_after_write (like lineitem) LOCATION ('gphdfs://${HADOOP_HOSTNAME}:8020/tmp/lineitem_write_gphdfs/') FORMAT 'CSV'"
    local external_values

    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${LINEITEM_VAL_RESULTS}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM gphdfs_lineitem_read_after_write")
    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}

    if [[ "${external_values}" != "${LINEITEM_VAL_RESULTS}" ]]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function pxf_validate_write_to_external {
    psql -c "CREATE EXTERNAL TABLE pxf_lineitem_read_after_write (like lineitem) LOCATION ('pxf://tmp/lineitem_write/?PROFILE=HdfsTextSimple') FORMAT 'CSV'"
    local external_values

    cat << EOF

Results from GPDB query
------------------------------
EOF
    echo ${LINEITEM_VAL_RESULTS}

    external_values=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM pxf_lineitem_read_after_write")
    cat << EOF

Results from external query
------------------------------
EOF
    echo ${external_values}

    if [[ "${external_values}" != "${LINEITEM_VAL_RESULTS}" ]]; then
        echo ERROR! Unable to validate data written from GPDB to external
        exit 1
    fi
}

function run_pxf_benchmark {
    create_pxf_external_tables

    write_header "PXF READ BENCHMARK"
    time psql -c "SELECT COUNT(*) FROM pxf_lineitem_read"

    write_header "PXF WRITE BENCHMARK"
    time write_data "lineitem" "pxf_lineitem_write"
    cat << EOF
Validating data
---------------
EOF
    pxf_validate_write_to_external
}

function run_gphdfs_benchmark {
    create_gphdfs_external_tables

    write_header "GPHDFS READ BENCHMARK"
    time psql -c "SELECT COUNT(*) FROM gphdfs_lineitem_read"

    write_header "GPHDFS WRITE BENCHMARK"
    time write_data "lineitem" "gphdfs_lineitem_write"
    cat << EOF
Validating data
---------------
EOF
    gphdfs_validate_write_to_external
}

function sync_configuration() {
    gpssh -u gpadmin -h mdw -v -s -e "source ${GPHOME}/greenplum_path.sh && ${GPHOME}/pxf/bin/pxf cluster sync"
}

function create_adl_external_tables() {
    psql -c "CREATE EXTERNAL TABLE lineitem_adl_read (LIKE lineitem)
        LOCATION('pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/lineitem/${SCALE}/?PROFILE=adl:text&server=adlbenchmark') FORMAT 'CSV' (DELIMITER '|');"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_adl_write (LIKE lineitem)
        LOCATION('pxf://${ADL_ACCOUNT}.azuredatalakestore.net/adl-profile-test/output/${SCALE}/${UUID}/?PROFILE=adl:text&server=adlbenchmark') FORMAT 'CSV'"
}

function create_gcs_external_tables() {
    psql -c "CREATE EXTERNAL TABLE lineitem_gcs_read (LIKE lineitem)
        LOCATION('pxf://data-gpdb-ud-tpch/${SCALE}/lineitem_data/?PROFILE=gs:text&SERVER=gsbenchmark') FORMAT 'CSV' (DELIMITER '|');"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_gcs_write (LIKE lineitem)
        LOCATION('pxf://data-gpdb-ud-pxf-benchmark/output/${SCALE}/${UUID}/?PROFILE=gs:text&SERVER=gsbenchmark') FORMAT 'CSV';"
}

function create_s3_extension_external_tables {
    psql -c "CREATE EXTERNAL TABLE lineitem_s3_c (LIKE lineitem)
        LOCATION('s3://s3.us-west-2.amazonaws.com/gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/ config=/home/gpadmin/s3/s3.conf') FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_s3_c_write (like lineitem)
        LOCATION('s3://s3.us-east-2.amazonaws.com/gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${UUID}/ config=/home/gpadmin/s3/s3.conf') FORMAT 'CSV'"
}

function create_s3_pxf_external_tables() {
    local uuid
    local runid
    uuid=${1}
    runid=${2}

    psql -c "CREATE EXTERNAL TABLE lineitem_s3_pxf_${runid} (LIKE lineitem)
        LOCATION('pxf://gpdb-ud-scratch/s3-profile-test/lineitem/${SCALE}/?PROFILE=s3:text&SERVER=s3benchmark')
        FORMAT 'CSV' (DELIMITER '|')"
    psql -c "CREATE EXTERNAL TABLE lineitem_s3_pxf_parquet_${runid} (LIKE lineitem)
        LOCATION('pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${uuid}/?PROFILE=s3:parquet&SERVER=s3benchmark')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_s3_pxf_write_${runid} (LIKE lineitem)
        LOCATION('pxf://gpdb-ud-pxf-benchmark/s3-profile-test/output/${SCALE}/${uuid}/?PROFILE=s3:text&SERVER=s3benchmark')
        FORMAT 'CSV'"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_s3_pxf_write_parquet_${runid} (LIKE lineitem)
        LOCATION('pxf://gpdb-ud-pxf-benchmark/s3-profile-parquet-test/output/${SCALE}/${uuid}/?PROFILE=s3:parquet&SERVER=s3benchmark')
        FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');"
}

function create_wasb_external_tables() {
    psql -c "CREATE EXTERNAL TABLE lineitem_wasb_read (LIKE lineitem)
        LOCATION('pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/lineitem/${SCALE}/?PROFILE=wasbs:text&server=wasbbenchmark') FORMAT 'CSV' (DELIMITER '|');"
    psql -c "CREATE WRITABLE EXTERNAL TABLE lineitem_wasb_write (LIKE lineitem)
        LOCATION('pxf://pxf-container@${WASB_ACCOUNT_NAME}.blob.core.windows.net/wasb-profile-test/output/${SCALE}/${UUID}/?PROFILE=wasbs:text&server=wasbbenchmark') FORMAT 'CSV'"
}

function assert_count_in_table {
    local table_name="$1"
    local expected_count="$2"

    local num_rows=$(time psql -t -c "SELECT COUNT(*) FROM $table_name" | tr -d ' ')

    if [[ ${num_rows} != ${expected_count} ]]; then
        echo "Expected number of rows to be ${expected_count} but was ${num_rows}"
        exit 1
    fi
}

function run_wasb_benchmark() {
    create_wasb_external_tables

    WASB_SERVER_DIR="${PXF_SERVER_DIR}/wasbbenchmark"

    # Create the WASB Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $WASB_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/wasbs-site.xml $WASB_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_NAME|${WASB_ACCOUNT_NAME}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AZURE_BLOB_STORAGE_ACCOUNT_KEY|${WASB_ACCOUNT_KEY}|\" ${WASB_SERVER_DIR}/wasbs-site.xml"
    sync_configuration

    write_header "AZURE BLOB STORAGE PXF READ BENCHMARK"
    assert_count_in_table "lineitem_wasb_read" "${LINEITEM_COUNT}"

    write_header "AZURE BLOB STORAGE PXF WRITE BENCHMARK"
    time psql -c "INSERT INTO lineitem_wasb_write SELECT * FROM lineitem"
}

function run_adl_benchmark() {
    create_adl_external_tables

    ADL_SERVER_DIR="${PXF_SERVER_DIR}/adlbenchmark"

    # Create the ADL Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $ADL_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/adl-site.xml $ADL_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_REFRESH_URL|${ADL_REFRESH_URL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_CLIENT_ID|${ADL_CLIENT_ID}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_ADL_CREDENTIAL|${ADL_CREDENTIAL}|\" ${ADL_SERVER_DIR}/adl-site.xml"
    sync_configuration

    write_header "ADL PXF READ BENCHMARK"
    assert_count_in_table "lineitem_adl_read" "${LINEITEM_COUNT}"

    write_header "ADL PXF WRITE BENCHMARK"
    time psql -c "INSERT INTO lineitem_adl_write SELECT * FROM lineitem"
}

function run_gcs_benchmark() {
    create_gcs_external_tables

    cat << EOF > /tmp/gsc-ci-service-account.key.json
${GOOGLE_CREDENTIALS}
EOF

    GS_SERVER_DIR="${PXF_SERVER_DIR}/gsbenchmark"

    # Create the Google Cloud Storage Benchmark server and copy core-site.xml
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $GS_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/gs-site.xml $GS_SERVER_DIR"
    gpscp -u gpadmin -h mdw /tmp/gsc-ci-service-account.key.json =:${GS_SERVER_DIR}/
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_GOOGLE_STORAGE_KEYFILE|${GS_SERVER_DIR}/gsc-ci-service-account.key.json|\" ${GS_SERVER_DIR}/gs-site.xml"
    sync_configuration

    write_header "GOOGLE CLOUD STORAGE PXF READ BENCHMARK"
    assert_count_in_table "lineitem_gcs_read" "${LINEITEM_COUNT}"

    write_header "GOOGLE CLOUD STORAGE PXF WRITE BENCHMARK"
    time psql -c "INSERT INTO lineitem_gcs_write SELECT * FROM lineitem"
}

function prepare_for_s3_pxf_benchmark() {
    # We need to create s3-site.xml and provide AWS credentials
    S3_SERVER_DIR="${PXF_SERVER_DIR}/s3benchmark"

    # Make a backup of core-site and update it with the S3 core-site
    gpssh -u gpadmin -h mdw -v -s -e "mkdir -p $S3_SERVER_DIR && cp ${PXF_CONF_DIR}/templates/s3-site.xml $S3_SERVER_DIR"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AWS_ACCESS_KEY_ID|$(AWS_ACCESS_KEY_ID)|\" $S3_SERVER_DIR/s3-site.xml"
    gpssh -u gpadmin -h mdw -v -s -e "sed -i \"s|YOUR_AWS_SECRET_ACCESS_KEY|$(AWS_SECRET_ACCESS_KEY)|\" $S3_SERVER_DIR/s3-site.xml"
    sync_configuration
}

function run_s3_pxf_benchmark () {
    local uuid
    local runid
    uuid=${1}
    runid=${2}

    create_s3_pxf_external_tables ${uuid} ${runid}

    write_header "S3 PXF READ BENCHMARK"
    assert_count_in_table "lineitem_s3_pxf_${runid}" "${LINEITEM_COUNT}"

    write_header "S3 PXF WRITE BENCHMARK"
    time psql -c "INSERT INTO lineitem_s3_pxf_write_${runid} SELECT * FROM lineitem"

    write_header "S3 PXF WRITE PARQUET BENCHMARK"
    time psql -c "INSERT INTO lineitem_s3_pxf_write_parquet_${runid} SELECT * FROM lineitem"

    write_header "S3 PXF READ PARQUET BENCHMARK"
    assert_count_in_table "lineitem_s3_pxf_parquet_${runid}" "${LINEITEM_COUNT}"
}

function run_s3_extension_benchmark {
    create_s3_extension_external_tables

    write_header "S3 C Ext READ BENCHMARK"
    assert_count_in_table "lineitem_s3_c" "${LINEITEM_COUNT}"

    write_header "S3 C Ext WRITE BENCHMARK"
    time psql -c "INSERT INTO lineitem_s3_c_write SELECT * FROM lineitem"
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
    LINEITEM_VAL_RESULTS=$(psql -t -c "SELECT ${VALIDATION_QUERY} FROM lineitem")

    if [[ ${BENCHMARK_ADL} == true ]]; then
        run_adl_benchmark
    fi

    if [[ ${BENCHMARK_WASB} == true ]]; then
        # Azure Blob Storage Benchmark
        run_wasb_benchmark
    fi

    if [[ ${BENCHMARK_GCS} == true ]]; then
        run_gcs_benchmark
    fi

    if [[ ${BENCHMARK_S3_EXTENSION} == true ]]; then
        run_s3_extension_benchmark
    fi

    if [[ ${BENCHMARK_S3} == true ]]; then
        concurrency=${BENCHMARK_S3_CONCURRENCY:-1}
        run_uuid=${UUID}

        prepare_for_s3_pxf_benchmark
        for i in `seq 1 ${concurrency}`; do
            echo "Running S3 Extension Benchmark ${i} with UUID ${run_uuid}"
            run_s3_pxf_benchmark ${run_uuid} ${i} &
            run_uuid=$(cat /proc/sys/kernel/random/uuid)
        done
        wait
    fi

    if [[ ${BENCHMARK_GPHDFS} == true ]]; then
        run_gphdfs_benchmark
    fi

    run_pxf_benchmark
}

main
