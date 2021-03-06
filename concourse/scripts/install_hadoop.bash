#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${CWDIR}/pxf_common.bash"

SSH_OPTS="-i cluster_env_files/private_key.pem"
GPHD_ROOT="/singlecluster"

function setup_pxf {

    local segment=${1}
    local hadoop_ip=${2}
    scp -r ${SSH_OPTS} pxf_tarball centos@${segment}:
    scp ${SSH_OPTS} cluster_env_files/etc_hostfile centos@${segment}:

	# install PXF as superuser
    ssh ${SSH_OPTS} centos@${segment} "
        sudo sed -i -e 's/edw0/hadoop/' /etc/hosts &&
        sudo yum install -y -d 1 java-1.8.0-openjdk-devel &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~gpadmin/.bashrc &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' | sudo tee -a ~centos/.bashrc &&
        sudo tar -xzf pxf_tarball/pxf.tar.gz -C ${GPHOME} &&
        sudo chown -R gpadmin:gpadmin ${GPHOME}/pxf"
}

function install_hadoop_single_cluster() {

    local hadoop_ip=${1}
    ssh ${SSH_OPTS} centos@edw0 "sudo mkdir -p /root/.ssh &&
        sudo cp /home/centos/.ssh/authorized_keys /root/.ssh &&
        sudo sed -i 's/PermitRootLogin no/PermitRootLogin yes/' /etc/ssh/sshd_config &&
        sudo service sshd restart"

    tar -xzf pxf_tarball/pxf.tar.gz -C /tmp
    cp /tmp/pxf/lib/pxf-hbase-*.jar /singlecluster/hbase/lib

    scp ${SSH_OPTS} cluster_env_files/etc_hostfile root@edw0:
    scp ${SSH_OPTS} -rq /singlecluster root@edw0:/
    scp ${SSH_OPTS} pxf_src/concourse/scripts/pxf_common.bash root@edw0:

    ssh ${SSH_OPTS} root@edw0 "
        source pxf_common.bash &&
        export IMPERSONATION=${IMPERSONATION} &&
        export GPHD_ROOT=${GPHD_ROOT} &&
        sed -i 's/edw0/hadoop/' /etc/hosts &&
        sed -i -e 's/>tez/>mr/g' -e 's/localhost/${hadoop_ip}/g' \${GPHD_ROOT}/hive/conf/hive-site.xml &&
        sed -i -e 's/0.0.0.0/${hadoop_ip}/g' \${GPHD_ROOT}/hadoop/etc/hadoop/{core,hdfs,yarn}-site.xml &&

        yum install -y -d 1 java-1.8.0-openjdk-devel &&
        echo 'export JAVA_HOME=/usr/lib/jvm/jre' >> ~/.bashrc &&
        export JAVA_HOME=/etc/alternatives/jre_1.8.0_openjdk &&
        export HADOOP_ROOT=${GPHD_ROOT} &&
        export HBASE_ROOT=${GPHD_ROOT}/hbase &&
        export HIVE_ROOT=${GPHD_ROOT}/hive &&
        export ZOOKEEPER_ROOT=${GPHD_ROOT}/zookeeper &&
        export PATH=\$PATH:${GPHD_ROOT}/bin:\${HADOOP_ROOT}/bin:\${HBASE_ROOT}/bin:\${HIVE_ROOT}/bin:\${ZOOKEEPER_ROOT}/bin &&

        groupadd supergroup && usermod -a -G supergroup gpadmin &&
        setup_impersonation ${GPHD_ROOT} &&
        start_hadoop_services ${GPHD_ROOT}"
}

function update_pghba_conf() {

    local sdw_ips=("$@")
    for ip in ${sdw_ips[@]}; do
        echo "host     all         gpadmin         $ip/32    trust" >> pg_hba.patch
    done
    scp ${SSH_OPTS} pg_hba.patch gpadmin@mdw:

    ssh ${SSH_OPTS} gpadmin@mdw "
        cat pg_hba.patch >> /data/gpdata/master/gpseg-1/pg_hba.conf &&
        cat /data/gpdata/master/gpseg-1/pg_hba.conf"
}

function install_pxf_and_hadoop() {
    # install hadoop server and untar pxf on all nodes in the cluster
    install_hadoop_single_cluster ${hadoop_ip} &
    for node in ${gpdb_nodes}; do
        setup_pxf ${node} ${hadoop_ip} &
    done
    wait
    # init all PXFs using cluster command, configure PXF on master, sync configs and start pxf
    ssh ${SSH_OPTS} gpadmin@mdw "source ${GPHOME}/greenplum_path.sh &&
        PXF_CONF=${PXF_CONF_DIR} ${GPHOME}/pxf/bin/pxf cluster init &&
        cp ${PXF_CONF_DIR}/templates/{hdfs,mapred,yarn,core,hbase,hive}-site.xml ${PXF_CONF_DIR}/servers/default/ &&
        mkdir -p ${PXF_CONF_DIR}/servers/s3 && mkdir -p ${PXF_CONF_DIR}/servers/s3-invalid &&
        cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3/ &&
        cp ${PXF_CONF_DIR}/templates/s3-site.xml ${PXF_CONF_DIR}/servers/s3-invalid/ &&
        sed -i \"s|YOUR_AWS_ACCESS_KEY_ID|${ACCESS_KEY_ID}|\" ${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
        sed -i \"s|YOUR_AWS_SECRET_ACCESS_KEY|${SECRET_ACCESS_KEY}|\" ${PXF_CONF_DIR}/servers/s3/s3-site.xml &&
        sed -i -e 's/\(0.0.0.0\|localhost\|127.0.0.1\)/${hadoop_ip}/g' ${PXF_CONF_DIR}/servers/default/*-site.xml &&
        if [ ${IMPERSONATION} == false ]; then
            echo 'export PXF_USER_IMPERSONATION=false' >> ${PXF_CONF_DIR}/conf/pxf-env.sh
        fi &&
        echo 'export PXF_JVM_OPTS=\"${PXF_JVM_OPTS}\"' >> ${PXF_CONF_DIR}/conf/pxf-env.sh &&
        ${GPHOME}/pxf/bin/pxf cluster sync &&
        ${GPHOME}/pxf/bin/pxf cluster start"
}

function _main() {

    cp -R cluster_env_files/.ssh/* /root/.ssh
    gpdb_nodes=$( < cluster_env_files/etc_hostfile grep -e "sdw\|mdw" | awk '{print $1}')
    gpdb_segments=$( < cluster_env_files/etc_hostfile grep -e "sdw" | awk '{print $1}')

    hadoop_ip=$( < cluster_env_files/etc_hostfile grep "edw0" | awk '{print $1}')
    install_pxf_and_hadoop

    # widen access to mdw to all nodes in the cluster for JDBC test
    update_pghba_conf "${gpdb_segments[@]}"
}

_main
